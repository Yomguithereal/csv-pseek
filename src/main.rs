use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom};

use clap::Parser;
use csv::{ByteRecord, Position, Reader, ReaderBuilder, Writer};
use rand::Rng;
use rayon::prelude::*;

fn find_max_record_size_from_sample<R: Read + Seek>(
    reader: &mut Reader<R>,
    max_records_to_read: usize,
) -> Result<u64, csv::Error> {
    let mut record = ByteRecord::new();

    let mut i = 0;
    let mut max_record_size = 0;
    let mut last_offset = reader.position().byte();

    while i < max_records_to_read && reader.read_byte_record(&mut record)? {
        let record_byte_pos = record.position().unwrap().byte();
        let record_size = record_byte_pos - last_offset;

        if record_size > max_record_size {
            max_record_size = record_size;
        }

        i += 1;
        last_offset = record_byte_pos;
    }

    Ok(max_record_size)
}

fn read_byte_record_up_to<R: Read>(
    reader: &mut Reader<R>,
    record: &mut ByteRecord,
    up_to: Option<u64>,
) -> Result<bool, csv::Error> {
    let was_read = reader.read_byte_record(record)?;

    if !was_read {
        return Ok(false);
    }

    if let Some(byte) = up_to {
        if record.position().unwrap().byte() >= byte {
            return Ok(false);
        }
    }

    Ok(true)
}

#[derive(Debug, Clone)]
enum NextRecord {
    Start,
    EndOfFile,
    Offset(bool, u64),
    Fail,
}

impl NextRecord {
    fn offset(&self) -> Option<u64> {
        match self {
            Self::Offset(_, o) => Some(*o),
            _ => None,
        }
    }
}

fn find_next_record_offset<R: Read + Seek>(
    reader: &mut Reader<R>,
    offset: u64,
    max_record_size: u64,
    expected_field_count: usize,
) -> Result<NextRecord, csv::Error> {
    let mut pos = Position::new();
    pos.set_byte(offset);
    reader.seek_raw(SeekFrom::Start(offset), pos)?;

    let up_to = offset + max_record_size * 16;
    dbg!(up_to);

    let mut record_infos = Vec::with_capacity(16);
    let mut record = ByteRecord::new();

    while read_byte_record_up_to(reader, &mut record, Some(up_to))? {
        record_infos.push((record.position().unwrap().byte(), record.len()));
    }

    dbg!(record_infos.len());

    if record_infos.len() < 2 {
        return Ok(NextRecord::EndOfFile);
    }

    // NOTE: we never return the current record, only the next one, because
    // even if we have found the expected number of fields in current record,
    // we cannot likely have read the beginning of first field without reading
    // backwards.
    if record_infos[1..]
        .iter()
        .all(|(_, field_count)| *field_count == expected_field_count)
    {
        return Ok(NextRecord::Offset(false, record_infos[1].0));
    }

    let mut pos = Position::new();
    pos.set_byte(offset);
    reader.seek_raw(SeekFrom::Start(offset), pos)?;

    // TODO: quote char must be known if different
    let mut altered_reader = ReaderBuilder::new()
        .flexible(true)
        .has_headers(false)
        .from_reader(Cursor::new("\"").chain(reader.get_mut()));

    record_infos.clear();
    let up_to = max_record_size * 16 + 1;

    while read_byte_record_up_to(&mut altered_reader, &mut record, Some(up_to))? {
        record_infos.push((record.position().unwrap().byte(), record.len()));
    }

    dbg!(record_infos.len());

    if record_infos.len() < 2 {
        return Ok(NextRecord::EndOfFile);
    }

    if record_infos[1..]
        .iter()
        .all(|(_, field_count)| *field_count == expected_field_count)
    {
        return Ok(NextRecord::Offset(true, offset + record_infos[1].0 - 1));
    }

    Ok(NextRecord::Fail)
}

fn segment_file_into_offsets(file_len: u64, threads: usize) -> Vec<u64> {
    if threads < 2 {
        return vec![0];
    }

    let mut offsets = vec![0];

    for i in 1..threads {
        offsets.push(((i as f64 / threads as f64) * file_len as f64).floor() as u64);
    }

    offsets
}

#[derive(Parser, Debug)]
struct Args {
    /// Path to CSV file to test
    path: String,
    /// Whether to parallelize reads
    #[clap(short, long)]
    parallel: bool,
}

fn main() -> Result<(), csv::Error> {
    let args = Args::parse();
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_path(args.path.clone())?;
    let headers = reader.byte_headers()?.clone();
    let field_count = headers.len();

    let max_record_size = find_max_record_size_from_sample(&mut reader, 64)?;
    let file_len = reader.get_mut().seek(SeekFrom::End(0))?;
    let random_offset = rand::rng().random_range(0..file_len);
    let segment_offsets = segment_file_into_offsets(file_len, 4);

    dbg!(field_count, max_record_size, file_len, random_offset);
    dbg!(&segment_offsets);

    // let next_record =
    //     find_next_record_offset(&mut reader, random_offset, max_record_size, field_count)?;

    // match next_record {
    //     NextRecord::Offset(quoted, offset) => {
    //         assert!(offset >= random_offset);
    //         dbg!(if quoted { "QUOTED" } else { "UNQUOTED" }, offset);

    //         let mut writer = Writer::from_writer(std::io::stdout());
    //         writer.write_byte_record(&headers)?;

    //         let mut record = ByteRecord::new();
    //         let mut pos = Position::new();
    //         pos.set_byte(offset);
    //         reader.seek_raw(SeekFrom::Start(offset), pos)?;

    //         if reader.read_byte_record(&mut record)? {
    //             writer.write_byte_record(&record)?;
    //         }

    //         writer.flush()?;
    //     }
    //     r => {
    //         dbg!(r);
    //     }
    // }

    let mut next_records = segment_offsets
        .iter()
        .copied()
        .map(|offset| {
            if offset == 0 {
                Ok(NextRecord::Start)
            } else {
                find_next_record_offset(&mut reader, offset, max_record_size, field_count)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    next_records.push(NextRecord::EndOfFile);

    dbg!(&next_records);

    if !args.parallel {
        let mut count: u64 = 0;
        let mut record = ByteRecord::new();
        let mut reader = Reader::from_path(args.path)?;

        while reader.read_byte_record(&mut record)? {
            count += 1;
        }

        println!("{}", count);
    } else {
        let counts = next_records
            .windows(2)
            .map(|w| (&w[0], &w[1]))
            .collect::<Vec<_>>()
            .par_iter()
            .map(|(current, next)| -> Result<u64, csv::Error> {
                let mut count: u64 = 0;
                let mut record = ByteRecord::new();
                let file = File::open(args.path.clone())?;

                match current {
                    NextRecord::Start => {
                        let mut reader = Reader::from_reader(file);

                        while read_byte_record_up_to(&mut reader, &mut record, next.offset())? {
                            count += 1;
                        }
                    }
                    NextRecord::Offset(_, offset) => {
                        let mut reader = Reader::from_reader(file);
                        let mut pos = Position::new();
                        pos.set_byte(*offset);
                        reader.seek(pos)?;

                        while read_byte_record_up_to(&mut reader, &mut record, next.offset())? {
                            count += 1;
                        }
                    }
                    _ => unreachable!(),
                }

                Ok(count)
            })
            .collect::<Result<Vec<_>, _>>()?;

        dbg!(&counts);

        println!("{}", counts.iter().sum::<u64>());
    }

    Ok(())
}
