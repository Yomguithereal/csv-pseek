use std::io::{Cursor, Read, Seek, SeekFrom};

use clap::Parser;
use csv::{ByteRecord, Position, Reader, ReaderBuilder};
use rand::Rng;

fn find_max_record_size<R: Read + Seek>(
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
    up_to: u64,
) -> Result<bool, csv::Error> {
    let was_read = reader.read_byte_record(record)?;

    if !was_read {
        return Ok(false);
    }

    if record.position().unwrap().byte() > up_to {
        return Ok(false);
    }

    Ok(true)
}

fn find_next_record_offset<R: Read + Seek>(
    reader: &mut Reader<R>,
    offset: u64,
    max_record_size: u64,
    expected_field_count: usize,
) -> Result<Option<u64>, csv::Error> {
    let mut pos = Position::new();
    pos.set_byte(offset);
    reader.seek_raw(SeekFrom::Start(offset), pos)?;

    let up_to = offset + max_record_size * 16;
    dbg!(up_to);

    let mut record_infos = Vec::with_capacity(16);
    let mut record = ByteRecord::new();

    while read_byte_record_up_to(reader, &mut record, up_to)? {
        record_infos.push((record.position().unwrap().byte(), record.len()));
    }

    dbg!(record_infos.len());

    if record_infos.len() < 2 {
        return Ok(None);
    }

    if record_infos[1..]
        .iter()
        .all(|(_, field_count)| *field_count == expected_field_count)
    {
        dbg!("we were inside an UNQUOTED cell!");
        return Ok(Some(record_infos[1].0));
    }

    let mut pos = Position::new();
    pos.set_byte(offset);
    reader.seek_raw(SeekFrom::Start(offset), pos)?;

    let mut altered_reader = ReaderBuilder::new()
        .flexible(true)
        .has_headers(false)
        .from_reader(Cursor::new("\"").chain(reader.get_mut()));

    record_infos.clear();

    while read_byte_record_up_to(&mut altered_reader, &mut record, max_record_size * 16)? {
        record_infos.push((record.position().unwrap().byte(), record.len()));
    }

    dbg!(record_infos.len());

    if record_infos.len() < 2 {
        return Ok(None);
    }

    if record_infos[1..]
        .iter()
        .all(|(_, field_count)| *field_count == expected_field_count)
    {
        dbg!("we were inside an QUOTED cell!");
        return Ok(Some(record_infos[1].0));
    }

    Ok(None)
}

#[derive(Parser, Debug)]
struct Args {
    /// Path to CSV file to test
    path: String,
}

fn main() -> Result<(), csv::Error> {
    let args = Args::parse();
    let mut reader = ReaderBuilder::new().flexible(true).from_path(args.path)?;
    let field_count = reader.byte_headers()?.len();

    let max_record_size = find_max_record_size(&mut reader, 64)?;
    let file_len = reader.get_mut().seek(SeekFrom::End(0))?;
    let random_offset = rand::rng().random_range(0..file_len);

    dbg!(field_count, max_record_size, file_len, random_offset);

    find_next_record_offset(&mut reader, random_offset, max_record_size, field_count)?;

    Ok(())
}
