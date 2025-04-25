use std::io::{Read, Seek};

use clap::Parser;
use csv::{ByteRecord, Reader};

fn find_max_record_size<R: Read + Seek>(
    reader: &mut Reader<R>,
    max_records_to_read: usize,
) -> Result<u64, csv::Error> {
    let mut record = ByteRecord::new();

    let mut i = 0;
    let mut max_record_size = 0;
    let mut last_offset = reader.get_mut().stream_position().unwrap();

    while i < max_records_to_read && reader.read_byte_record(&mut record)? {
        i += 1;
        let record_byte_pos = record.position().unwrap().byte();
        let record_size = record_byte_pos - last_offset;
        last_offset = record_byte_pos;

        if record_size > max_record_size {
            max_record_size = record_size;
        }
    }

    Ok(max_record_size)
}

#[derive(Parser, Debug)]
struct Args {
    /// Path to CSV file to test
    path: String,
}

fn main() -> Result<(), csv::Error> {
    let args = Args::parse();
    let mut reader = Reader::from_path(args.path).unwrap();

    let max_record_size = find_max_record_size(&mut reader, 32)?;

    dbg!(max_record_size);

    Ok(())
}
