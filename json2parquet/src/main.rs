use arrow::datatypes::{DataType, Field, Schema};
use arrow::json::reader::{DecoderOptions, Reader};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::io;
use std::sync::Arc;

fn main() {
    let stdin = io::stdin();
    let reader = stdin.lock();
    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("kokyu", DataType::Utf8, true),
    ]);
    let options = DecoderOptions::new();
    let json_reader = Reader::new(reader, Arc::new(schema), options);
    for (i, batch_) in json_reader.enumerate() {
        let batch = batch_.unwrap();
        let file = File::create(format!("kimetsu_{i}.parquet")).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();
    }
}
