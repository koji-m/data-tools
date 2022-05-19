mod extracter_file;
mod reader_json;
mod loader_s3;
mod writer_parquet;

use arrow::{
    datatypes::{DataType, Field, Schema},
    json::reader::DecoderOptions,
};
use clap::{Command, Arg};
use extracter_file::Extracter;
use reader_json::Reader;
use loader_s3::Loader;
use parquet::{
    basic::Compression,
    file::{
        properties::WriterProperties,
    }
};
use serde::{Deserialize, Serialize};
use std::{
    error::Error,
    fmt,
    fs::File,
    io,
    io::BufReader,
    path::Path,
    result::Result,
};
use writer_parquet::Writer;

#[derive(Serialize, Deserialize)]
struct BigQueryColumnDefinition {
    name: String,
    r#type: String,
    mode: String,
}

#[derive(Debug, Clone)]
struct UnknownTypeError {
    type_name: String,
}

impl fmt::Display for UnknownTypeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "unknown BigQuery type: {}", self.type_name)
    }
}

impl Error for UnknownTypeError {
    fn description(&self) -> &str {
        "unknown BigQuery types"
    }
}

fn create_field(name: &str, type_: &str, mode: Option<&str>) -> Result<Field, Box<dyn Error>> {
    let t = match type_ {
        "STRING" => Ok(DataType::Utf8),
        "INTEGER" => Ok(DataType::Int64),
        "FLOAT64" => Ok(DataType::Float64),
        "NUMERIC" => Ok(DataType::Decimal(38, 9)),
        unknown => Err(UnknownTypeError{type_name: String::from(unknown)})
    }?;
    let nullable = if let Some(nullable_) = mode { nullable_ == "NULLABLE" } else { false };
        
    Ok(Field::new(&name, t, nullable))
}

fn get_schema<P: AsRef<Path>>(path: P) -> Result<Schema, Box<dyn Error>>  {
    let mut column_definitions = vec![];
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let schema: Vec<BigQueryColumnDefinition> = serde_json::from_reader(reader)?;
    for column_definition in &schema {
        let field = create_field(
            &column_definition.name,
            &column_definition.r#type,
            Some(&column_definition.mode)
        )?;
        column_definitions.push(field);
    }
    Ok(Schema::new(column_definitions))
}

#[tokio::main]
async fn main() {
    let args = Command::new("json2parquet")
        .version("0.0.1")
        .about("transform JSON to Parquet")
        .arg(Arg::new("schema-file")
            .help("Schema file (BigQuery schema)")
            .long("schema")
            .takes_value(true)
            .required(true))
        .arg(Arg::new("s3-bucket")
            .help("S3 bucket name")
            .long("s3-bucket")
            .takes_value(true)
            .required(true))
        .arg(Arg::new("key-prefix")
            .help("S3 key prefix")
            .long("key-prefix")
            .takes_value(true)
            .required(true))
        .arg(Arg::new("batch-size")
            .help("number of records in each files")
            .long("batch-size")
            .takes_value(true)
            .default_value("10000"))
        .arg(Arg::new("input-file")
            .help("input file path or '-' for stdin")
            .takes_value(true)
            .required(true))
        .get_matches();
    let schema_file_path = args.value_of("schema-file").unwrap();
    let bucket = args.value_of("s3-bucket").unwrap();
    let key_prefix = args.value_of("key-prefix").unwrap();
    let batch_size: usize = args.value_of_t("batch-size").expect("batch-size must be number");
    let input_file = args.value_of("input-file").unwrap();

    
    let schema = get_schema(schema_file_path).unwrap();
    let options = DecoderOptions::new().with_batch_size(batch_size);
    let properties = WriterProperties::builder().set_compression(Compression::SNAPPY).build();
    let loader = Loader::new(&bucket, &key_prefix).await;
    let writer = Writer::new(properties);
    if input_file == "-" {
        let stdin = io::stdin();
        let mut extracter = Extracter::new(stdin.lock());
        let batch_reader = Reader::new(extracter.into_inner(), schema, options);
        for (i, batch_) in batch_reader.enumerate() {
            let cursor = writer.write(batch_.unwrap());
            loader.load(cursor.into_inner().unwrap(), i).await;
        }
    } else {
        let file = File::open(input_file).unwrap();
        let mut extracter = Extracter::new(BufReader::new(file));
        let batch_reader = Reader::new(extracter.into_inner(), schema, options);
        for (i, batch_) in batch_reader.enumerate() {
            let cursor = writer.write(batch_.unwrap());
            loader.load(cursor.into_inner().unwrap(), i).await;
        }
    };
}
