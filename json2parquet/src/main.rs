use arrow::datatypes::{DataType, Field, Schema};
use arrow::json::reader::{DecoderOptions, Reader};
use arrow::record_batch::RecordBatch;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Region};
use clap::{App, Arg};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{InMemoryWriteableCursor, TryClone};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::path::Path;
use std::result::Result;
use std::sync::Arc;

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

async fn get_s3_client() -> Option<Client> {
    let region_provider = RegionProviderChain::default_provider()
        .or_else(Region::new("us-east-1"));
    let config = aws_config::from_env().region(region_provider).load().await;
    Some(Client::new(&config))
}

fn write_parquet_to_memory(batch: RecordBatch, properties: WriterProperties) -> InMemoryWriteableCursor {
    let cursor = InMemoryWriteableCursor::default();
    let mut writer = ArrowWriter::try_new(
        cursor.try_clone().unwrap(),
        batch.schema(),
        Some(properties)
    ).unwrap();
    writer.write(&batch).expect("Writing batch");
    writer.close().unwrap();
    cursor
}

async fn upload_to_s3(cursor: InMemoryWriteableCursor, bucket: &str, key_prefix: &str, suffix: usize, client: &Client) {
    let stream = ByteStream::from(cursor.into_inner().unwrap());
    let file = format!("{}{}.parquet", key_prefix, suffix);
    let resp = client
        .put_object()
        .bucket(bucket)
        .key(&file)
        .body(stream)
        .send()
        .await;
    match resp {
        Ok(_) => println!("Wrote s3://{}/{}", bucket, file),
        Err(_) => println!("Error write s3://{}/{}", bucket, file),
    }
}

#[tokio::main]
async fn main() {
    let args = App::new("json2parquet")
        .version("0.0.1")
        .about("transform JSON to Parquet")
        .arg(Arg::with_name("schema-file")
            .help("Schema file (BigQuery schema)")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("s3-bucket")
            .help("S3 bucket name")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("key-prefix")
            .help("S3 key prefix")
            .takes_value(true)
            .required(true))
        .get_matches();
    let schema_file_path = args.value_of("schema-file").unwrap();
    let bucket = args.value_of("s3-bucket").unwrap();
    let key_prefix = args.value_of("key-prefix").unwrap();

    let stdin = io::stdin();
    let reader = stdin.lock();
    let schema = get_schema(schema_file_path).unwrap();
    let options = DecoderOptions::new();
    let json_reader = Reader::new(reader, Arc::new(schema), options);
    let client = get_s3_client().await.unwrap();    
    let properties = WriterProperties::builder().build();
    for (i, batch_) in json_reader.enumerate() {
        let cursor = write_parquet_to_memory(batch_.unwrap(), properties.clone());
        upload_to_s3(cursor, bucket, key_prefix, i, &client).await;
    }
}
