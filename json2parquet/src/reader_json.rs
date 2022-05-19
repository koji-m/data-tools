use arrow::{
    datatypes::Schema,
    json,
};
use serde::{Deserialize, Serialize};
use std::{
    error::Error,
    fmt,
    io::Read,
    sync::Arc,
};

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

pub struct Reader();

impl Reader {
    pub fn new<R: Read>(input_file: R, schema: Schema, options: json::reader::DecoderOptions) -> json::reader::Reader<R> {
            json::reader::Reader::new(input_file, Arc::new(schema), options)
    }
}
