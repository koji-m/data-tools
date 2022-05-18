use arrow::{
    datatypes::Schema,
    json::reader::{DecoderOptions, Reader},
};
use serde::{Deserialize, Serialize};
use std::{
    error::Error,
    fmt,
    io::Read,
    result::Result,
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

pub fn reader<R: Read>(input_file: R, schema: Schema, options: DecoderOptions) -> Result<Reader<R>, Box<dyn Error>> {
        return Ok(Reader::new(input_file, Arc::new(schema), options));
}
