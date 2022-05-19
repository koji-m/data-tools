use arrow::{
    record_batch::RecordBatch,
};
use parquet::{
    arrow::arrow_writer::ArrowWriter,
    file::{
        properties::WriterProperties,
        writer::{InMemoryWriteableCursor, TryClone},
    }
};

pub struct Writer {
    properties: WriterProperties,
}

impl Writer {
    pub fn new(properties: WriterProperties) -> Self {
        Self {properties: properties}
    }

    pub fn write(&self, batch: RecordBatch) -> InMemoryWriteableCursor {
        let cursor = InMemoryWriteableCursor::default();
        let mut writer = ArrowWriter::try_new(
            cursor.try_clone().unwrap(),
            batch.schema(),
            Some(self.properties.clone())
        ).unwrap();
        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();
        cursor
    }
}
