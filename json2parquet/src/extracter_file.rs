use std::{
    io::Read,
};

pub struct Extracter<R: Read> {
    stream: Box<R>
}

impl <R: Read> Extracter<R> {
    pub fn new(input_stream: R) -> Self {
        Self{
            stream: Box::new(input_stream),
        }
    }

    pub fn into_inner(&mut self) -> &mut R {
        &mut self.stream
    }
}