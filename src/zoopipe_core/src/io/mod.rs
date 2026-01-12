use std::fs::File;
use std::io::{BufReader, Read, BufRead};

pub enum BoxedReader {
    File(BufReader<File>),
    Cursor(std::io::Cursor<Vec<u8>>),
}

impl Read for BoxedReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            BoxedReader::File(f) => f.read(buf),
            BoxedReader::Cursor(c) => c.read(buf),
        }
    }
}

impl BufRead for BoxedReader {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        match self {
            BoxedReader::File(f) => f.fill_buf(),
            BoxedReader::Cursor(c) => c.fill_buf(),
        }
    }

    fn consume(&mut self, amt: usize) {
        match self {
            BoxedReader::File(f) => f.consume(amt),
            BoxedReader::Cursor(c) => c.consume(amt),
        }
    }
}
