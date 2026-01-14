pub mod storage;

use std::fs::File;
use std::io::{BufReader, Read, BufRead, Write, Seek, SeekFrom};
use std::sync::Arc;
use object_store::path::Path;
use object_store::ObjectStore;
use tokio::runtime::Runtime;
use parquet::file::reader::{ChunkReader, Length};
use bytes::Bytes;

pub enum BoxedReader {
    File(BufReader<File>),
    Cursor(std::io::Cursor<Vec<u8>>),
    Remote(RemoteReader),
}

pub struct RemoteReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    runtime: Arc<Runtime>,
    buffer: Vec<u8>,
    pos: u64,
}

impl RemoteReader {
    pub fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        let runtime = Arc::new(Runtime::new().expect("Failed to create tokio runtime"));
        Self {
            store,
            path,
            runtime,
            buffer: Vec::new(),
            pos: 0,
        }
    }

    fn fetch_if_needed(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            let path = self.path.clone();
            let store = self.store.clone();
            let bytes = self.runtime.block_on(async move {
                let res = store.get(&path).await.map_err(std::io::Error::other)?;
                res.bytes().await.map_err(std::io::Error::other)
            })?;
            self.buffer = bytes.to_vec();
        }
        Ok(())
    }
}

impl Read for RemoteReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.fetch_if_needed()?;
        let available = self.buffer.len() as u64 - self.pos;
        let to_copy = std::cmp::min(available, buf.len() as u64) as usize;
        buf[..to_copy].copy_from_slice(&self.buffer[self.pos as usize..self.pos as usize + to_copy]);
        self.pos += to_copy as u64;
        Ok(to_copy)
    }
}

impl BufRead for RemoteReader {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.fetch_if_needed()?;
        Ok(&self.buffer[self.pos as usize..])
    }

    fn consume(&mut self, amt: usize) {
        self.pos = std::cmp::min(self.pos + amt as u64, self.buffer.len() as u64);
    }
}

impl Seek for RemoteReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.fetch_if_needed()?;
        let new_pos = match pos {
            SeekFrom::Start(p) => p as i64,
            SeekFrom::End(p) => self.buffer.len() as i64 + p,
            SeekFrom::Current(p) => self.pos as i64 + p,
        };

        if new_pos < 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid seek to a negative or overflowing position"));
        }

        self.pos = std::cmp::min(new_pos as u64, self.buffer.len() as u64);
        Ok(self.pos)
    }
}

impl Read for BoxedReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            BoxedReader::File(f) => f.read(buf),
            BoxedReader::Cursor(c) => c.read(buf),
            BoxedReader::Remote(r) => r.read(buf),
        }
    }
}

impl BufRead for BoxedReader {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        match self {
            BoxedReader::File(f) => f.fill_buf(),
            BoxedReader::Cursor(c) => c.fill_buf(),
            BoxedReader::Remote(r) => r.fill_buf(),
        }
    }

    fn consume(&mut self, amt: usize) {
        match self {
            BoxedReader::File(f) => f.consume(amt),
            BoxedReader::Cursor(c) => c.consume(amt),
            BoxedReader::Remote(r) => r.consume(amt),
        }
    }
}

impl Seek for BoxedReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match self {
            BoxedReader::File(f) => f.seek(pos),
            BoxedReader::Cursor(c) => c.seek(pos),
            BoxedReader::Remote(r) => r.seek(pos),
        }
    }
}

impl Length for BoxedReader {
    fn len(&self) -> u64 {
        match self {
            BoxedReader::File(f) => f.get_ref().metadata().map(|m| m.len()).unwrap_or(0),
            BoxedReader::Cursor(c) => c.get_ref().len() as u64,
            BoxedReader::Remote(r) => {
                let mut tmp = r.runtime.block_on(async {
                    r.store.head(&r.path).await.map(|m| m.size as u64).unwrap_or(0)
                });
                if tmp == 0 && !r.buffer.is_empty() {
                    tmp = r.buffer.len() as u64;
                }
                tmp
            }
        }
    }
}

impl ChunkReader for BoxedReader {
    type T = BoxedReaderChild;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        match self {
            BoxedReader::File(f) => {
                let mut file = f.get_ref().try_clone().map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
                file.seek(SeekFrom::Start(start)).map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer).map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
                Ok(BoxedReaderChild::Bytes(Bytes::from(buffer)))
            }
            BoxedReader::Cursor(c) => {
                let bytes = c.get_ref();
                Ok(BoxedReaderChild::Bytes(Bytes::copy_from_slice(&bytes[start as usize..])))
            }
            BoxedReader::Remote(r) => {
                let path = r.path.clone();
                let store = r.store.clone();
                let bytes = r.runtime.block_on(async move {
                    store.get(&path).await.map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?
                        .bytes().await.map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))
                })?;
                Ok(BoxedReaderChild::Bytes(bytes.slice(start as usize..)))
            }
        }
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        match self {
            BoxedReader::File(f) => {
                let mut file = f.get_ref().try_clone().map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
                file.seek(SeekFrom::Start(start)).map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
                let mut buffer = vec![0; length];
                file.read_exact(&mut buffer).map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
                Ok(Bytes::from(buffer))
            }
            BoxedReader::Cursor(c) => {
                let bytes = c.get_ref();
                let end = std::cmp::min(start as usize + length, bytes.len());
                Ok(Bytes::copy_from_slice(&bytes[start as usize..end]))
            }
            BoxedReader::Remote(r) => {
                let path = r.path.clone();
                let store = r.store.clone();
                let bytes = r.runtime.block_on(async move {
                    let range = start as usize..(start as usize + length);
                    store.get_range(&path, range).await.map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))
                })?;
                Ok(bytes)
            }
        }
    }
}

pub enum BoxedReaderChild {
    Bytes(Bytes),
}

impl Read for BoxedReaderChild {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            BoxedReaderChild::Bytes(b) => {
                let to_copy = std::cmp::min(b.len(), buf.len());
                buf[..to_copy].copy_from_slice(&b[..to_copy]);
                *b = b.slice(to_copy..);
                Ok(to_copy)
            }
        }
    }
}

pub enum BoxedWriter {
    File(std::io::BufWriter<File>),
    Remote(RemoteWriter),
}

pub struct RemoteWriter {
    store: Arc<dyn ObjectStore>,
    path: Path,
    runtime: Arc<Runtime>,
    buffer: Vec<u8>,
}

impl RemoteWriter {
    pub fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        let runtime = Arc::new(Runtime::new().expect("Failed to create tokio runtime"));
        Self {
            store,
            path,
            runtime,
            buffer: Vec::new(),
        }
    }
}

impl Write for RemoteWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let path = self.path.clone();
        let store = self.store.clone();
        let buffer = std::mem::take(&mut self.buffer);
        self.runtime.block_on(async move {
            store.put(&path, buffer.into()).await
                .map_err(std::io::Error::other)
        })?;
        Ok(())
    }
}

impl Write for BoxedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            BoxedWriter::File(f) => f.write(buf),
            BoxedWriter::Remote(r) => r.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            BoxedWriter::File(f) => f.flush(),
            BoxedWriter::Remote(r) => r.flush(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, BufRead, Seek, SeekFrom};

    #[test]
    fn test_boxed_reader_cursor_read() {
        let data = vec![1, 2, 3, 4, 5];
        let cursor = std::io::Cursor::new(data.clone());
        let mut reader = BoxedReader::Cursor(cursor);
        
        let mut buf = vec![0u8; 3];
        let n = reader.read(&mut buf).unwrap();
        
        assert_eq!(n, 3);
        assert_eq!(buf, vec![1, 2, 3]);
    }

    #[test]
    fn test_boxed_reader_cursor_read_all() {
        let data = vec![10, 20, 30];
        let cursor = std::io::Cursor::new(data.clone());
        let mut reader = BoxedReader::Cursor(cursor);
        
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        
        assert_eq!(buf, data);
    }

    #[test]
    fn test_boxed_reader_cursor_seek() {
        let data = vec![1, 2, 3, 4, 5];
        let cursor = std::io::Cursor::new(data);
        let mut reader = BoxedReader::Cursor(cursor);
        
        let pos = reader.seek(SeekFrom::Start(2)).unwrap();
        assert_eq!(pos, 2);
        
        let mut buf = vec![0u8; 2];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(buf, vec![3, 4]);
    }

    #[test]
    fn test_boxed_reader_cursor_seek_from_end() {
        let data = vec![1, 2, 3, 4, 5];
        let cursor = std::io::Cursor::new(data);
        let mut reader = BoxedReader::Cursor(cursor);
        
        let pos = reader.seek(SeekFrom::End(-2)).unwrap();
        assert_eq!(pos, 3);
        
        let mut buf = vec![0u8; 2];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(buf, vec![4, 5]);
    }

    #[test]
    fn test_boxed_reader_cursor_seek_current() {
        let data = vec![1, 2, 3, 4, 5];
        let cursor = std::io::Cursor::new(data);
        let mut reader = BoxedReader::Cursor(cursor);
        
        reader.seek(SeekFrom::Start(1)).unwrap();
        let pos = reader.seek(SeekFrom::Current(2)).unwrap();
        assert_eq!(pos, 3);
        
        let mut buf = vec![0u8; 1];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(buf, vec![4]);
    }

    #[test]
    fn test_boxed_reader_cursor_bufread() {
        let data = b"hello\nworld\n".to_vec();
        let cursor = std::io::Cursor::new(data);
        let mut reader = BoxedReader::Cursor(cursor);
        
        let mut line = String::new();
        std::io::BufRead::read_line(&mut reader, &mut line).unwrap();
        
        assert_eq!(line, "hello\n");
    }

    #[test]
    fn test_boxed_reader_cursor_fill_buf() {
        let data = vec![1, 2, 3, 4, 5];
        let cursor = std::io::Cursor::new(data.clone());
        let mut reader = BoxedReader::Cursor(cursor);
        
        let buf = reader.fill_buf().unwrap();
        assert_eq!(buf, &data[..]);
    }

    #[test]
    fn test_boxed_reader_cursor_consume() {
        let data = vec![1, 2, 3, 4, 5];
        let cursor = std::io::Cursor::new(data);
        let mut reader = BoxedReader::Cursor(cursor);
        
        reader.consume(2);
        
        let mut buf = vec![0u8; 3];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(buf, vec![3, 4, 5]);
    }

    #[test]
    fn test_boxed_reader_cursor_length() {
        let data = vec![1, 2, 3, 4, 5];
        let cursor = std::io::Cursor::new(data);
        let reader = BoxedReader::Cursor(cursor);
        
        assert_eq!(reader.len(), 5);
    }

    #[test]
    fn test_boxed_reader_cursor_empty() {
        let data: Vec<u8> = vec![];
        let cursor = std::io::Cursor::new(data);
        let reader = BoxedReader::Cursor(cursor);
        
        assert_eq!(reader.len(), 0);
    }

    #[test]
    fn test_boxed_reader_child_read() {
        let data = bytes::Bytes::from(vec![1, 2, 3, 4, 5]);
        let mut child = BoxedReaderChild::Bytes(data);
        
        let mut buf = vec![0u8; 3];
        let n = child.read(&mut buf).unwrap();
        
        assert_eq!(n, 3);
        assert_eq!(buf, vec![1, 2, 3]);
    }

    #[test]
    fn test_boxed_reader_child_read_partial() {
        let data = bytes::Bytes::from(vec![1, 2]);
        let mut child = BoxedReaderChild::Bytes(data);
        
        let mut buf = vec![0u8; 10];
        let n = child.read(&mut buf).unwrap();
        
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], &[1, 2]);
    }

    #[test]
    fn test_boxed_reader_child_multiple_reads() {
        let data = bytes::Bytes::from(vec![1, 2, 3, 4, 5]);
        let mut child = BoxedReaderChild::Bytes(data);
        
        let mut buf1 = vec![0u8; 2];
        child.read_exact(&mut buf1).unwrap();
        assert_eq!(buf1, vec![1, 2]);
        
        let mut buf2 = vec![0u8; 3];
        child.read_exact(&mut buf2).unwrap();
        assert_eq!(buf2, vec![3, 4, 5]);
    }
}
