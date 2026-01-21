pub mod storage;
pub mod smart_reader;

use std::fs::File;
use std::io::{BufReader, Read, BufRead, Write, Seek, SeekFrom};
use std::sync::Arc;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::runtime::Runtime;
use parquet::file::reader::{ChunkReader, Length};
use bytes::Bytes;

pub use smart_reader::SmartReader;

/// Unified reader that abstracts over local files, in-memory cursors, and remote storage.
/// 
/// It implements standard I/O traits and provides transparent access to 
/// different backends, enabling the parsers to work with any source.
pub enum BoxedReader {
    File(BufReader<File>),
    Cursor(std::io::Cursor<Vec<u8>>),
    Remote(RemoteReader),
}

pub struct CountingReader<R> {
    inner: R,
    count: Arc<std::sync::atomic::AtomicU64>,
}

impl<R> CountingReader<R> {
    pub fn new(inner: R, initial_count: u64) -> Self {
        Self {
            inner,
            count: Arc::new(std::sync::atomic::AtomicU64::new(initial_count)),
        }
    }

    pub fn get_count_handle(&self) -> Arc<std::sync::atomic::AtomicU64> {
        self.count.clone()
    }
}

impl<R: std::io::Read> std::io::Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.count.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(n)
    }
}

impl<R: std::io::BufRead> std::io::BufRead for CountingReader<R> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.inner.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.inner.consume(amt);
        self.count.fetch_add(amt as u64, std::sync::atomic::Ordering::Relaxed);
    }
}

impl<R: std::io::Seek> std::io::Seek for CountingReader<R> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let n = self.inner.seek(pos)?;
        self.count.store(n, std::sync::atomic::Ordering::Relaxed);
        Ok(n)
    }
}

use std::sync::OnceLock;

pub fn get_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime")
    })
}

const READ_BUFFER_SIZE: usize = 8 * 1024 * 1024; // 8MB
const WRITE_BUFFER_SIZE: usize = 8 * 1024 * 1024; // 8MB

pub struct RemoteReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    buffer: Bytes,
    pos: u64,
    file_len: u64,
}

impl RemoteReader {
    pub fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        let file_len = get_runtime().block_on(async {
            store.head(&path).await.map(|m| m.size).unwrap_or(0)
        });

        Self::new_with_len(store, path, file_len)
    }

    pub fn new_with_len(store: Arc<dyn ObjectStore>, path: Path, file_len: u64) -> Self {
        Self {
            store,
            path,
            buffer: Bytes::new(),
            pos: 0,
            file_len,
        }
    }

    fn fetch_next_chunk(&mut self) -> std::io::Result<()> {
        if self.pos >= self.file_len {
            return Ok(());
        }

        let end = std::cmp::min(self.pos + READ_BUFFER_SIZE as u64, self.file_len);
        let range = self.pos..end;
        
        let path = self.path.clone();
        let store = self.store.clone();
        
        let bytes = get_runtime().block_on(async move {
            store.get_range(&path, range).await.map_err(std::io::Error::other)
        })?;
        
        self.buffer = bytes;
        Ok(())
    }
}

impl Read for RemoteReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.buffer.is_empty() && self.pos < self.file_len {
            self.fetch_next_chunk()?;
        }

        if self.buffer.is_empty() {
            return Ok(0);
        }

        let to_copy = std::cmp::min(self.buffer.len(), buf.len());
        buf[..to_copy].copy_from_slice(&self.buffer[..to_copy]);
        self.buffer = self.buffer.slice(to_copy..);
        self.pos += to_copy as u64;
        Ok(to_copy)
    }
}

impl BufRead for RemoteReader {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        if self.buffer.is_empty() && self.pos < self.file_len {
            self.fetch_next_chunk()?;
        }
        Ok(&self.buffer)
    }

    fn consume(&mut self, amt: usize) {
        let amt = std::cmp::min(amt, self.buffer.len());
        self.buffer = self.buffer.slice(amt..);
        self.pos += amt as u64;
    }
}

impl Seek for RemoteReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(p) => p as i64,
            SeekFrom::End(p) => self.file_len as i64 + p,
            SeekFrom::Current(p) => (self.pos - self.buffer.len() as u64) as i64 + p,
        };

        if new_pos < 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid seek to a negative or overflowing position"));
        }

        let new_pos = new_pos as u64;
        
        // If the new position is within the current buffer, we just slice it
        let current_start = self.pos - self.buffer.len() as u64;
        if new_pos >= current_start && new_pos < self.pos {
            let offset = (new_pos - current_start) as usize;
            self.buffer = self.buffer.slice(offset..);
            // pos remains the same as it points to the end of the buffered chunk
        } else {
            // Otherwise, we invalidate the buffer and seek
            self.buffer = Bytes::new();
            self.pos = std::cmp::min(new_pos, self.file_len);
        }

        Ok(new_pos)
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
                let mut tmp = get_runtime().block_on(async {
                    r.store.head(&r.path).await.map(|m| m.size).unwrap_or(0)
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
                Ok(BoxedReaderChild::File(file))
            }
            BoxedReader::Cursor(c) => {
                let bytes = c.get_ref();
                Ok(BoxedReaderChild::Bytes(Bytes::copy_from_slice(&bytes[start as usize..])))
            }
            BoxedReader::Remote(r) => {
                let mut child_reader = RemoteReader::new_with_len(r.store.clone(), r.path.clone(), r.file_len);
                child_reader.seek(SeekFrom::Start(start)).map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
                Ok(BoxedReaderChild::Remote(child_reader))
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
                let bytes = get_runtime().block_on(async move {
                    let range = start..(start + length as u64);
                    store.get_range(&path, range).await.map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))
                })?;
                Ok(bytes)
            }
        }
    }
}

pub enum BoxedReaderChild {
    Bytes(Bytes),
    File(File),
    Remote(RemoteReader),
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
            BoxedReaderChild::File(f) => f.read(buf),
            BoxedReaderChild::Remote(r) => r.read(buf),
        }
    }
}

/// Unified writer that abstracts over local files and remote storage.
/// 
/// Provides a consistent interface for persistent storage, allowing 
/// the pipeline to save results across different environments.
pub enum BoxedWriter {
    File(std::io::BufWriter<File>),
    Remote(RemoteWriter),
}

pub struct RemoteWriter {
    store: Arc<dyn ObjectStore>,
    path: Path,
    buffer: Vec<u8>,
    multipart: Option<Box<dyn object_store::MultipartUpload>>,
}

impl RemoteWriter {
    pub fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self {
            store,
            path,
            buffer: Vec::with_capacity(WRITE_BUFFER_SIZE),
            multipart: None,
        }
    }
}

impl Write for RemoteWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        if self.buffer.len() >= WRITE_BUFFER_SIZE {
            self.flush()?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.buffer.len() < WRITE_BUFFER_SIZE {
            return Ok(());
        }
        self.force_flush()
    }
}

impl RemoteWriter {
    fn force_flush(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let store = self.store.clone();
        let path = self.path.clone();
        let data = std::mem::take(&mut self.buffer);
        let mut m_opt = self.multipart.take();

        m_opt = get_runtime().block_on(async move {
            let mut m = match m_opt {
                Some(m) => m,
                None => store.put_multipart(&path).await.map_err(std::io::Error::other)?,
            };
            m.put_part(data.into()).await.map_err(std::io::Error::other)?;
            Ok::<Option<Box<dyn object_store::MultipartUpload>>, std::io::Error>(Some(m))
        })?;
        
        self.multipart = m_opt;
        Ok(())
    }

    pub fn close(&mut self) -> std::io::Result<()> {
        self.force_flush()?;
        if let Some(mut m) = self.multipart.take() {
            get_runtime().block_on(async move {
                m.complete().await.map_err(std::io::Error::other)
            })?;
        }
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

impl BoxedWriter {
    pub fn close(&mut self) -> std::io::Result<()> {
        match self {
            BoxedWriter::File(f) => f.flush(),
            BoxedWriter::Remote(r) => r.close(),
        }
    }
}

pub struct SharedWriter(pub Arc<std::sync::Mutex<BoxedWriter>>);

impl Write for SharedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.lock().unwrap().flush()
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

    #[test]
    fn test_parquet_remote_reader_integration() {
        use object_store::memory::InMemory;
        use parquet::arrow::ArrowWriter;
        use arrow::array::{Int32Array, ArrayRef};
        use arrow::record_batch::RecordBatch;
        use arrow::datatypes::{Schema, Field, DataType};

        let store = Arc::new(InMemory::new());
        let path = Path::from("test.parquet");
        
        // 1. Setup: Create a real parquet file in memory using a temporary runtime
        {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
                let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef]).unwrap();
                
                let mut buffer = Vec::new();
                {
                    let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();
                    writer.write(&batch).unwrap();
                    writer.close().unwrap();
                }
                store.put(&path, buffer.into()).await.unwrap();
            });
        }

        // 2. Verification: Read it back using BoxedReader::Remote in a clean thread
        // We use spawn to ensure we are NOT in a tokio runtime context when we call RemoteReader
        std::thread::spawn(move || {
            let remote_reader = RemoteReader::new(store, path);
            let boxed_reader = BoxedReader::Remote(remote_reader);
            
            let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(boxed_reader).unwrap();
            let mut reader = builder.build().unwrap();
            
            let read_batch = reader.next().unwrap().unwrap();
            assert_eq!(read_batch.num_rows(), 3);
        }).join().unwrap();
    }
}
