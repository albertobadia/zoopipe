use crossbeam_channel::Receiver;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

pub enum SmartReaderIter<T> {
    Sync(Box<dyn Iterator<Item = Result<T, String>> + Send>),
    Threaded(Receiver<Result<T, String>>),
}

/// A hybrid I/O reader that optimizes for both local and remote sources.
///
/// SmartReader offloads I/O and parsing/decompression to a background thread
/// to allow the main thread (holding the GIL) and other threads (e.g. Writer)
/// to overlap execution, maximizing multicore throughput.
pub struct SmartReader<T> {
    iter: SmartReaderIter<T>,
    shutdown: Arc<AtomicBool>,
}

impl<T: Send + 'static> SmartReader<T> {
    pub fn new<S, F, I>(_path: &str, source: S, parser: F) -> Self
    where
        S: Send + 'static,
        F: FnOnce(S) -> I + Send + 'static,
        I: Iterator<Item = Result<T, String>> + Send + 'static,
    {
        // Always use threaded reader to unblock GIL during I/O and parsing/decompression.
        // Using a small buffer (8) to keep pipeline fed without consuming too much RAM
        // (especially for RecordBatches).
        let (tx, rx) = crossbeam_channel::bounded(8);
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_thread = shutdown.clone();

        thread::spawn(move || {
            let iter = parser(source);
            for item in iter {
                if shutdown_thread.load(Ordering::Relaxed) || tx.send(item).is_err() {
                    return;
                }
            }
        });

        SmartReader {
            iter: SmartReaderIter::Threaded(rx),
            shutdown,
        }
    }
}

impl<T> Drop for SmartReader<T> {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

impl<T: Send + 'static> Iterator for SmartReader<T> {
    type Item = Result<T, String>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.iter {
            SmartReaderIter::Sync(iter) => iter.next(),
            SmartReaderIter::Threaded(rx) => rx.recv().ok(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_smart_reader_basic_iteration() {
        let data = vec![1, 2, 3, 4, 5];
        let reader = SmartReader::new("test", data.clone(), |d| d.into_iter().map(Ok));

        let results: Vec<i32> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(results, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_smart_reader_with_errors() {
        let data = vec![1, 2, 3];
        let reader = SmartReader::new("test", data, |d| {
            d.into_iter().enumerate().map(|(i, v)| {
                if i == 1 {
                    Err(format!("Error at position {}", i))
                } else {
                    Ok(v)
                }
            })
        });

        let results: Vec<Result<i32, String>> = reader.collect();
        assert_eq!(results.len(), 3);
        assert!(results[0].is_ok());
        assert!(results[1].is_err());
        assert!(results[2].is_ok());
    }

    #[test]
    fn test_smart_reader_empty_source() {
        let data: Vec<i32> = vec![];
        let reader = SmartReader::new("test", data, |d| d.into_iter().map(Ok));

        let results: Vec<i32> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_smart_reader_large_dataset() {
        let data: Vec<i32> = (0..1000).collect();
        let reader = SmartReader::new("test", data.clone(), |d| d.into_iter().map(Ok));

        let results: Vec<i32> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(results.len(), 1000);
        assert_eq!(results[0], 0);
        assert_eq!(results[999], 999);
    }

    #[test]
    fn test_smart_reader_shutdown_on_drop() {
        let data: Vec<i32> = (0..100).collect();
        let reader = SmartReader::new("test", data, |d| d.into_iter().map(Ok));

        let shutdown_flag = reader.shutdown.clone();
        assert!(!shutdown_flag.load(Ordering::Relaxed));

        drop(reader);

        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(shutdown_flag.load(Ordering::Relaxed));
    }

    #[test]
    fn test_smart_reader_partial_consumption() {
        let data: Vec<i32> = (0..100).collect();
        let mut reader = SmartReader::new("test", data, |d| d.into_iter().map(Ok));

        let first = reader.next();
        assert_eq!(first, Some(Ok(0)));

        let second = reader.next();
        assert_eq!(second, Some(Ok(1)));

        drop(reader);
    }

    #[test]
    fn test_smart_reader_with_string_data() {
        let data = vec!["hello", "world", "rust"];
        let reader = SmartReader::new("test", data, |d| d.into_iter().map(|s| Ok(s.to_string())));

        let results: Vec<String> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(results, vec!["hello", "world", "rust"]);
    }

    #[test]
    fn test_smart_reader_buffered_channel() {
        let data: Vec<i32> = (0..20).collect();
        let reader = SmartReader::new("test", data, |d| {
            d.into_iter().map(|v| {
                std::thread::sleep(std::time::Duration::from_micros(100));
                Ok(v)
            })
        });

        let results: Vec<i32> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(results.len(), 20);
    }

    #[test]
    fn test_smart_reader_with_io_cursor() {
        let csv_data = "a,b,c\n1,2,3\n4,5,6";
        let cursor = Cursor::new(csv_data.as_bytes());

        let reader = SmartReader::new("test.csv", cursor, |c| {
            std::io::BufRead::lines(c).map(|line| line.map_err(|e| e.to_string()))
        });

        let lines: Vec<String> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "a,b,c");
        assert_eq!(lines[1], "1,2,3");
        assert_eq!(lines[2], "4,5,6");
    }
}
