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
