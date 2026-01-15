use crossbeam_channel::Receiver;
use std::thread;

pub enum SmartReaderIter<T> {
    Sync(Box<dyn Iterator<Item = Result<T, String>> + Send>),
    Threaded(Receiver<Result<T, String>>),
}

pub struct SmartReader<T> {
    iter: SmartReaderIter<T>,
}

impl<T: Send + 'static> SmartReader<T> {
    pub fn new<S, F, I>(path: &str, source: S, parser: F) -> Self
    where
        S: Send + 'static,
        F: FnOnce(S) -> I + Send + 'static,
        I: Iterator<Item = Result<T, String>> + Send + 'static,
    {
        if path.starts_with("s3://") {
            let (tx, rx) = crossbeam_channel::bounded(1000);
            
            thread::spawn(move || {
                let iter = parser(source);
                for item in iter {
                    if tx.send(item).is_err() {
                        return;
                    }
                }
            });
            
            SmartReader {
                iter: SmartReaderIter::Threaded(rx),
            }
        } else {
            let iter = parser(source);
            SmartReader {
                iter: SmartReaderIter::Sync(Box::new(iter)),
            }
        }
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
