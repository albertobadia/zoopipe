use pyo3::prelude::*;
use pyo3::types::PyList;

/// Trait defining the interface for different batch execution strategies.
pub trait ExecutionStrategy: Send + Sync {
    fn process_batches<'py>(
        &self,
        py: Python<'py>,
        batches: Vec<Bound<'py, PyList>>,
        processor: &Bound<'py, PyAny>,
    ) -> PyResult<Vec<Bound<'py, PyAny>>>;
}

/// Strategy that executes batches one-by-one on the main thread.
pub struct SingleThreadStrategy;

impl ExecutionStrategy for SingleThreadStrategy {
    fn process_batches<'py>(
        &self,
        _py: Python<'py>,
        batches: Vec<Bound<'py, PyList>>,
        processor: &Bound<'py, PyAny>,
    ) -> PyResult<Vec<Bound<'py, PyAny>>> {
        batches
            .into_iter()
            .map(|batch| processor.call1((batch,)))
            .collect()
    }
}

/// Strategy that utilizes a Rayon thread pool for concurrent batch processing.
///
/// It coordinates parallel execution while safely managing GIL acquisition
/// for each worker thread during call-backs to Python.
pub struct ParallelStrategy {
    thread_pool: rayon::ThreadPool,
}

impl ParallelStrategy {
    pub fn new(num_threads: usize) -> Self {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads.max(1))
            .build()
            .expect("Failed to create Rayon thread pool");
        Self { thread_pool }
    }

    pub fn num_threads(&self) -> usize {
        self.thread_pool.current_num_threads()
    }
}

impl ExecutionStrategy for ParallelStrategy {
    fn process_batches<'py>(
        &self,
        py: Python<'py>,
        batches: Vec<Bound<'py, PyList>>,
        processor: &Bound<'py, PyAny>,
    ) -> PyResult<Vec<Bound<'py, PyAny>>> {
        use crossbeam_channel::bounded;
        use rayon::prelude::*;

        let batch_count = batches.len();
        let (result_tx, result_rx) = bounded(batch_count);

        let processor_py: Py<PyAny> = processor.clone().unbind();

        let batch_data: Vec<Py<PyList>> = batches.into_iter().map(|b| b.unbind()).collect();

        // Release the GIL while we wait for the Rayon threads to finish
        // This is CRITICAL to avoid deadlocks:
        // Main Thread (GIL) -> waits for Rayon -> Rayon Thread -> needs GIL -> DEADLOCK
        Python::detach(py, || {
            self.thread_pool.scope(|_scope| {
                batch_data
                    .into_par_iter()
                    .enumerate()
                    .for_each(|(idx, batch)| {
                        // Re-acquire GIL per task to execute Python code
                        let result = Python::attach(|py| {
                            let processor = processor_py.bind(py);
                            let batch = batch.bind(py);
                            processor.call1((batch,)).map(|res| res.unbind())
                        });
                        let _ = result_tx.send((idx, result));
                    });
            });
        });

        let mut results: Vec<Option<Py<PyAny>>> = (0..batch_count).map(|_| None).collect();

        for _ in 0..batch_count {
            match result_rx.recv() {
                Ok((idx, Ok(result))) => {
                    results[idx] = Some(result);
                }
                Ok((_, Err(e))) => return Err(e),
                Err(e) => {
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Channel receive error: {}",
                        e
                    )));
                }
            }
        }

        results
            .into_iter()
            .map(|opt| {
                opt.ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Missing batch result"))
            })
            .map(|r| r.map(|py_obj| py_obj.bind(py).clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_strategy_creation() {
        let strategy = ParallelStrategy::new(4);
        assert_eq!(strategy.num_threads(), 4);
    }

    #[test]
    fn test_parallel_strategy_min_threads() {
        let strategy = ParallelStrategy::new(0);
        assert_eq!(strategy.num_threads(), 1);
    }

    #[test]
    fn test_parallel_strategy_custom_threads() {
        let strategy = ParallelStrategy::new(8);
        assert_eq!(strategy.num_threads(), 8);
    }

    #[test]
    fn test_parallel_strategy_large_thread_count() {
        let strategy = ParallelStrategy::new(128);
        assert_eq!(strategy.num_threads(), 128);
    }
}
