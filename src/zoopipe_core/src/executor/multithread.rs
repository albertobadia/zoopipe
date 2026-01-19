use pyo3::prelude::*;
use pyo3::types::PyList;
use super::strategy::{ExecutionStrategy, SingleThreadStrategy, ParallelStrategy};

/// Executor that processes batches sequentially on a single thread.
/// 
/// This is the simplest execution strategy and is often best for 
/// low-latency pipelines or when processing overhead is minimal.
#[pyclass]
pub struct SingleThreadExecutor {
    strategy: SingleThreadStrategy,
    batch_size: usize,
}

#[pymethods]
impl SingleThreadExecutor {
    /// Create a new SingleThreadExecutor.
    /// 
    /// Args:
    ///     batch_size: Number of records to process in a single batch.
    #[new]
    #[pyo3(signature = (batch_size=1000))]
    fn new(batch_size: usize) -> Self {
        Self {
            strategy: SingleThreadStrategy,
            batch_size,
        }
    }

    pub fn get_batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn get_concurrency(&self) -> usize {
        1
    }
}

impl SingleThreadExecutor {
    pub fn process_batches<'py>(
        &self,
        py: Python<'py>,
        batches: Vec<Bound<'py, PyList>>,
        processor: &Bound<'py, PyAny>,
    ) -> PyResult<Vec<Bound<'py, PyAny>>> {
        self.strategy.process_batches(py, batches, processor)
    }
}

/// Executor that processes batches in parallel using a thread pool.
/// 
/// This strategy is highly effective for CPU-bound validation tasks or 
/// complex hook executions, as it scales across available cores.
#[pyclass]
pub struct MultiThreadExecutor {
    strategy: ParallelStrategy,
    batch_size: usize,
}

#[pymethods]
impl MultiThreadExecutor {
    /// Create a new MultiThreadExecutor.
    /// 
    /// Args:
    ///     max_workers: Number of threads to use (defaults to CPU count).
    ///     batch_size: Number of records to process per batch on each thread.
    #[new]
    #[pyo3(signature = (max_workers=None, batch_size=1000))]
    fn new(max_workers: Option<usize>, batch_size: usize) -> Self {
        let num_threads = max_workers.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        });
        
        Self {
            strategy: ParallelStrategy::new(num_threads),
            batch_size,
        }
    }

    pub fn get_batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn get_concurrency(&self) -> usize {
        self.strategy.num_threads()
    }
}

impl MultiThreadExecutor {
    pub fn process_batches<'py>(
        &self,
        py: Python<'py>,
        batches: Vec<Bound<'py, PyList>>,
        processor: &Bound<'py, PyAny>,
    ) -> PyResult<Vec<Bound<'py, PyAny>>> {
        self.strategy.process_batches(py, batches, processor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_thread_executor_default_batch_size() {
        let executor = SingleThreadExecutor::new(1000);
        assert_eq!(executor.get_batch_size(), 1000);
    }

    #[test]
    fn test_single_thread_executor_custom_batch_size() {
        let executor = SingleThreadExecutor::new(500);
        assert_eq!(executor.get_batch_size(), 500);
    }

    #[test]
    fn test_multi_thread_executor_default_batch_size() {
        let executor = MultiThreadExecutor::new(None, 1000);
        assert_eq!(executor.get_batch_size(), 1000);
    }

    #[test]
    fn test_multi_thread_executor_custom_batch_size() {
        let executor = MultiThreadExecutor::new(Some(4), 2000);
        assert_eq!(executor.get_batch_size(), 2000);
    }

    #[test]
    fn test_multi_thread_executor_auto_detect_threads() {
        let executor = MultiThreadExecutor::new(None, 1000);
        assert_eq!(executor.get_batch_size(), 1000);
    }

    #[test]
    fn test_multi_thread_executor_custom_threads() {
        let executor = MultiThreadExecutor::new(Some(8), 1000);
        assert_eq!(executor.get_batch_size(), 1000);
    }
}
