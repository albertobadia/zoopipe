use pyo3::prelude::*;
use pyo3::types::PyList;
use super::strategy::{ExecutionStrategy, SingleThreadStrategy, ParallelStrategy};

#[pyclass]
pub struct SingleThreadExecutor {
    strategy: SingleThreadStrategy,
    batch_size: usize,
}

#[pymethods]
impl SingleThreadExecutor {
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

#[pyclass]
pub struct MultiThreadExecutor {
    strategy: ParallelStrategy,
    batch_size: usize,
}

#[pymethods]
impl MultiThreadExecutor {
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
