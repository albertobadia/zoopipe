use crate::error::PipeError;
use crossbeam_channel::{Receiver, Sender, bounded};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyList};
use std::sync::Mutex;

/// Adapter that allows any Python iterable to be used as a pipeline source.
#[pyclass]
pub struct PyGeneratorReader {
    iterable: Py<PyAny>,
    iterator: Mutex<Option<Py<PyAny>>>,
    position: Mutex<usize>,
    status_pending: Py<PyAny>,
    generate_ids: bool,
    keys: InternedKeys,
}

use crate::utils::interning::InternedKeys;

#[pymethods]
impl PyGeneratorReader {
    #[new]
    #[pyo3(signature = (iterable, generate_ids=true))]
    fn new(py: Python<'_>, iterable: Py<PyAny>, generate_ids: bool) -> PyResult<Self> {
        let models = py.import("zoopipe.structs")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(PyGeneratorReader {
            iterable,
            iterator: Mutex::new(None),
            position: Mutex::new(0),
            status_pending,
            generate_ids,
            keys: InternedKeys::new(py),
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __next__(slf: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyAny>>> {
        slf.next_internal(slf.py())
    }

    pub fn read_batch<'py>(
        &self,
        py: Python<'py>,
        batch_size: usize,
    ) -> PyResult<Option<Bound<'py, PyList>>> {
        let batch = PyList::empty(py);

        for _ in 0..batch_size {
            if let Some(item) = self.next_internal(py)? {
                batch.append(item)?;
            } else {
                break;
            }
        }

        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }
}

impl PyGeneratorReader {
    fn next_internal<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let mut iter_lock = self.iterator.lock().map_err(|_| PipeError::MutexLock)?;

        if iter_lock.is_none() {
            let iter = self.iterable.bind(py).try_iter()?;
            *iter_lock = Some(iter.into());
        }

        let iter_bound = iter_lock
            .as_ref()
            .expect("Iterator should be initialized after is_none() check")
            .bind(py);
        let iterator = iter_bound.cast::<pyo3::types::PyIterator>()?;

        match iterator.clone().next() {
            Some(item_res) => {
                let raw_data = item_res?;
                let mut pos = self.position.lock().map_err(|_| PipeError::MutexLock)?;
                let current_pos = *pos;
                *pos += 1;

                let envelope = crate::utils::wrap_in_envelope(
                    py,
                    &self.keys,
                    raw_data,
                    self.status_pending.bind(py).clone(),
                    current_pos,
                    self.generate_ids,
                )?;

                Ok(Some(envelope.into_any()))
            }
            None => Ok(None),
        }
    }
}

/// flexible output adapter that acts as a synchronized bridge back to Python.
///
/// It utilizes a bounded channel to allow the pipeline to push processed
/// results into a Python-accessible iterator, enabling custom post-processing.
#[pyclass]
pub struct PyGeneratorWriter {
    sender: Mutex<Option<Sender<Py<PyAny>>>>,
    receiver: Receiver<Py<PyAny>>,
}

#[pymethods]
impl PyGeneratorWriter {
    #[new]
    #[pyo3(signature = (queue_size=1000))]
    fn new(queue_size: usize) -> Self {
        let (s, r) = bounded(queue_size);
        PyGeneratorWriter {
            sender: Mutex::new(Some(s)),
            receiver: r,
        }
    }

    pub fn write(&self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        let sender = {
            let lock = self.sender.lock().map_err(|_| PipeError::MutexLock)?;
            lock.clone()
        };

        if let Some(s) = sender {
            let val = data.unbind();
            py.detach(|| {
                s.send(val)
                    .map_err(|_| PyRuntimeError::new_err("Writer channel is closed"))
            })
        } else {
            Err(PyRuntimeError::new_err("Writer is closed"))
        }
    }

    pub fn write_batch(&self, py: Python<'_>, entries: Bound<'_, PyAny>) -> PyResult<()> {
        let iterator = entries.try_iter()?;
        for entry in iterator {
            self.write(py, entry?)?;
        }
        Ok(())
    }

    pub fn flush(&self) -> PyResult<()> {
        Ok(())
    }

    pub fn close(&self) -> PyResult<()> {
        let mut lock = self.sender.lock().map_err(|_| PipeError::MutexLock)?;
        *lock = None;
        Ok(())
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(slf: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyAny>>> {
        let py = slf.py();
        let receiver = slf.receiver.clone();

        loop {
            let res = py.detach(|| receiver.recv_timeout(std::time::Duration::from_millis(250)));

            match res {
                Ok(item) => return Ok(Some(item.into_bound(py))),
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    py.check_signals()?;
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return Ok(None),
            }
        }
    }
}
