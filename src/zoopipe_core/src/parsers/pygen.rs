use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyList, PyString};
use pyo3::exceptions::PyRuntimeError;
use std::sync::Mutex;
use crossbeam_channel::{bounded, Sender, Receiver};
use crate::error::PipeError;

#[pyclass]
pub struct PyGeneratorReader {
    iterable: Py<PyAny>,
    iterator: Mutex<Option<Py<PyAny>>>,
    position: Mutex<usize>,
    status_pending: Py<PyAny>,
    generate_ids: bool,
    id_key: Py<PyString>,
    status_key: Py<PyString>,
    raw_data_key: Py<PyString>,
    metadata_key: Py<PyString>,
    position_key: Py<PyString>,
    errors_key: Py<PyString>,
}

#[pymethods]
impl PyGeneratorReader {
    #[new]
    #[pyo3(signature = (iterable, generate_ids=true))]
    fn new(py: Python<'_>, iterable: Py<PyAny>, generate_ids: bool) -> PyResult<Self> {
        let models = py.import("zoopipe.report")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(PyGeneratorReader {
            iterable,
            iterator: Mutex::new(None),
            position: Mutex::new(0),
            status_pending,
            generate_ids,
            id_key: pyo3::intern!(py, "id").clone().unbind(),
            status_key: pyo3::intern!(py, "status").clone().unbind(),
            raw_data_key: pyo3::intern!(py, "raw_data").clone().unbind(),
            metadata_key: pyo3::intern!(py, "metadata").clone().unbind(),
            position_key: pyo3::intern!(py, "position").clone().unbind(),
            errors_key: pyo3::intern!(py, "errors").clone().unbind(),
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __next__(slf: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyAny>>> {
        let py = slf.py();
        let mut iter_lock = slf.iterator.lock().map_err(|_| PipeError::MutexLock)?;
        
        if iter_lock.is_none() {
            let iter = slf.iterable.bind(py).try_iter()?;
            *iter_lock = Some(iter.into());
        }

        let iter_bound = iter_lock.as_ref()
            .expect("Iterator should be initialized after is_none() check")
            .bind(py);
        let iterator = iter_bound.cast::<pyo3::types::PyIterator>()?;
        
        match iterator.clone().next() {
            Some(item_res) => {
                let raw_data = item_res?;
                let mut pos = slf.position.lock().map_err(|_| PipeError::MutexLock)?;
                let current_pos = *pos;
                *pos += 1;

                let envelope = PyDict::new(py);
                
                let id = if slf.generate_ids {
                    crate::utils::generate_entry_id(py)?
                } else {
                    py.None().into_bound(py)
                };

                envelope.set_item(slf.id_key.bind(py), id)?;
                envelope.set_item(slf.status_key.bind(py), slf.status_pending.bind(py))?;
                envelope.set_item(slf.raw_data_key.bind(py), raw_data)?;
                envelope.set_item(slf.metadata_key.bind(py), PyDict::new(py))?;
                envelope.set_item(slf.position_key.bind(py), current_pos)?;
                envelope.set_item(slf.errors_key.bind(py), PyList::empty(py))?;

                Ok(Some(envelope.into_any()))
            }
            None => Ok(None),
        }
    }
}

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
                s.send(val).map_err(|_| PyRuntimeError::new_err("Writer channel is closed"))
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
        
        let res = py.detach(|| {
            receiver.recv()
        });

        match res {
            Ok(item) => Ok(Some(item.into_bound(py))),
            Err(_) => Ok(None),
        }
    }
}
