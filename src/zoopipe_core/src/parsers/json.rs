use pyo3::prelude::*;
use std::fs::File;
use std::io::{BufReader, Write};
use std::sync::Mutex;
use serde_json::Value;
use serde::Serialize;
use crate::io::BoxedReader;
use crate::utils::{serde_to_py, wrap_py_err, PySerializable};
use crate::error::PipeError;
use pyo3::types::{PyAnyMethods, PyDict, PyList, PyString};

struct JSONReaderState {
    iter: serde_json::StreamDeserializer<'static, serde_json::de::IoRead<BoxedReader>, Value>,
    array_iter: Option<std::vec::IntoIter<Value>>,
    position: usize,
}

#[pyclass]
pub struct JSONReader {
    state: Mutex<JSONReaderState>,
    pub(crate) status_pending: Py<PyAny>,
    id_key: Py<PyString>,
    status_key: Py<PyString>,
    raw_data_key: Py<PyString>,
    metadata_key: Py<PyString>,
    position_key: Py<PyString>,
    errors_key: Py<PyString>,
}

#[pymethods]
impl JSONReader {
    #[new]
    fn new(py: Python<'_>, path: String) -> PyResult<Self> {
        use crate::io::storage::StorageController;
        use object_store::path::Path;
        use crate::io::RemoteReader;

        let controller = StorageController::new(&path).map_err(wrap_py_err)?;
        let boxed_reader = if path.starts_with("s3://") {
            BoxedReader::Remote(RemoteReader::new(controller.store(), Path::from(controller.path())))
        } else {
            let file = File::open(&path).map_err(wrap_py_err)?;
            BoxedReader::File(BufReader::new(file))
        };
        
        let models = py.import("zoopipe.report")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(JSONReader {
            state: Mutex::new(JSONReaderState {
                iter: serde_json::Deserializer::from_reader(boxed_reader).into_iter::<Value>(),
                array_iter: None,
                position: 0,
            }),
            status_pending,
            id_key: pyo3::intern!(py, "id").clone().unbind(),
            status_key: pyo3::intern!(py, "status").clone().unbind(),
            raw_data_key: pyo3::intern!(py, "raw_data").clone().unbind(),
            metadata_key: pyo3::intern!(py, "metadata").clone().unbind(),
            position_key: pyo3::intern!(py, "position").clone().unbind(),
            errors_key: pyo3::intern!(py, "errors").clone().unbind(),
        })
    }

    #[staticmethod]
    fn from_bytes(py: Python<'_>, data: Vec<u8>) -> PyResult<Self> {
        let buf_reader = BoxedReader::Cursor(std::io::Cursor::new(data));
        
        let models = py.import("zoopipe.report")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(JSONReader {
            state: Mutex::new(JSONReaderState {
                iter: serde_json::Deserializer::from_reader(buf_reader).into_iter::<Value>(),
                array_iter: None,
                position: 0,
            }),
            status_pending,
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
        let mut state = slf.state.lock().map_err(|_| PipeError::MutexLock)?;
        
        let py = slf.py();
        if let Some(ref mut ai) = state.array_iter {
            if let Some(val) = ai.next() {
                let current_pos = state.position;
                state.position += 1;
                return Ok(Some(slf.wrap_in_envelope(py, val, current_pos)?));
            } else {
                state.array_iter = None;
            }
        }

        match state.iter.next() {
            Some(Ok(value)) => {
                if let Value::Array(arr) = value {
                    let mut ai = arr.into_iter();
                    if let Some(val) = ai.next() {
                        let current_pos = state.position;
                        state.position += 1;
                        state.array_iter = Some(ai);
                        return Ok(Some(slf.wrap_in_envelope(py, val, current_pos)?));
                    } else {
                        return Ok(None);
                    }
                }
                let current_pos = state.position;
                state.position += 1;
                Ok(Some(slf.wrap_in_envelope(py, value, current_pos)?))
            }
            Some(Err(e)) => {
                let pos = state.position + 1;
                Err(PipeError::Other(format!("JSON parse error at position {}: {}", pos, e)).into())
            }
            None => Ok(None),
        }
    }
}

impl JSONReader {
    fn wrap_in_envelope<'py>(&self, py: Python<'py>, value: Value, position: usize) -> PyResult<Bound<'py, PyAny>> {
        let envelope = PyDict::new(py);
        envelope.set_item(self.id_key.bind(py), crate::utils::generate_entry_id(py)?)?;
        envelope.set_item(self.status_key.bind(py), self.status_pending.bind(py))?;
        envelope.set_item(self.raw_data_key.bind(py), serde_to_py(py, value)?)?;
        envelope.set_item(self.metadata_key.bind(py), PyDict::new(py))?;
        envelope.set_item(self.position_key.bind(py), position)?;
        envelope.set_item(self.errors_key.bind(py), PyList::empty(py))?;
        Ok(envelope.into_any())
    }
}

#[pyclass]
pub struct JSONWriter {
    writer: Mutex<crate::io::BoxedWriter>,
    is_first_item: Mutex<bool>,
    format: String,
    indent: Option<usize>,
}

#[pymethods]
impl JSONWriter {
    #[new]
    #[pyo3(signature = (path, format="array".to_string(), indent=None))]
    fn new(
        _py: Python<'_>,
        path: String,
        format: String,
        indent: Option<usize>,
    ) -> PyResult<Self> {
        use crate::io::storage::StorageController;
        use object_store::path::Path;
        use crate::io::{BoxedWriter, RemoteWriter};

        let controller = StorageController::new(&path).map_err(wrap_py_err)?;
        let mut boxed_writer = if path.starts_with("s3://") {
            BoxedWriter::Remote(RemoteWriter::new(controller.store(), Path::from(controller.path())))
        } else {
            let file = File::create(&path).map_err(wrap_py_err)?;
            BoxedWriter::File(std::io::BufWriter::new(file))
        };

        if format == "array" {
            boxed_writer.write_all(b"[").map_err(wrap_py_err)?;
            if indent.is_some() {
                boxed_writer.write_all(b"\n").map_err(wrap_py_err)?;
            }
        }

        Ok(JSONWriter {
            writer: Mutex::new(boxed_writer),
            is_first_item: Mutex::new(true),
            format,
            indent,
        })
    }

    pub fn write(&self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        let mut writer = self.writer.lock().map_err(|_| PipeError::MutexLock)?;
        let mut is_first = self.is_first_item.lock().map_err(|_| PipeError::MutexLock)?;

        self.write_internal(py, data, &mut writer, &mut is_first)
    }

    pub fn write_batch(&self, py: Python<'_>, entries: Bound<'_, PyAny>) -> PyResult<()> {
        let mut writer = self.writer.lock().map_err(|_| PipeError::MutexLock)?;
        let mut is_first = self.is_first_item.lock().map_err(|_| PipeError::MutexLock)?;
        
        let iterator = entries.try_iter()?;
        for entry in iterator {
            self.write_internal(py, entry?, &mut writer, &mut is_first)?;
        }
        Ok(())
    }

    pub fn flush(&self) -> PyResult<()> {
        let mut writer = self.writer.lock().map_err(|_| PipeError::MutexLock)?;
        writer.flush().map_err(wrap_py_err)?;
        Ok(())
    }

    pub fn close(&self) -> PyResult<()> {
        let mut writer = self.writer.lock().map_err(|_| PipeError::MutexLock)?;
        if self.format == "array" {
            if self.indent.is_some() {
                writer.write_all(b"\n").map_err(wrap_py_err)?;
            }
            writer.write_all(b"]").map_err(wrap_py_err)?;
        }
        writer.flush().map_err(wrap_py_err)?;
        Ok(())
    }
}

impl JSONWriter {
    fn write_internal(
        &self,
        _py: Python<'_>,
        data: Bound<'_, PyAny>,
        writer: &mut crate::io::BoxedWriter,
        is_first: &mut bool,
    ) -> PyResult<()> {
        let wrapper = PySerializable(data);

        let json_str = if let Some(indent_size) = self.indent {
            let mut buf = Vec::new();
            let indent_bytes = b" ".repeat(indent_size);
            let formatter = serde_json::ser::PrettyFormatter::with_indent(&indent_bytes);
            let mut ser = serde_json::Serializer::with_formatter(&mut buf, formatter);
            wrapper.serialize(&mut ser).map_err(wrap_py_err)?;
            String::from_utf8(buf).map_err(wrap_py_err)?
        } else {
            serde_json::to_string(&wrapper).map_err(wrap_py_err)?
        };

        if self.format == "jsonl" {
            writer.write_all(json_str.as_bytes()).map_err(wrap_py_err)?;
            writer.write_all(b"\n").map_err(wrap_py_err)?;
        } else {
            if !*is_first {
                writer.write_all(b",").map_err(wrap_py_err)?;
                if self.indent.is_some() {
                    writer.write_all(b"\n").map_err(wrap_py_err)?;
                }
            }
            writer.write_all(json_str.as_bytes()).map_err(wrap_py_err)?;
            *is_first = false;
        }

        Ok(())
    }
}
