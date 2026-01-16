use pyo3::prelude::*;
use std::fs::File;
use std::io::{BufReader, Write};
use std::sync::Mutex;
use serde_json::Value;
use serde::Serialize;
use crate::io::{BoxedReader, SmartReader};
use crate::utils::{serde_to_py, wrap_py_err, PySerializable};
use crate::error::PipeError;
use pyo3::types::{PyAnyMethods, PyDict, PyList};

use crate::utils::interning::InternedKeys;

struct JSONReaderState {
    reader: SmartReader<Value>,
    array_iter: Option<std::vec::IntoIter<Value>>,
    position: usize,
}

/// Fast JSON reader that handles both single objects and arrays of objects.
/// 
/// It streams data efficiently using a background thread and supports
/// complex nested structures by bridging Serde with PyO3.
#[pyclass]
pub struct JSONReader {
    state: Mutex<JSONReaderState>,
    pub(crate) status_pending: Py<PyAny>,
    keys: InternedKeys,
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
        
        let reader = SmartReader::new(
            &path,
            boxed_reader,
            |r| serde_json::Deserializer::from_reader(r)
                .into_iter::<Value>()
                .map(|res| res.map_err(|e| format!("{}", e)))
        );
        
        let models = py.import("zoopipe.report")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(JSONReader {
            state: Mutex::new(JSONReaderState {
                reader,
                array_iter: None,
                position: 0,
            }),
            status_pending,
            keys: InternedKeys::new(py),
        })
    }

    #[staticmethod]
    fn from_bytes(py: Python<'_>, data: Vec<u8>) -> PyResult<Self> {
        let boxed_reader = BoxedReader::Cursor(std::io::Cursor::new(data));
        
        let reader = SmartReader::new(
            "",
            boxed_reader,
            |r| serde_json::Deserializer::from_reader(r)
                .into_iter::<Value>()
                .map(|res| res.map_err(|e| format!("{}", e)))
        );
        
        let models = py.import("zoopipe.report")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(JSONReader {
            state: Mutex::new(JSONReaderState {
                reader,
                array_iter: None,
                position: 0,
            }),
            status_pending,
            keys: InternedKeys::new(py),
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __next__(slf: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyAny>>> {
        let mut state = slf.state.lock().map_err(|_| PipeError::MutexLock)?;
        slf.next_internal(slf.py(), &mut state)
    }

    pub fn read_batch<'py>(&self, py: Python<'py>, batch_size: usize) -> PyResult<Option<Bound<'py, PyList>>> {
        let mut state = self.state.lock().map_err(|_| PipeError::MutexLock)?;
        let batch = PyList::empty(py);
        
        for _ in 0..batch_size {
            if let Some(item) = self.next_internal(py, &mut state)? {
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

impl JSONReader {
    fn next_internal<'py>(&self, py: Python<'py>, state: &mut JSONReaderState) -> PyResult<Option<Bound<'py, PyAny>>> {
        if let Some(ref mut ai) = state.array_iter {
            if let Some(val) = ai.next() {
                let current_pos = state.position;
                state.position += 1;
                return Ok(Some(self.wrap_in_envelope(py, val, current_pos)?));
            } else {
                state.array_iter = None;
            }
        }

        match state.reader.next() {
            Some(Ok(value)) => {
                if let Value::Array(arr) = value {
                    let mut ai = arr.into_iter();
                    if let Some(val) = ai.next() {
                        let current_pos = state.position;
                        state.position += 1;
                        state.array_iter = Some(ai);
                        return Ok(Some(self.wrap_in_envelope(py, val, current_pos)?));
                    } else {
                        return Ok(None);
                    }
                }
                let current_pos = state.position;
                state.position += 1;
                Ok(Some(self.wrap_in_envelope(py, value, current_pos)?))
            }
            Some(Err(e)) => {
                let pos = state.position + 1;
                Err(PipeError::Other(format!("JSON parse error at position {}: {}", pos, e)).into())
            }
            None => Ok(None),
        }
    }
    fn wrap_in_envelope<'py>(&self, py: Python<'py>, value: Value, position: usize) -> PyResult<Bound<'py, PyAny>> {
        let envelope = PyDict::new(py);
        envelope.set_item(self.keys.get_id(py), crate::utils::generate_entry_id(py)?)?;
        envelope.set_item(self.keys.get_status(py), self.status_pending.bind(py))?;
        envelope.set_item(self.keys.get_raw_data(py), serde_to_py(py, value)?)?;
        envelope.set_item(self.keys.get_metadata(py), PyDict::new(py))?;
        envelope.set_item(self.keys.get_position(py), position)?;
        envelope.set_item(self.keys.get_errors(py), PyList::empty(py))?;
        Ok(envelope.into_any())
    }
}

/// flexible JSON writer that supports standard arrays and JSONLines output.
/// 
/// It provides high-performance serialization of Python objects directly 
/// into JSON format, with support for pretty-printing and chunked I/O.
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
        if !*is_first && self.format != "jsonl" {
            writer.write_all(b",").map_err(wrap_py_err)?;
            if self.indent.is_some() {
                writer.write_all(b"\n").map_err(wrap_py_err)?;
            }
        }

        let wrapper = PySerializable(data);

        if let Some(indent_size) = self.indent {
            let indent_bytes = b" ".repeat(indent_size);
            let formatter = serde_json::ser::PrettyFormatter::with_indent(&indent_bytes);
            let mut ser = serde_json::Serializer::with_formatter(&mut *writer, formatter);
            wrapper.serialize(&mut ser).map_err(wrap_py_err)?;
        } else {
            serde_json::to_writer(&mut *writer, &wrapper).map_err(wrap_py_err)?;
        }

        if self.format == "jsonl" {
            writer.write_all(b"\n").map_err(wrap_py_err)?;
        }
        
        *is_first = false;
        Ok(())
    }
}
