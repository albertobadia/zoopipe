use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::fs::File;
use std::io::{BufReader, Write};
use std::sync::Mutex;
use serde_json::Value;
use serde::Serialize;
use crate::io::BoxedReader;
use crate::utils::{serde_to_py, python_to_serde, wrap_py_err};

#[pyclass]
pub struct JSONReader {
    pub(crate) iter: Mutex<serde_json::StreamDeserializer<'static, serde_json::de::IoRead<BoxedReader>, Value>>,
    pub(crate) array_iter: Mutex<Option<std::vec::IntoIter<Value>>>,
    pub(crate) position: Mutex<usize>,
    pub(crate) status_pending: Py<PyAny>,
}

#[pymethods]
impl JSONReader {
    #[new]
    fn new(py: Python<'_>, path: String) -> PyResult<Self> {
        let file = File::open(path).map_err(wrap_py_err)?;
        let buf_reader = BoxedReader::File(BufReader::new(file));
        
        let models = py.import("zoopipe.models.core")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(JSONReader {
            iter: Mutex::new(serde_json::Deserializer::from_reader(buf_reader).into_iter::<Value>()),
            array_iter: Mutex::new(None), position: Mutex::new(0),
            status_pending,
        })
    }

    #[staticmethod]
    fn from_bytes(py: Python<'_>, data: Vec<u8>) -> PyResult<Self> {
        let buf_reader = BoxedReader::Cursor(std::io::Cursor::new(data));
        
        let models = py.import("zoopipe.models.core")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(JSONReader {
            iter: Mutex::new(serde_json::Deserializer::from_reader(buf_reader).into_iter::<Value>()),
            array_iter: Mutex::new(None), position: Mutex::new(0),
            status_pending,
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(slf: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyAny>>> {
        let mut array_iter_lock = slf.array_iter.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut pos_lock = slf.position.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        
        let py = slf.py();
        if let Some(ref mut ai) = *array_iter_lock {
            if let Some(val) = ai.next() {
                let current_pos = *pos_lock;
                *pos_lock += 1;
                return Ok(Some(wrap_in_envelope(py, val, current_pos, &slf.status_pending.bind(py))?));
            } else {
                *array_iter_lock = None;
            }
        }

        let mut iter = slf.iter.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        match iter.next() {
            Some(Ok(value)) => {
                if let Value::Array(arr) = value {
                    let mut ai = arr.into_iter();
                    if let Some(val) = ai.next() {
                        *array_iter_lock = Some(ai);
                        let current_pos = *pos_lock;
                        *pos_lock += 1;
                        return Ok(Some(wrap_in_envelope(py, val, current_pos, &slf.status_pending.bind(py))?));
                    } else {
                        return Ok(None);
                    }
                }
                let current_pos = *pos_lock;
                *pos_lock += 1;
                Ok(Some(wrap_in_envelope(py, value, current_pos, &slf.status_pending.bind(py))?))
            }
            Some(Err(e)) => Err(wrap_py_err(e)),
            None => Ok(None),
        }
    }
}

fn wrap_in_envelope<'py>(py: Python<'py>, value: Value, position: usize, status_pending: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    let envelope = pyo3::types::PyDict::new(py);
    envelope.set_item(pyo3::intern!(py, "id"), uuid::Uuid::new_v4().to_string())?;
    envelope.set_item(pyo3::intern!(py, "status"), status_pending)?;
    envelope.set_item(pyo3::intern!(py, "raw_data"), serde_to_py(py, value)?)?;
    envelope.set_item(pyo3::intern!(py, "metadata"), pyo3::types::PyDict::new(py))?;
    envelope.set_item(pyo3::intern!(py, "position"), position)?;
    envelope.set_item(pyo3::intern!(py, "errors"), pyo3::types::PyList::empty(py))?;
    Ok(envelope.into_any())
}

#[pyclass]
pub struct JSONWriter {
    writer: Mutex<std::io::BufWriter<File>>,
    is_first_item: Mutex<bool>,
    format: String,
    indent: Option<usize>,
    include_metadata: bool,
}

#[pymethods]
impl JSONWriter {
    #[new]
    #[pyo3(signature = (path, format="array".to_string(), indent=None, include_metadata=false))]
    fn new(
        _py: Python<'_>,
        path: String,
        format: String,
        indent: Option<usize>,
        include_metadata: bool,
    ) -> PyResult<Self> {
        let file = File::create(path).map_err(wrap_py_err)?;
        let mut writer = std::io::BufWriter::new(file);

        if format == "array" {
            writer.write_all(b"[").map_err(wrap_py_err)?;
            if indent.is_some() {
                writer.write_all(b"\n").map_err(wrap_py_err)?;
            }
        }

        Ok(JSONWriter {
            writer: Mutex::new(writer),
            is_first_item: Mutex::new(true),
            format,
            indent,
            include_metadata,
        })
    }

    fn write(&self, py: Python<'_>, entry: Bound<'_, PyAny>) -> PyResult<()> {
        let mut writer = self.writer.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut is_first = self.is_first_item.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;

        self.write_internal(py, entry, &mut writer, &mut is_first)
    }

    fn write_batch(&self, py: Python<'_>, entries: Bound<'_, pyo3::types::PyList>) -> PyResult<()> {
        let mut writer = self.writer.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut is_first = self.is_first_item.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        
        for entry in entries.iter() {
            self.write_internal(py, entry, &mut writer, &mut is_first)?;
        }
        Ok(())
    }

    fn flush(&self) -> PyResult<()> {
        let mut writer = self.writer.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        writer.flush().map_err(wrap_py_err)?;
        Ok(())
    }

    fn close(&self) -> PyResult<()> {
        let mut writer = self.writer.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
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
        py: Python<'_>,
        entry: Bound<'_, PyAny>,
        writer: &mut std::io::BufWriter<File>,
        is_first: &mut bool,
    ) -> PyResult<()> {
        let entry_dict = entry.cast::<pyo3::types::PyDict>()?;
        
        let id = entry_dict.get_item(pyo3::intern!(py, "id"))?.ok_or_else(|| PyRuntimeError::new_err("Missing 'id'"))?;
        let status = entry_dict.get_item(pyo3::intern!(py, "status"))?.ok_or_else(|| PyRuntimeError::new_err("Missing 'status'"))?;
        let position = entry_dict.get_item(pyo3::intern!(py, "position"))?.ok_or_else(|| PyRuntimeError::new_err("Missing 'position'"))?;
        
        let record_py = if let Ok(Some(val)) = entry_dict.get_item(pyo3::intern!(py, "validated_data")) {
            val
        } else {
            entry_dict.get_item(pyo3::intern!(py, "raw_data"))?.ok_or_else(|| PyRuntimeError::new_err("Missing 'raw_data'"))?
        };

        let mut map = serde_json::Map::with_capacity(5);
        map.insert("id".to_string(), python_to_serde(py, id)?);
        map.insert("status".to_string(), python_to_serde(py, status)?);
        map.insert("position".to_string(), python_to_serde(py, position)?);
        
        if self.include_metadata {
            if let Some(meta) = entry_dict.get_item(pyo3::intern!(py, "metadata"))? {
                map.insert("metadata".to_string(), python_to_serde(py, meta)?);
            }
        }
        
        map.insert("data".to_string(), python_to_serde(py, record_py)?);
        let value = Value::Object(map);

        let json_str = if let Some(indent_size) = self.indent {
            let mut buf = Vec::new();
            let indent_bytes = b" ".repeat(indent_size);
            let formatter = serde_json::ser::PrettyFormatter::with_indent(&indent_bytes);
            let mut ser = serde_json::Serializer::with_formatter(&mut buf, formatter);
            value.serialize(&mut ser).map_err(wrap_py_err)?;
            String::from_utf8(buf).map_err(wrap_py_err)?
        } else {
            serde_json::to_string(&value).map_err(wrap_py_err)?
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

