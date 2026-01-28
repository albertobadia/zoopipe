use crate::io::{BoxedReader, CountingReader, SmartReader};
use crate::utils::{PySerializable, serde_to_py, wrap_py_err};
use pyo3::prelude::*;
use serde::Serialize;
use serde_json::Value;
use std::io::{BufRead, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use crate::error::PipeError;
use crate::utils::interning::InternedKeys;
use pyo3::types::{PyAnyMethods, PyList};

struct JSONReaderState {
    reader: SmartReader<Value>,
    array_iter: Option<std::vec::IntoIter<Value>>,
    position: usize,
}

// Helper struct for byte-bounded JSON iteration
struct BoundedJsonIter<I> {
    iter: I,
    count: Arc<std::sync::atomic::AtomicU64>,
    end_byte: Option<u64>,
}

impl<I: Iterator<Item = Result<Value, String>>> Iterator for BoundedJsonIter<I> {
    type Item = Result<Value, String>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(end) = self.end_byte
            && self.count.load(std::sync::atomic::Ordering::Relaxed) >= end
        {
            return None;
        }
        self.iter.next()
    }
}

impl JSONReaderState {
    fn next_value(&mut self) -> Option<Result<(Value, usize), String>> {
        if let Some(ref mut ai) = self.array_iter {
            if let Some(val) = ai.next() {
                let current_pos = self.position;
                self.position += 1;
                return Some(Ok((val, current_pos)));
            } else {
                self.array_iter = None;
            }
        }

        match self.reader.next() {
            Some(Ok(value)) => {
                if let Value::Array(arr) = value {
                    let mut ai = arr.into_iter();
                    if let Some(val) = ai.next() {
                        let current_pos = self.position;
                        self.position += 1;
                        self.array_iter = Some(ai);
                        return Some(Ok((val, current_pos)));
                    } else {
                        return None;
                    }
                }
                let current_pos = self.position;
                self.position += 1;
                Some(Ok((value, current_pos)))
            }
            Some(Err(e)) => {
                let pos = self.position + 1;
                Some(Err(format!("JSON parse error at position {}: {}", pos, e)))
            }
            None => None,
        }
    }
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
    #[pyo3(signature = (path, start_byte=0, end_byte=None))]
    fn new(py: Python<'_>, path: String, start_byte: u64, end_byte: Option<u64>) -> PyResult<Self> {
        let boxed_reader = if let Ok(mut r) = crate::io::get_reader(&path).map_err(wrap_py_err) {
            if start_byte > 0 {
                if path.ends_with(".gz") || path.ends_with(".zst") {
                    return Err(PipeError::Other(
                        "Cannot use start_byte with compressed files".into(),
                    )
                    .into());
                }
                r.seek(SeekFrom::Start(start_byte)).map_err(wrap_py_err)?;
                // Skip partial line
                let mut dummy = String::new();
                r.read_line(&mut dummy).map_err(wrap_py_err)?;
            }
            r
        } else {
            return Err(PipeError::Other(format!("Failed to open file: {}", path)).into());
        };

        let reader = SmartReader::new(&path, boxed_reader, move |r| {
            let counting_reader = CountingReader::new(r, start_byte);
            let count_handle = counting_reader.get_count_handle();
            let iter = serde_json::Deserializer::from_reader(counting_reader)
                .into_iter::<Value>()
                .map(|res| res.map_err(|e| format!("{}", e)));

            BoundedJsonIter {
                iter,
                count: count_handle,
                end_byte,
            }
        });

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

        let reader = SmartReader::new("", boxed_reader, |r| {
            serde_json::Deserializer::from_reader(r)
                .into_iter::<Value>()
                .map(|res| res.map_err(|e| format!("{}", e)))
        });

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
        let py = slf.py();
        let mut state = slf.state.lock().map_err(|_| PipeError::MutexLock)?;
        slf.next_internal(py, &mut state)
    }

    pub fn read_batch<'py>(
        &self,
        py: Python<'py>,
        batch_size: usize,
    ) -> PyResult<Option<Bound<'py, PyList>>> {
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
    fn next_internal<'py>(
        &self,
        py: Python<'py>,
        state: &mut JSONReaderState,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        match state.next_value() {
            Some(Ok((value, pos))) => Ok(Some(self.wrap_in_envelope(py, value, pos)?)),
            Some(Err(e)) => Err(PipeError::Other(e).into()),
            None => Ok(None),
        }
    }

    fn wrap_in_envelope<'py>(
        &self,
        py: Python<'py>,
        value: Value,
        position: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let raw_data = serde_to_py(py, value)?;
        crate::utils::wrap_in_envelope(
            py,
            &self.keys,
            raw_data,
            self.status_pending.bind(py).clone(),
            position,
            true, // JSON reader usually generates IDs
        )
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
    fn new(_py: Python<'_>, path: String, format: String, indent: Option<usize>) -> PyResult<Self> {
        let mut boxed_writer = crate::io::get_writer(&path).map_err(wrap_py_err)?;

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
        let mut is_first = self
            .is_first_item
            .lock()
            .map_err(|_| PipeError::MutexLock)?;

        self.write_internal(py, data, &mut writer, &mut is_first)
    }

    pub fn write_batch(&self, py: Python<'_>, entries: Bound<'_, PyAny>) -> PyResult<()> {
        let mut writer = self.writer.lock().map_err(|_| PipeError::MutexLock)?;
        let mut is_first = self
            .is_first_item
            .lock()
            .map_err(|_| PipeError::MutexLock)?;

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
        writer.close().map_err(wrap_py_err)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Cursor;

    fn create_state(data: Vec<u8>) -> JSONReaderState {
        let boxed_reader = BoxedReader::Cursor(Cursor::new(data));
        let reader = SmartReader::new("", boxed_reader, |r| {
            serde_json::Deserializer::from_reader(r)
                .into_iter::<Value>()
                .map(|res| res.map_err(|e| format!("{}", e)))
        });
        JSONReaderState {
            reader,
            array_iter: None,
            position: 0,
        }
    }

    #[test]
    fn test_json_reader_single_objects() {
        // NDJSON / Concatenated JSON
        let data = r#"{"a": 1}{"b": 2}"#.as_bytes().to_vec();
        let mut state = create_state(data);

        let (val1, pos1) = state.next_value().unwrap().unwrap();
        assert_eq!(val1, json!({"a": 1}));
        assert_eq!(pos1, 0);

        let (val2, pos2) = state.next_value().unwrap().unwrap();
        assert_eq!(val2, json!({"b": 2}));
        assert_eq!(pos2, 1);

        assert!(state.next_value().is_none());
    }

    #[test]
    fn test_json_reader_array() {
        // Single array
        let data = r#"[{"a": 1}, {"b": 2}]"#.as_bytes().to_vec();
        let mut state = create_state(data);

        let (val1, pos1) = state.next_value().unwrap().unwrap();
        assert_eq!(val1, json!({"a": 1}));
        assert_eq!(pos1, 0);

        let (val2, pos2) = state.next_value().unwrap().unwrap();
        assert_eq!(val2, json!({"b": 2}));
        assert_eq!(pos2, 1);

        assert!(state.next_value().is_none());
    }

    #[test]
    fn test_json_reader_mixed() {
        // Array then object
        let data = r#"[{"a": 1}] {"b": 2}"#.as_bytes().to_vec();
        let mut state = create_state(data);

        let (val1, pos1) = state.next_value().unwrap().unwrap();
        assert_eq!(val1, json!({"a": 1}));
        assert_eq!(pos1, 0);

        // After array finishes, next_value() calls reader.next()
        let (val2, pos2) = state.next_value().unwrap().unwrap();
        assert_eq!(val2, json!({"b": 2}));
        assert_eq!(pos2, 1);

        assert!(state.next_value().is_none());
    }
}
