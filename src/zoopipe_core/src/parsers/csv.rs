use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::fs::File;
use std::io::{BufReader, Cursor};
use std::sync::Mutex;
use csv::StringRecord;
use crate::io::BoxedReader;
use crate::utils::{python_to_serde, wrap_py_err};

#[pyclass]
pub struct CSVReader {
    pub(crate) reader: Mutex<csv::Reader<BoxedReader>>,
    pub(crate) headers: Vec<String>,
    pub(crate) position: Mutex<usize>,
    pub(crate) status_pending: Py<PyAny>,
}

#[pymethods]
impl CSVReader {
    #[new]
    #[pyo3(signature = (path, delimiter=b',', quote=b'"', skip_rows=0, fieldnames=None))]
    fn new(
        py: Python<'_>,
        path: String,
        delimiter: u8,
        quote: u8,
        skip_rows: usize,
        fieldnames: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let file = File::open(path).map_err(wrap_py_err)?;
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .quote(quote)
            .from_reader(BoxedReader::File(BufReader::new(file)));

        for _ in 0..skip_rows {
            let mut record = csv::StringRecord::new();
            if !reader.read_record(&mut record).map_err(wrap_py_err)? {
                break;
            }
        }

        let headers = if let Some(fields) = fieldnames {
            fields
        } else {
            reader
                .headers()
                .map_err(wrap_py_err)?
                .iter()
                .map(|s| s.to_string())
                .collect()
        };

        let models = py.import("zoopipe.models.core")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(CSVReader {
            reader: Mutex::new(reader),
            headers,
            position: Mutex::new(0),
            status_pending,
        })
    }

    #[staticmethod]
    #[pyo3(signature = (data, delimiter=b',', quote=b'"', skip_rows=0, fieldnames=None))]
    fn from_bytes(
        py: Python<'_>,
        data: Vec<u8>,
        delimiter: u8,
        quote: u8,
        skip_rows: usize,
        fieldnames: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .quote(quote)
            .from_reader(BoxedReader::Cursor(Cursor::new(data)));

        for _ in 0..skip_rows {
            let mut record = csv::StringRecord::new();
            if !reader.read_record(&mut record).map_err(wrap_py_err)? {
                break;
            }
        }

        let headers = if let Some(fields) = fieldnames {
            fields
        } else {
            reader
                .headers()
                .map_err(wrap_py_err)?
                .iter()
                .map(|s| s.to_string())
                .collect()
        };

        let models = py.import("zoopipe.models.core")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(CSVReader {
            reader: Mutex::new(reader),
            headers,
            position: Mutex::new(0),
            status_pending,
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(slf: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyAny>>> {
        let mut reader = slf.reader.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut pos = slf.position.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        
        let mut record = StringRecord::new();
        match reader.read_record(&mut record) {
            Ok(true) => {
                let py = slf.py();
                let current_pos = *pos;
                *pos += 1;
                
                // 1. Build raw_data
                let raw_data = pyo3::types::PyDict::new(py);
                for (header, value) in slf.headers.iter().zip(record.iter()) {
                    raw_data.set_item(header, value)?;
                }
                
                // 2. Build Envelope
                let envelope = pyo3::types::PyDict::new(py);
                envelope.set_item(pyo3::intern!(py, "id"), uuid::Uuid::new_v4().to_string())?;
                envelope.set_item(pyo3::intern!(py, "status"), slf.status_pending.bind(py))?;
                envelope.set_item(pyo3::intern!(py, "raw_data"), raw_data)?;
                envelope.set_item(pyo3::intern!(py, "metadata"), pyo3::types::PyDict::new(py))?;
                envelope.set_item(pyo3::intern!(py, "position"), current_pos)?;
                envelope.set_item(pyo3::intern!(py, "errors"), pyo3::types::PyList::empty(py))?;

                Ok(Some(envelope.into_any()))
            }
            Ok(false) => Ok(None),
            Err(e) => Err(wrap_py_err(e)),
        }
    }
}

#[pyclass]
pub struct CSVWriter {
    writer: Mutex<csv::Writer<File>>,
    fieldnames: Mutex<Option<Vec<String>>>,
    header_written: Mutex<bool>,
    #[allow(dead_code)]
    delimiter: u8,
    #[allow(dead_code)]
    quote: u8,
    include_metadata: bool,
}

#[pymethods]
impl CSVWriter {
    #[new]
    #[pyo3(signature = (path, delimiter=b',', quote=b'"', fieldnames=None, include_metadata=false))]
    fn new(
        _py: Python<'_>,
        path: String,
        delimiter: u8,
        quote: u8,
        fieldnames: Option<Vec<String>>,
        include_metadata: bool,
    ) -> PyResult<Self> {
        let file = File::create(path).map_err(wrap_py_err)?;
        let writer = csv::WriterBuilder::new()
            .delimiter(delimiter)
            .quote(quote)
            .from_writer(file);
        
        Ok(CSVWriter {
            writer: Mutex::new(writer),
            fieldnames: Mutex::new(fieldnames),
            header_written: Mutex::new(false),
            delimiter,
            quote,
            include_metadata,
        })
    }

    fn write(&self, py: Python<'_>, entry: Bound<'_, PyAny>) -> PyResult<()> {
        let mut writer = self.writer.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut header_written = self.header_written.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut fieldnames = self.fieldnames.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;

        self.write_internal(py, entry, &mut writer, &mut header_written, &mut fieldnames)
    }

    fn write_batch(&self, py: Python<'_>, entries: Bound<'_, pyo3::types::PyList>) -> PyResult<()> {
        let mut writer = self.writer.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut header_written = self.header_written.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut fieldnames = self.fieldnames.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        
        for entry in entries.iter() {
            self.write_internal(py, entry, &mut writer, &mut header_written, &mut fieldnames)?;
        }
        Ok(())
    }

    fn flush(&self) -> PyResult<()> {
        let mut writer = self.writer.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        writer.flush().map_err(wrap_py_err)?;
        Ok(())
    }

    fn close(&self) -> PyResult<()> {
        self.flush()
    }
}

impl CSVWriter {
    fn write_internal(
        &self,
        py: Python<'_>,
        entry: Bound<'_, PyAny>,
        writer: &mut csv::Writer<File>,
        header_written: &mut bool,
        fieldnames: &mut Option<Vec<String>>,
    ) -> PyResult<()> {
        let entry_dict = entry.cast::<pyo3::types::PyDict>()?;
        
        let record_py = if let Ok(Some(val)) = entry_dict.get_item(pyo3::intern!(py, "validated_data")) {
            val
        } else {
            entry_dict.get_item(pyo3::intern!(py, "raw_data"))?.ok_or_else(|| PyRuntimeError::new_err("Missing 'raw_data' in entry"))?
        };
        let record = record_py.cast::<pyo3::types::PyDict>()?;

        if !*header_written {
            let names = if let Some(ref names) = *fieldnames {
                names.clone()
            } else {
                let mut names = vec!["id".to_string(), "status".to_string(), "position".to_string()];
                if self.include_metadata {
                    names.push("metadata".to_string());
                }
                let mut record_keys: Vec<String> = record.keys().iter().map(|k| k.to_string()).collect();
                record_keys.sort();
                names.extend(record_keys);
                *fieldnames = Some(names.clone());
                names
            };
            writer.write_record(&names).map_err(wrap_py_err)?;
            *header_written = true;
        }

        if let Some(ref names) = *fieldnames {
            let mut row = Vec::with_capacity(names.len());
            
            let id_key = pyo3::intern!(py, "id");
            let status_key = pyo3::intern!(py, "status");
            let pos_key = pyo3::intern!(py, "position");
            let meta_key = pyo3::intern!(py, "metadata");
            let value_attr = pyo3::intern!(py, "value");

            for name in names {
                match name.as_str() {
                    "id" => {
                        let id_raw = entry_dict.get_item(id_key)?.ok_or_else(|| PyRuntimeError::new_err("Missing 'id' in entry"))?;
                        row.push(id_raw.to_string());
                    }
                    "status" => {
                        let status = entry_dict.get_item(status_key)?.ok_or_else(|| PyRuntimeError::new_err("Missing 'status' in entry"))?;
                        if let Ok(s) = status.cast::<pyo3::types::PyString>() {
                            row.push(s.to_string());
                        } else if let Ok(val) = status.getattr(value_attr) {
                             row.push(val.to_string());
                        } else {
                            row.push(status.to_string());
                        }
                    }
                    "position" => row.push(entry_dict.get_item(pos_key)?.ok_or_else(|| PyRuntimeError::new_err("Missing 'position' in entry"))?.to_string()),
                    "metadata" => {
                        if self.include_metadata {
                            let metadata = entry_dict.get_item(meta_key)?.ok_or_else(|| PyRuntimeError::new_err("Missing 'metadata' in entry"))?;
                            let meta_val = python_to_serde(py, metadata)?;
                            row.push(serde_json::to_string(&meta_val).map_err(wrap_py_err)?);
                        } else {
                            row.push(String::new());
                        }
                    }
                    _ => {
                        let val = record.get_item(name)?.map(|v| v.to_string()).unwrap_or_default();
                        row.push(val);
                    }
                }
            }
            writer.write_record(&row).map_err(wrap_py_err)?;
        }

        Ok(())
    }
}

