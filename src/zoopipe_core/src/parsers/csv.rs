use pyo3::prelude::*;
use std::fs::File;
use std::io::{BufReader, Cursor};
use std::sync::Mutex;
use csv::StringRecord;
use crate::io::BoxedReader;
use crate::utils::wrap_py_err;
use crate::error::PipeError;
use pyo3::types::{PyAnyMethods, PyString, PyDict, PyList};

#[pyclass]
pub struct CSVReader {
    pub(crate) reader: Mutex<csv::Reader<BoxedReader>>,
    pub(crate) headers: Vec<Py<PyString>>,
    pub(crate) position: Mutex<usize>,
    pub(crate) status_pending: Py<PyAny>,
    pub(crate) generate_ids: bool,
}

#[pymethods]
impl CSVReader {
    #[new]
    #[pyo3(signature = (path, delimiter=b',', quote=b'"', skip_rows=0, fieldnames=None, generate_ids=true))]
    fn new(
        py: Python<'_>,
        path: String,
        delimiter: u8,
        quote: u8,
        skip_rows: usize,
        fieldnames: Option<Vec<String>>,
        generate_ids: bool,
    ) -> PyResult<Self> {
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

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .quote(quote)
            .from_reader(boxed_reader);

        for _ in 0..skip_rows {
            let mut record = csv::StringRecord::new();
            if !reader.read_record(&mut record).map_err(wrap_py_err)? {
                break;
            }
        }

        let headers_str = if let Some(fields) = fieldnames {
            fields
        } else {
            reader
                .headers()
                .map_err(wrap_py_err)?
                .iter()
                .map(|s| s.to_string())
                .collect()
        };

        let headers: Vec<Py<PyString>> = headers_str
            .into_iter()
            .map(|s| PyString::new(py, &s).unbind())
            .collect();

        let models = py.import("zoopipe.report")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(CSVReader {
            reader: Mutex::new(reader),
            headers,
            position: Mutex::new(0),
            status_pending,
            generate_ids,
        })
    }

    #[staticmethod]
    #[pyo3(signature = (data, delimiter=b',', quote=b'"', skip_rows=0, fieldnames=None, generate_ids=true))]
    fn from_bytes(
        py: Python<'_>,
        data: Vec<u8>,
        delimiter: u8,
        quote: u8,
        skip_rows: usize,
        fieldnames: Option<Vec<String>>,
        generate_ids: bool,
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

        let headers_str = if let Some(fields) = fieldnames {
            fields
        } else {
            reader
                .headers()
                .map_err(wrap_py_err)?
                .iter()
                .map(|s| s.to_string())
                .collect()
        };

        let headers: Vec<Py<PyString>> = headers_str
            .into_iter()
            .map(|s| PyString::new(py, &s).unbind())
            .collect();

        let models = py.import("zoopipe.report")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(CSVReader {
            reader: Mutex::new(reader),
            headers,
            position: Mutex::new(0),
            status_pending,
            generate_ids,
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __next__(slf: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyAny>>> {
        let mut reader = slf.reader.lock().map_err(|_| PipeError::MutexLock)?;
        let mut pos = slf.position.lock().map_err(|_| PipeError::MutexLock)?;
        
        let mut record = StringRecord::new();
        match reader.read_record(&mut record) {
            Ok(true) => {
                let py = slf.py();
                let current_pos = *pos;
                *pos += 1;
                
                let raw_data = PyDict::new(py);
                for (header_py, value) in slf.headers.iter().zip(record.iter()) {
                    raw_data.set_item(header_py.bind(py), value)?;
                }
                
                let envelope = PyDict::new(py);
                
                let id = if slf.generate_ids {
                    uuid::Uuid::new_v4().to_string()
                } else {
                    String::new()
                };

                envelope.set_item(pyo3::intern!(py, "id"), id)?;
                envelope.set_item(pyo3::intern!(py, "status"), slf.status_pending.bind(py))?;
                envelope.set_item(pyo3::intern!(py, "raw_data"), raw_data)?;
                envelope.set_item(pyo3::intern!(py, "metadata"), PyDict::new(py))?;
                envelope.set_item(pyo3::intern!(py, "position"), current_pos)?;
                envelope.set_item(pyo3::intern!(py, "errors"), PyList::empty(py))?;

                Ok(Some(envelope.into_any()))
            }
            Ok(false) => Ok(None),
            Err(e) => Err(wrap_py_err(e)),
        }
    }
}

#[pyclass]
pub struct CSVWriter {
    writer: Mutex<csv::Writer<crate::io::BoxedWriter>>,
    fieldnames: Mutex<Option<Vec<Py<PyString>>>>,
    header_written: Mutex<bool>,
}

#[pymethods]
impl CSVWriter {
    #[new]
    #[pyo3(signature = (path, delimiter=b',', quote=b'"', fieldnames=None))]
    pub fn new(
        py: Python<'_>,
        path: String,
        delimiter: u8,
        quote: u8,
        fieldnames: Option<Vec<String>>,
    ) -> PyResult<Self> {
        use crate::io::storage::StorageController;
        use object_store::path::Path;
        use crate::io::{BoxedWriter, RemoteWriter};

        let controller = StorageController::new(&path).map_err(wrap_py_err)?;
        let boxed_writer = if path.starts_with("s3://") {
            BoxedWriter::Remote(RemoteWriter::new(controller.store(), Path::from(controller.path())))
        } else {
            let file = File::create(&path).map_err(wrap_py_err)?;
            BoxedWriter::File(std::io::BufWriter::new(file))
        };

        let writer = csv::WriterBuilder::new()
            .delimiter(delimiter)
            .quote(quote)
            .from_writer(boxed_writer);
        
        let py_fieldnames = fieldnames.map(|names| {
            names.into_iter().map(|s| PyString::new(py, &s).unbind()).collect()
        });

        Ok(CSVWriter {
            writer: Mutex::new(writer),
            fieldnames: Mutex::new(py_fieldnames),
            header_written: Mutex::new(false),
        })
    }

    pub fn write(&self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        let mut writer = self.writer.lock().map_err(|_| PipeError::MutexLock)?;
        let mut header_written = self.header_written.lock().map_err(|_| PipeError::MutexLock)?;
        let mut fieldnames = self.fieldnames.lock().map_err(|_| PipeError::MutexLock)?;

        self.write_internal(py, data, &mut writer, &mut header_written, &mut fieldnames)
    }

    pub fn write_batch(&self, py: Python<'_>, entries: Bound<'_, PyAny>) -> PyResult<()> {
        let mut writer = self.writer.lock().map_err(|_| PipeError::MutexLock)?;
        let mut header_written = self.header_written.lock().map_err(|_| PipeError::MutexLock)?;
        let mut fieldnames = self.fieldnames.lock().map_err(|_| PipeError::MutexLock)?;
        
        let iterator = entries.try_iter()?;
        for entry in iterator {
            self.write_internal(py, entry?, &mut writer, &mut header_written, &mut fieldnames)?;
        }
        Ok(())
    }

    pub fn flush(&self) -> PyResult<()> {
        let mut writer = self.writer.lock().map_err(|_| PipeError::MutexLock)?;
        writer.flush().map_err(wrap_py_err)?;
        Ok(())
    }

    pub fn close(&self) -> PyResult<()> {
        self.flush()
    }
}

use std::borrow::Cow;

impl CSVWriter {
    fn write_internal(
        &self,
        py: Python<'_>,
        data: Bound<'_, PyAny>,
        writer: &mut csv::Writer<crate::io::BoxedWriter>,
        header_written: &mut bool,
        fieldnames: &mut Option<Vec<Py<PyString>>>,
    ) -> PyResult<()> {
        let record = data.cast::<PyDict>()?;

        if !*header_written {
            if fieldnames.is_none() {
                let keys_list = record.keys();
                let mut record_keys = Vec::with_capacity(keys_list.len());
                for k in keys_list.iter() {
                    record_keys.push(k.cast::<PyString>()?.clone());
                }
                
                record_keys.sort_by(|a, b| {
                    let s1 = a.to_str().unwrap_or("");
                    let s2 = b.to_str().unwrap_or("");
                    s1.cmp(s2)
                });
                let interned: Vec<Py<PyString>> = record_keys.into_iter().map(|s| s.unbind()).collect();
                *fieldnames = Some(interned);
            }
            
            let names = fieldnames.as_ref()
                .expect("Fieldnames should be initialized before header write");
            let bounds: Vec<Bound<'_, PyString>> = names.iter().map(|n| n.bind(py).clone()).collect();
            let mut name_strs = Vec::with_capacity(names.len());
            for b in &bounds {
                name_strs.push(b.to_str().unwrap_or(""));
            }
            writer.write_record(&name_strs).map_err(wrap_py_err)?;
            *header_written = true;
        }

        if let Some(names) = fieldnames.as_ref() {
            let mut row_bounds: Vec<Option<Bound<'_, PyAny>>> = Vec::with_capacity(names.len());
            for name_py in names {
                row_bounds.push(record.get_item(name_py.bind(py))?);
            }

            let mut row_out: Vec<Cow<str>> = Vec::with_capacity(names.len());
            for opt_val in &row_bounds {
                if let Some(v) = opt_val {
                    if let Ok(s) = v.cast::<PyString>() {
                        row_out.push(Cow::Borrowed(s.to_str().unwrap_or("")));
                    } else {
                        row_out.push(Cow::Owned(v.to_string()));
                    }
                } else {
                    row_out.push(Cow::Borrowed(""));
                }
            }
            writer.write_record(row_out.iter().map(|c| c.as_bytes())).map_err(wrap_py_err)?;
        }

        Ok(())
    }
}

