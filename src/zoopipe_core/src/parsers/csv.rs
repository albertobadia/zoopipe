use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::fs::File;
use std::io::{BufReader, Cursor};
use std::sync::Mutex;
use csv::StringRecord;
use crate::io::BoxedReader;
use crate::utils::wrap_py_err;
use pyo3::types::PyAnyMethods;

#[pyclass]
pub struct CSVReader {
    pub(crate) reader: Mutex<csv::Reader<BoxedReader>>,
    pub(crate) headers: Vec<String>,
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
            generate_ids,
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
                
                let raw_data = pyo3::types::PyDict::new(py);
                for (header, value) in slf.headers.iter().zip(record.iter()) {
                    raw_data.set_item(header, value)?;
                }
                
                let envelope = pyo3::types::PyDict::new(py);
                
                let id = if slf.generate_ids {
                    uuid::Uuid::new_v4().to_string()
                } else {
                    String::new()
                };

                envelope.set_item(pyo3::intern!(py, "id"), id)?;
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
    writer: Mutex<csv::Writer<std::io::BufWriter<File>>>,
    fieldnames: Mutex<Option<Vec<String>>>,
    header_written: Mutex<bool>,
    #[allow(dead_code)]
    delimiter: u8,
    #[allow(dead_code)]
    quote: u8,
}

#[pymethods]
impl CSVWriter {
    #[new]
    #[pyo3(signature = (path, delimiter=b',', quote=b'"', fieldnames=None))]
    fn new(
        _py: Python<'_>,
        path: String,
        delimiter: u8,
        quote: u8,
        fieldnames: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let file = File::create(path).map_err(wrap_py_err)?;
        let writer = csv::WriterBuilder::new()
            .delimiter(delimiter)
            .quote(quote)
            .from_writer(std::io::BufWriter::new(file));
        
        Ok(CSVWriter {
            writer: Mutex::new(writer),
            fieldnames: Mutex::new(fieldnames),
            header_written: Mutex::new(false),
            delimiter,
            quote,
        })
    }

    fn write(&self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        let mut writer = self.writer.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut header_written = self.header_written.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut fieldnames = self.fieldnames.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;

        self.write_internal(py, data, &mut writer, &mut header_written, &mut fieldnames)
    }

    fn write_batch(&self, py: Python<'_>, entries: Bound<'_, PyAny>) -> PyResult<()> {
        let mut writer = self.writer.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut header_written = self.header_written.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut fieldnames = self.fieldnames.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        
        let iterator = entries.try_iter()?;
        for entry in iterator {
            self.write_internal(py, entry?, &mut writer, &mut header_written, &mut fieldnames)?;
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
        _py: Python<'_>,
        data: Bound<'_, PyAny>,
        writer: &mut csv::Writer<std::io::BufWriter<File>>,
        header_written: &mut bool,
        fieldnames: &mut Option<Vec<String>>,
    ) -> PyResult<()> {
        let record = data.cast::<pyo3::types::PyDict>()?;

        if !*header_written {
            let names = if let Some(ref names) = *fieldnames {
                names.clone()
            } else {
                let mut record_keys: Vec<String> = record.keys().iter().map(|k| k.to_string()).collect();
                record_keys.sort();
                *fieldnames = Some(record_keys.clone());
                record_keys
            };
            writer.write_record(&names).map_err(wrap_py_err)?;
            *header_written = true;
        }

        if let Some(ref names) = *fieldnames {
            let mut row = Vec::with_capacity(names.len());
            for name in names {
                let val = record.get_item(name)?.map(|v| v.to_string()).unwrap_or_default();
                row.push(val);
            }
            writer.write_record(&row).map_err(wrap_py_err)?;
        }

        Ok(())
    }
}

