use pyo3::prelude::*;
use std::fs::File;
use std::io::{BufReader, Cursor};
use std::sync::Mutex;
use csv::StringRecord;
use crate::io::BoxedReader;
use crate::utils::wrap_py_err;
use crate::error::PipeError;
use pyo3::types::{PyAnyMethods, PyString, PyDict, PyList};

struct CSVReaderState {
    reader: csv::Reader<BoxedReader>,
    position: usize,
}

#[pyclass]
pub struct CSVReader {
    state: Mutex<CSVReaderState>,
    pub(crate) headers: Vec<Py<PyString>>,
    pub(crate) status_pending: Py<PyAny>,
    pub(crate) generate_ids: bool,
    id_key: Py<PyString>,
    status_key: Py<PyString>,
    raw_data_key: Py<PyString>,
    metadata_key: Py<PyString>,
    position_key: Py<PyString>,
    errors_key: Py<PyString>,
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
            state: Mutex::new(CSVReaderState { reader, position: 0 }),
            headers,
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
            state: Mutex::new(CSVReaderState { reader, position: 0 }),
            headers,
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
        let mut state = slf.state.lock().map_err(|_| PipeError::MutexLock)?;
        
        let mut record = StringRecord::new();
        match state.reader.read_record(&mut record) {
            Ok(true) => {
                let py = slf.py();
                let current_pos = state.position;
                state.position += 1;
                
                let raw_data = PyDict::new(py);
                for (header_py, value) in slf.headers.iter().zip(record.iter()) {
                    raw_data.set_item(header_py.bind(py), value)?;
                }
                
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
            Ok(false) => Ok(None),
            Err(e) => {
                let line = state.position + 1;
                Err(PipeError::Other(format!("CSV parse error at line {}: {}", line, e)).into())
            }
        }
    }

    pub fn read_batch<'py>(&self, py: Python<'py>, batch_size: usize) -> PyResult<Option<Bound<'py, PyList>>> {
        let mut state = self.state.lock().map_err(|_| PipeError::MutexLock)?;
        
        let batch = PyList::empty(py);
        let mut count = 0;
        let mut record = StringRecord::new();

        while count < batch_size {
            match state.reader.read_record(&mut record) {
                Ok(true) => {
                    let current_pos = state.position;
                    state.position += 1;
                    
                    let raw_data = PyDict::new(py);
                    for (header_py, value) in self.headers.iter().zip(record.iter()) {
                        raw_data.set_item(header_py.bind(py), value)?;
                    }
                    
                    let envelope = PyDict::new(py);
                    let id = if self.generate_ids {
                        crate::utils::generate_entry_id(py)?
                    } else {
                        py.None().into_bound(py)
                    };

                    envelope.set_item(self.id_key.bind(py), id)?;
                    envelope.set_item(self.status_key.bind(py), self.status_pending.bind(py))?;
                    envelope.set_item(self.raw_data_key.bind(py), raw_data)?;
                    envelope.set_item(self.metadata_key.bind(py), PyDict::new(py))?;
                    envelope.set_item(self.position_key.bind(py), current_pos)?;
                    envelope.set_item(self.errors_key.bind(py), PyList::empty(py))?;

                    batch.append(envelope)?;
                    count += 1;
                }
                Ok(false) => break,
                Err(e) => {
                    let line = state.position + 1;
                    return Err(PipeError::Other(format!("CSV parse error at line {}: {}", line, e)).into());
                }
            }
        }

        if count == 0 {
            return Ok(None);
        }

        Ok(Some(batch))
    }
}

#[pyclass]
pub struct CSVWriter {
    state: Mutex<CSVWriterState>,
}

struct CSVWriterState {
    writer: csv::Writer<crate::io::BoxedWriter>,
    fieldnames: Option<Vec<Py<PyString>>>,
    header_written: bool,
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
            state: Mutex::new(CSVWriterState {
                writer,
                fieldnames: py_fieldnames,
                header_written: false,
            }),
        })
    }

    pub fn write(&self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        let mut state = self.state.lock().map_err(|_| PipeError::MutexLock)?;
        write_record_to_csv(py, data, &mut state)
    }

    pub fn write_batch(&self, py: Python<'_>, entries: Bound<'_, PyAny>) -> PyResult<()> {
        let mut state = self.state.lock().map_err(|_| PipeError::MutexLock)?;
        
        let iterator = entries.try_iter()?;
        for entry in iterator {
            write_record_to_csv(py, entry?, &mut state)?;
        }
        Ok(())
    }

    pub fn flush(&self) -> PyResult<()> {
        let mut state = self.state.lock().map_err(|_| PipeError::MutexLock)?;
        state.writer.flush().map_err(wrap_py_err)?;
        Ok(())
    }

    pub fn close(&self) -> PyResult<()> {
        self.flush()
    }
}

use std::borrow::Cow;

fn write_record_to_csv(
    py: Python<'_>,
    data: Bound<'_, PyAny>,
    state: &mut CSVWriterState,
) -> PyResult<()> {
    let record = data.cast::<PyDict>()?;

    if !state.header_written {
        if state.fieldnames.is_none() {
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
            state.fieldnames = Some(interned);
        }
        
        let names = state.fieldnames.as_ref()
            .expect("Fieldnames should be initialized before header write");
        let bounds: Vec<Bound<'_, PyString>> = names.iter().map(|n| n.bind(py).clone()).collect();
        let mut name_strs = Vec::with_capacity(names.len());
        for b in &bounds {
            name_strs.push(b.to_str().unwrap_or(""));
        }
        state.writer.write_record(&name_strs).map_err(wrap_py_err)?;
        state.header_written = true;
    }

    if let Some(names) = state.fieldnames.as_ref() {
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
        state.writer.write_record(row_out.iter().map(|c| c.as_bytes())).map_err(wrap_py_err)?;
    }

    Ok(())
}
