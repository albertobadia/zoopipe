use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::fs::File;
use std::io::BufReader;
use std::sync::{Arc, Mutex};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::*;
use object_store::path::Path as ObjectPath;
use crate::io::{BoxedReader, BoxedWriter, RemoteReader, RemoteWriter};
use crate::io::storage::StorageController;
use crate::utils::wrap_py_err;
use crate::error::PipeError;
use pyo3::types::{PyString, PyDict, PyList, PyAnyMethods};
use crate::parsers::arrow_utils::{arrow_to_py, build_record_batch};

/// Fast Arrow IPC (Feather) reader for ultra-efficient data loading.
/// 
/// It leverages zero-copy principles where possible to stream records directly 
/// from the Arrow IPC format into the pipeline.
#[pyclass]
pub struct ArrowReader {
    reader: Mutex<FileReader<BoxedReader>>,
    current_batch: Mutex<Option<(RecordBatch, usize)>>,
    headers: Vec<Py<PyString>>,
    position: Mutex<usize>,
    status_pending: Py<PyAny>,
    generate_ids: bool,
    keys: InternedKeys,
}

use crate::utils::interning::InternedKeys;

#[pymethods]
impl ArrowReader {
    #[new]
    #[pyo3(signature = (path, generate_ids=true))]
    fn new(py: Python<'_>, path: String, generate_ids: bool) -> PyResult<Self> {

        let controller = StorageController::new(&path).map_err(wrap_py_err)?;
        let boxed_reader = if path.starts_with("s3://") {
            BoxedReader::Remote(RemoteReader::new(controller.store(), ObjectPath::from(controller.path())))
        } else {
            let file = File::open(&path).map_err(wrap_py_err)?;
            BoxedReader::File(BufReader::new(file))
        };

        let reader = FileReader::try_new(boxed_reader, None).map_err(wrap_py_err)?;
        let schema = reader.schema();
        let headers: Vec<Py<PyString>> = schema.fields()
            .iter()
            .map(|f: &FieldRef| PyString::new(py, f.name()).unbind())
            .collect();

        let models = py.import("zoopipe.report")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(ArrowReader {
            reader: Mutex::new(reader),
            current_batch: Mutex::new(None),
            headers,
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

    pub fn read_batch<'py>(&self, py: Python<'py>, batch_size: usize) -> PyResult<Option<Bound<'py, PyList>>> {
        let batch = PyList::empty(py);
        let mut count = 0;
        
        while count < batch_size {
            if let Some(item) = self.next_internal(py)? {
                batch.append(item)?;
                count += 1;
            } else {
                break;
            }
        }

        if count == 0 {
            return Ok(None);
        }

        Ok(Some(batch))
    }
}

impl ArrowReader {
    fn next_internal<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let mut reader = self.reader.lock().map_err(|_| PipeError::MutexLock)?;
        let mut current_opt = self.current_batch.lock().map_err(|_| PipeError::MutexLock)?;
        let mut pos = self.position.lock().map_err(|_| PipeError::MutexLock)?;

        if current_opt.is_none() {
            if let Some(batch_res) = reader.next() {
                let batch: RecordBatch = batch_res.map_err(wrap_py_err)?;
                *current_opt = Some((batch, 0));
            } else {
                return Ok(None);
            }
        }

        if let Some((batch, row_idx)) = current_opt.as_mut() {
            let current_pos = *pos;
            *pos += 1;

            let raw_data = PyDict::new(py);
            for (i, header) in self.headers.iter().enumerate() {
                let col = batch.column(i);
                let val = arrow_to_py(py, col, *row_idx)?;
                raw_data.set_item(header.bind(py), val)?;
            }

            *row_idx += 1;
            if *row_idx >= batch.num_rows() {
                *current_opt = None;
            }

            let envelope = PyDict::new(py);
            let id = if self.generate_ids {
                crate::utils::generate_entry_id(py)?
            } else {
                py.None().into_bound(py)
            };

            envelope.set_item(self.keys.get_id(py), id)?;
            envelope.set_item(self.keys.get_status(py), self.status_pending.bind(py))?;
            envelope.set_item(self.keys.get_raw_data(py), raw_data)?;
            envelope.set_item(self.keys.get_metadata(py), PyDict::new(py))?;
            envelope.set_item(self.keys.get_position(py), current_pos)?;
            envelope.set_item(self.keys.get_errors(py), PyList::empty(py))?;

            Ok(Some(envelope.into_any()))
        } else {
            Ok(None)
        }
    }
}


/// Optimized Arrow IPC writer for high-performance data serialization.
/// 
/// It provides the fastest egress path by writing data in the native 
/// Arrow format, which matches the internal pipeline representation.
#[pyclass]
pub struct ArrowWriter {
    path: String,
    writer: Mutex<Option<FileWriter<crate::io::SharedWriter>>>,
    inner_writer: Mutex<Option<Arc<std::sync::Mutex<crate::io::BoxedWriter>>>>,
    schema: Mutex<Option<SchemaRef>>,
}

#[pymethods]
impl ArrowWriter {
    #[new]
    pub fn new(path: String) -> Self {
        ArrowWriter {
            path,
            writer: Mutex::new(None),
            inner_writer: Mutex::new(None),
            schema: Mutex::new(None),
        }
    }

    pub fn write_batch(&self, py: Python<'_>, entries: Bound<'_, PyAny>) -> PyResult<()> {
        let list = entries.cast::<PyList>()?;
        if list.is_empty() { return Ok(()); }

        let mut writer_guard = self.writer.lock().map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
        let mut schema_guard = self.schema.lock().map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
        
        if writer_guard.is_none() {

            let first = list.get_item(0)?;
            let dict = first.cast::<PyDict>()?;
            let mut fields = Vec::new();
            let mut keys: Vec<String> = dict.keys().iter().map(|k| k.to_string()).collect();
            keys.sort();
            
            for key in &keys {
                if let Some(val) = dict.get_item(key)? {
                    let dt = if val.is_instance_of::<pyo3::types::PyBool>() { DataType::Boolean }
                    else if val.is_instance_of::<pyo3::types::PyInt>() { DataType::Int64 }
                    else if val.is_instance_of::<pyo3::types::PyFloat>() { DataType::Float64 }
                    else { DataType::Utf8 };
                    fields.push(Field::new(key.clone(), dt, true));
                } else {
                    fields.push(Field::new(key.clone(), DataType::Utf8, true));
                }
            }
            let schema = SchemaRef::new(Schema::new(fields));
            
            let controller = crate::io::storage::StorageController::new(&self.path).map_err(wrap_py_err)?;
            let boxed_writer = if self.path.starts_with("s3://") {
                BoxedWriter::Remote(RemoteWriter::new(controller.store(), ObjectPath::from(controller.path())))
            } else {
                let file = crate::io::create_local_file(&self.path).map_err(wrap_py_err)?;
                BoxedWriter::File(std::io::BufWriter::new(file))
            };

            let shared_writer = Arc::new(std::sync::Mutex::new(boxed_writer));
            *writer_guard = Some(FileWriter::try_new(crate::io::SharedWriter(shared_writer.clone()), &schema).map_err(wrap_py_err)?);
            *schema_guard = Some(schema);
            let mut inner_guard = self.inner_writer.lock().map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
            *inner_guard = Some(shared_writer);
        }

        let writer = writer_guard.as_mut().expect("ArrowWriter not initialized");
        let schema = schema_guard.as_ref().expect("ArrowWriter schema not initialized");
        
        let batch = build_record_batch(py, schema, list)?;
        writer.write(&batch).map_err(wrap_py_err)?;
        Ok(())
    }

    pub fn close(&self) -> PyResult<()> {
        let mut writer_guard = self.writer.lock().map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
        if let Some(mut w) = writer_guard.take() {
            w.finish().map_err(wrap_py_err)?;
        }
        let mut inner_guard = self.inner_writer.lock().map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
        if let Some(shared) = inner_guard.take() {
            let mut writer: std::sync::MutexGuard<'_, crate::io::BoxedWriter> = shared.lock().unwrap();
            writer.close().map_err(wrap_py_err)?;
        }
        Ok(())
    }
}
