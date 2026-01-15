use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::fs::File;
use std::sync::Mutex;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::*;
use crate::utils::wrap_py_err;
use crate::error::PipeError;
use pyo3::types::{PyString, PyDict, PyList, PyAnyMethods};
use crate::parsers::arrow_utils::{arrow_to_py, build_record_batch};
use crate::io::BoxedReader;

#[pyclass]
pub struct ArrowReader {
    reader: Mutex<FileReader<BoxedReader>>,
    current_batch: Mutex<Option<(RecordBatch, usize)>>,
    headers: Vec<Py<PyString>>,
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
impl ArrowReader {
    #[new]
    #[pyo3(signature = (path, generate_ids=true))]
    fn new(py: Python<'_>, path: String, generate_ids: bool) -> PyResult<Self> {
        use crate::io::storage::StorageController;
        use object_store::path::Path as ObjectPath;
        use crate::io::{BoxedReader, RemoteReader};
        use std::io::BufReader;

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
        let mut reader = slf.reader.lock().map_err(|_| PipeError::MutexLock)?;
        let mut current_opt = slf.current_batch.lock().map_err(|_| PipeError::MutexLock)?;
        let mut pos = slf.position.lock().map_err(|_| PipeError::MutexLock)?;

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
            for (i, header) in slf.headers.iter().enumerate() {
                let col = batch.column(i);
                let val = arrow_to_py(py, col, *row_idx)?;
                raw_data.set_item(header.bind(py), val)?;
            }

            *row_idx += 1;
            if *row_idx >= batch.num_rows() {
                *current_opt = None;
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
        } else {
            Ok(None)
        }
    }

    pub fn read_batch<'py>(&self, py: Python<'py>, _batch_size: usize) -> PyResult<Option<Bound<'py, PyList>>> {
        let mut reader = self.reader.lock().map_err(|_| PipeError::MutexLock)?;
        let mut current_opt = self.current_batch.lock().map_err(|_| PipeError::MutexLock)?;
        let mut pos = self.position.lock().map_err(|_| PipeError::MutexLock)?;

        let batch = if let Some((b, idx)) = current_opt.take() {
            if idx == 0 {
                b
            } else {
                b.slice(idx, b.num_rows() - idx)
            }
        } else {
            match reader.next() {
                Some(batch_res) => batch_res.map_err(wrap_py_err)?,
                None => return Ok(None),
            }
        };

        let num_rows = batch.num_rows();
        let py_list = PyList::empty(py);

        for row_idx in 0..num_rows {
            let current_pos = *pos;
            *pos += 1;

            let raw_data = PyDict::new(py);
            for (i, header) in self.headers.iter().enumerate() {
                let col = batch.column(i);
                let val = arrow_to_py(py, col, row_idx)?;
                raw_data.set_item(header.bind(py), val)?;
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

            py_list.append(envelope)?;
        }

        Ok(Some(py_list))
    }
}

#[pyclass]
pub struct ArrowWriter {
    path: String,
    writer: Mutex<Option<FileWriter<crate::io::BoxedWriter>>>,
    schema: Mutex<Option<SchemaRef>>,
}

#[pymethods]
impl ArrowWriter {
    #[new]
    pub fn new(path: String) -> Self {
        ArrowWriter {
            path,
            writer: Mutex::new(None),
            schema: Mutex::new(None),
        }
    }

    pub fn write_batch(&self, py: Python<'_>, entries: Bound<'_, PyAny>) -> PyResult<()> {
        let list = entries.cast::<PyList>()?;
        if list.is_empty() { return Ok(()); }

        let mut writer_guard = self.writer.lock().map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
        let mut schema_guard = self.schema.lock().map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
        
        if writer_guard.is_none() {
            use crate::io::storage::StorageController;
            use object_store::path::Path as ObjectPath;
            use crate::io::{BoxedWriter, RemoteWriter};

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
            
            let controller = StorageController::new(&self.path).map_err(wrap_py_err)?;
            let boxed_writer = if self.path.starts_with("s3://") {
                BoxedWriter::Remote(RemoteWriter::new(controller.store(), ObjectPath::from(controller.path())))
            } else {
                let file = File::create(&self.path).map_err(wrap_py_err)?;
                BoxedWriter::File(std::io::BufWriter::new(file))
            };

            *writer_guard = Some(FileWriter::try_new(boxed_writer, &schema).map_err(wrap_py_err)?);
            *schema_guard = Some(schema);
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
        Ok(())
    }
}
