use crate::error::PipeError;
use crate::io::storage::StorageController;
use crate::io::{BoxedReader, BoxedWriter, RemoteReader, RemoteWriter};
use crate::parsers::arrow_utils::{arrow_to_py, build_record_batch};
use crate::utils::wrap_py_err;
use arrow::datatypes::*;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use object_store::path::Path as ObjectPath;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyList, PyString};
use std::fs::File;
use std::io::BufReader;
use std::sync::{Arc, Mutex};

use crate::io::SmartReader;

struct ArrowReaderState {
    reader: SmartReader<RecordBatch>,
    current_batch: Option<(RecordBatch, usize)>,
    position: usize,
}

/// Fast Arrow IPC (Feather) reader for ultra-efficient data loading.
///
/// It leverages zero-copy principles where possible to stream records directly
/// from the Arrow IPC format into the pipeline.
#[pyclass]
pub struct ArrowReader {
    state: Mutex<ArrowReaderState>,
    headers: Vec<Py<PyString>>,
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
            BoxedReader::Remote(RemoteReader::new(
                controller.store(),
                ObjectPath::from(controller.path()),
            ))
        } else {
            let file = File::open(&path).map_err(wrap_py_err)?;
            BoxedReader::File(BufReader::new(file))
        };

        let reader = SmartReader::new(&path, boxed_reader, |br| {
            match FileReader::try_new(br, None) {
                Ok(r) => Box::new(r.map(|res| res.map_err(|e| e.to_string())))
                    as Box<dyn Iterator<Item = Result<RecordBatch, String>> + Send>,
                Err(e) => Box::new(std::iter::once(Err(e.to_string())))
                    as Box<dyn Iterator<Item = Result<RecordBatch, String>> + Send>,
            }
        });

        // Get schema from a temporary reader to extract headers
        let temp_reader = if path.starts_with("s3://") {
            BoxedReader::Remote(RemoteReader::new(
                controller.store(),
                ObjectPath::from(controller.path()),
            ))
        } else {
            let file = File::open(&path).map_err(wrap_py_err)?;
            BoxedReader::File(BufReader::new(file))
        };
        let fr = FileReader::try_new(temp_reader, None).map_err(wrap_py_err)?;
        let schema = fr.schema();

        let headers: Vec<Py<PyString>> = schema
            .fields()
            .iter()
            .map(|f: &FieldRef| PyString::new(py, f.name()).unbind())
            .collect();

        let models = py.import("zoopipe.structs")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(ArrowReader {
            state: Mutex::new(ArrowReaderState {
                reader,
                current_batch: None,
                position: 0,
            }),
            headers,
            status_pending,
            generate_ids,
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
        let mut count = 0;

        while count < batch_size {
            if let Some(item) = self.next_internal(py, &mut state)? {
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
    fn next_internal<'py>(
        &self,
        py: Python<'py>,
        state: &mut ArrowReaderState,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        if state.current_batch.is_none() {
            if let Some(batch_res) = state.reader.next() {
                let batch: RecordBatch = batch_res.map_err(PyRuntimeError::new_err)?;
                state.current_batch = Some((batch, 0));
            } else {
                return Ok(None);
            }
        }

        if let Some((batch, row_idx)) = state.current_batch.as_mut() {
            let current_pos = state.position;
            state.position += 1;

            let raw_data = PyDict::new(py);
            for (i, header) in self.headers.iter().enumerate() {
                let col = batch.column(i);
                let val = arrow_to_py(py, col, *row_idx)?;
                raw_data.set_item(header.bind(py), val)?;
            }

            *row_idx += 1;
            if *row_idx >= batch.num_rows() {
                state.current_batch = None;
            }

            let env = crate::utils::wrap_in_envelope(
                py,
                &self.keys,
                raw_data.into_any(),
                self.status_pending.bind(py).clone(),
                current_pos,
                self.generate_ids,
            )?;

            Ok(Some(env))
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
        if list.is_empty() {
            return Ok(());
        }

        let mut writer_guard = self
            .writer
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
        let mut schema_guard = self
            .schema
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Lock failed"))?;

        if writer_guard.is_none() {
            let first = list.get_item(0)?;
            let dict = first.cast::<PyDict>()?;
            let mut fields = Vec::new();
            let mut keys: Vec<String> = dict.keys().iter().map(|k| k.to_string()).collect();
            keys.sort();

            for key in &keys {
                if let Some(val) = dict.get_item(key)? {
                    let dt = if val.is_instance_of::<pyo3::types::PyBool>() {
                        DataType::Boolean
                    } else if val.is_instance_of::<pyo3::types::PyInt>() {
                        DataType::Int64
                    } else if val.is_instance_of::<pyo3::types::PyFloat>() {
                        DataType::Float64
                    } else {
                        DataType::Utf8
                    };
                    fields.push(Field::new(key.clone(), dt, true));
                } else {
                    fields.push(Field::new(key.clone(), DataType::Utf8, true));
                }
            }
            let schema = SchemaRef::new(Schema::new(fields));

            let controller =
                crate::io::storage::StorageController::new(&self.path).map_err(wrap_py_err)?;
            let boxed_writer = if self.path.starts_with("s3://") {
                BoxedWriter::Remote(RemoteWriter::new(
                    controller.store(),
                    ObjectPath::from(controller.path()),
                ))
            } else {
                let file = crate::io::create_local_file(&self.path).map_err(wrap_py_err)?;
                BoxedWriter::File(std::io::BufWriter::new(file))
            };

            let shared_writer = Arc::new(std::sync::Mutex::new(boxed_writer));
            *writer_guard = Some(
                FileWriter::try_new(crate::io::SharedWriter(shared_writer.clone()), &schema)
                    .map_err(wrap_py_err)?,
            );
            *schema_guard = Some(schema);
            let mut inner_guard = self
                .inner_writer
                .lock()
                .map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
            *inner_guard = Some(shared_writer);
        }

        let writer = writer_guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("ArrowWriter failed to initialize"))?;
        let schema = schema_guard
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("ArrowWriter schema failed to initialize"))?;

        let batch = build_record_batch(py, schema, list)?;
        writer.write(&batch).map_err(wrap_py_err)?;
        Ok(())
    }

    pub fn close(&self) -> PyResult<()> {
        let mut writer_guard = self
            .writer
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
        if let Some(mut w) = writer_guard.take() {
            w.finish().map_err(wrap_py_err)?;
        }
        let mut inner_guard = self
            .inner_writer
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
        if let Some(shared) = inner_guard.take() {
            let mut writer = shared
                .lock()
                .map_err(|_| PyRuntimeError::new_err("Lock poisoned during close"))?;
            writer.close().map_err(wrap_py_err)?;
        }
        Ok(())
    }
}
