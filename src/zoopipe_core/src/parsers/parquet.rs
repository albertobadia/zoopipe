use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::fs::File;
use std::sync::Mutex;
use arrow::record_batch::RecordBatch;

use arrow::datatypes::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use crate::utils::wrap_py_err;
use crate::error::PipeError;
use pyo3::types::{PyDict, PyList, PyString, PyAnyMethods};
use crate::parsers::arrow_utils::{arrow_to_py, build_record_batch, infer_type};
use crate::io::SmartReader;

struct ParquetReaderState {
    reader: SmartReader<RecordBatch>,
    current_batch: Option<(RecordBatch, usize)>,
    position: usize,
}

/// Fast Parquet reader that leverages the Arrow ecosystem for columnar I/O.
/// 
/// It supports streaming from both local paths and S3, performing 
/// efficient batch reads using native Rust kernels.
#[pyclass]
pub struct ParquetReader {
    state: Mutex<ParquetReaderState>,
    headers: Vec<Py<PyString>>,
    status_pending: Py<PyAny>,
    generate_ids: bool,
    keys: InternedKeys,
}

use crate::utils::interning::InternedKeys;

#[pymethods]
impl ParquetReader {
    #[new]
    #[pyo3(signature = (path, generate_ids=true, batch_size=1024))]
    fn new(py: Python<'_>, path: String, generate_ids: bool, batch_size: usize) -> PyResult<Self> {
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

        let builder = ParquetRecordBatchReaderBuilder::try_new(boxed_reader).map_err(wrap_py_err)?;
        let schema = builder.schema().clone();
        
        let headers: Vec<Py<PyString>> = schema.fields()
            .iter()
            .map(|f: &FieldRef| PyString::new(py, f.name()).unbind())
            .collect();
            
        let reader = SmartReader::new(
            &path,
            builder,
            move |b| {
                let reader = b.with_batch_size(batch_size).build().expect("Failed to build parquet reader");
                reader.map(|res| res.map_err(|e| format!("{}", e)))
            }
        );

        let models = py.import("zoopipe.report")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(ParquetReader {
            state: Mutex::new(ParquetReaderState {
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

impl ParquetReader {
    fn next_internal<'py>(&self, py: Python<'py>, state: &mut ParquetReaderState) -> PyResult<Option<Bound<'py, PyAny>>> {
        if state.current_batch.is_none() {
            if let Some(batch_res) = state.reader.next() {
                let batch = batch_res.map_err(wrap_py_err)?;
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

/// optimized Parquet writer for columnar data persistence.
/// 
/// It automatically infers the Arrow schema from processed batches and 
/// uses efficient compression and row-grouping strategies in Rust.
#[pyclass]
pub struct ParquetWriter {
    path: String,
    writer: Mutex<Option<ArrowWriter<crate::io::BoxedWriter>>>,
    schema: Mutex<Option<SchemaRef>>,
}

#[pymethods]
impl ParquetWriter {
    #[new]
    pub fn new(path: String) -> Self {
        ParquetWriter {
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
                    let dt = infer_type(&val);
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
            
            let props = WriterProperties::builder()
                .set_max_row_group_size(8_192)
                .build();

            *writer_guard = Some(ArrowWriter::try_new(boxed_writer, schema.clone(), Some(props)).map_err(wrap_py_err)?);
            *schema_guard = Some(schema);
        }

        let writer = writer_guard.as_mut().expect("ParquetWriter not initialized");
        let schema = schema_guard.as_ref().expect("ParquetWriter schema not initialized");
        
        let batch = build_record_batch(py, schema, list)?;
        writer.write(&batch).map_err(wrap_py_err)?;
        Ok(())
    }

    pub fn close(&self) -> PyResult<()> {
        let mut writer_guard = self.writer.lock().map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
        if let Some(w) = writer_guard.take() {
            w.close().map_err(wrap_py_err)?;
        }
        Ok(())
    }
}
