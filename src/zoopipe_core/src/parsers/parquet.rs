use crate::error::PipeError;
use crate::io::storage::StorageController;
use crate::io::{BoxedReader, BoxedWriter, RemoteReader, RemoteWriter, SharedWriter, SmartReader};
use crate::parsers::arrow_utils::{arrow_to_py, build_record_batch, infer_type};
use crate::utils::wrap_py_err;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use object_store::path::Path as ObjectPath;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyList, PyString};
use std::fs::File;
use std::io::BufReader;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Mutex as StdMutex};

struct ParquetReaderState {
    reader: SmartReader<RecordBatch>,
    current_batch: Option<(RecordBatch, usize)>,
    position: usize,
    rows_to_skip: usize,
    rows_to_read: Option<usize>,
    rows_read: usize,
}

impl ParquetReaderState {
    // Returns (Batch, RowIndex, Position) or None
    fn next_record(&mut self) -> Option<Result<(RecordBatch, usize, usize), String>> {
        if let Some(limit) = self.rows_to_read
            && self.rows_read >= limit
        {
            return None;
        }

        loop {
            if self.current_batch.is_none() {
                match self.reader.next() {
                    Some(Ok(batch)) => {
                        self.current_batch = Some((batch, 0));
                    }
                    Some(Err(e)) => return Some(Err(e.to_string())),
                    None => return None,
                }
            }

            if let Some((batch, row_idx)) = self.current_batch.as_mut() {
                if self.rows_to_skip > 0 {
                    let skip = std::cmp::min(self.rows_to_skip, batch.num_rows() - *row_idx);
                    self.rows_to_skip -= skip;
                    *row_idx += skip;
                    if *row_idx >= batch.num_rows() {
                        self.current_batch = None;
                        continue;
                    }
                }

                let current_pos = self.position;
                self.position += 1;
                self.rows_read += 1;

                let batch_ref = batch.clone();
                let current_idx = *row_idx;

                *row_idx += 1;
                if *row_idx >= batch.num_rows() {
                    self.current_batch = None;
                }

                return Some(Ok((batch_ref, current_idx, current_pos)));
            }
            break;
        }

        None
    }
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
    #[pyo3(signature = (path, generate_ids=true, batch_size=1024, limit=None, offset=0, row_groups=None))]
    fn new(
        py: Python<'_>,
        path: String,
        generate_ids: bool,
        batch_size: usize,
        limit: Option<usize>,
        offset: usize,
        row_groups: Option<Vec<usize>>,
    ) -> PyResult<Self> {
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

        let mut builder =
            ParquetRecordBatchReaderBuilder::try_new(boxed_reader).map_err(wrap_py_err)?;
        if let Some(groups) = row_groups {
            builder = builder.with_row_groups(groups);
        }
        let schema = builder.schema().clone();

        let headers: Vec<Py<PyString>> = schema
            .fields()
            .iter()
            .map(|f: &FieldRef| PyString::new(py, f.name()).unbind())
            .collect();

        let reader = SmartReader::new(&path, builder, move |b| {
            match b.with_batch_size(batch_size).build() {
                Ok(reader) => Box::new(reader.map(|res| res.map_err(|e| format!("{}", e))))
                    as Box<dyn Iterator<Item = Result<RecordBatch, String>> + Send>,
                Err(e) => Box::new(std::iter::once(Err(format!(
                    "Failed to build parquet reader: {}",
                    e
                ))))
                    as Box<dyn Iterator<Item = Result<RecordBatch, String>> + Send>,
            }
        });

        let models = py.import("zoopipe.structs")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(ParquetReader {
            state: Mutex::new(ParquetReaderState {
                reader,
                current_batch: None,
                position: 0,
                rows_to_skip: offset,
                rows_to_read: limit,
                rows_read: 0,
            }),
            headers,
            status_pending,
            generate_ids,
            keys: InternedKeys::new(py),
        })
    }

    #[staticmethod]
    pub fn count_rows(path: String) -> PyResult<usize> {
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

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(boxed_reader).map_err(wrap_py_err)?;
        Ok(builder.metadata().file_metadata().num_rows() as usize)
    }

    #[staticmethod]
    pub fn get_row_groups_info(path: String) -> PyResult<Vec<usize>> {
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

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(boxed_reader).map_err(wrap_py_err)?;
        let metadata = builder.metadata();
        let mut info = Vec::new();
        for i in 0..metadata.num_row_groups() {
            info.push(metadata.row_group(i).num_rows() as usize);
        }
        Ok(info)
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

        // Optimization: Fast-path for full batches
        if state.current_batch.is_none() && state.rows_to_skip == 0 {
            // Peek at limit check
            let can_read_full_batch = if let Some(limit) = state.rows_to_read {
                state.rows_read < limit
            } else {
                true
            };

            if can_read_full_batch {
                match state.reader.next() {
                    Some(Ok(batch)) => {
                        let batch_len = batch.num_rows();

                        // Check if batch fits in request AND within global limit
                        let fits_in_request = batch_len <= batch_size;
                        let fits_in_limit = if let Some(limit) = state.rows_to_read {
                            state.rows_read + batch_len <= limit
                        } else {
                            true
                        };

                        if fits_in_request && fits_in_limit {
                            let current_pos = state.position;
                            state.position += batch_len;
                            state.rows_read += batch_len;

                            let envelopes =
                                crate::parsers::arrow_utils::record_batch_to_py_envelopes(
                                    py,
                                    batch,
                                    &self.keys,
                                    self.status_pending.bind(py),
                                    self.generate_ids,
                                    current_pos,
                                )?;
                            return Ok(Some(envelopes));
                        } else {
                            // Too big or hits limit -> process row-by-row
                            state.current_batch = Some((batch, 0));
                        }
                    }
                    Some(Err(_e)) => return Ok(None), // Error handled in loop
                    None => return Ok(None),
                }
            }
        }

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
    fn next_internal<'py>(
        &self,
        py: Python<'py>,
        state: &mut ParquetReaderState,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        match state.next_record() {
            Some(Ok((batch, row_idx, pos))) => {
                let raw_data = PyDict::new(py);
                for (i, header) in self.headers.iter().enumerate() {
                    let col = batch.column(i);
                    let val = arrow_to_py(py, col, row_idx)?;
                    raw_data.set_item(header.bind(py), val)?;
                }

                let env = crate::utils::wrap_in_envelope(
                    py,
                    &self.keys,
                    raw_data.into_any(),
                    self.status_pending.bind(py).clone(),
                    pos,
                    self.generate_ids,
                )?;

                Ok(Some(env))
            }
            Some(Err(_e)) => Ok(None),
            None => Ok(None),
        }
    }
}

#[pyclass]
pub struct MultiParquetReader {
    paths: Vec<String>,
    current_reader: Mutex<Option<ParquetReader>>,
    current_path_idx: Mutex<usize>,
    generate_ids: bool,
    batch_size: usize,
    history_position: AtomicUsize,
}

#[pymethods]
impl MultiParquetReader {
    #[new]
    pub fn new(paths: Vec<String>, generate_ids: bool, batch_size: usize) -> Self {
        MultiParquetReader {
            paths,
            current_reader: Mutex::new(None),
            current_path_idx: Mutex::new(0),
            generate_ids,
            batch_size,
            history_position: AtomicUsize::new(0),
        }
    }

    pub fn __next__(slf: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyAny>>> {
        let py = slf.py();
        slf.get_next_item(py)
    }

    pub fn read_batch<'py>(
        &self,
        py: Python<'py>,
        batch_size: usize,
    ) -> PyResult<Option<Bound<'py, PyList>>> {
        let batch = PyList::empty(py);
        for _ in 0..batch_size {
            if let Some(item) = self.get_next_item(py)? {
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

impl MultiParquetReader {
    pub(crate) fn get_next_item<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        loop {
            // First, try to get an item from the current reader if it exists
            let mut exhausted = false;
            {
                let mut reader_guard = self
                    .current_reader
                    .lock()
                    .map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
                if let Some(reader) = reader_guard.as_ref() {
                    let mut state = reader.state.lock().map_err(|_| PipeError::MutexLock)?;
                    match reader.next_internal(py, &mut state)? {
                        Some(val) => {
                            let pos = self.history_position.fetch_add(1, Ordering::Relaxed);
                            if let Ok(dict) = val.cast::<PyDict>() {
                                dict.set_item("_pos", pos)?;
                            }
                            return Ok(Some(val));
                        }
                        None => {
                            // Current reader exhausted
                            exhausted = true;
                        }
                    }
                }

                if exhausted {
                    *reader_guard = None;
                }
            }

            if exhausted {
                *self
                    .current_path_idx
                    .lock()
                    .map_err(|_| PyRuntimeError::new_err("Lock poisoned"))? += 1;
                // continue loop to open next file
                continue;
            }

            // If we are here, we need to open a new file or we are finished
            let (path, _idx) = {
                let idx_guard = self
                    .current_path_idx
                    .lock()
                    .map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
                let idx = *idx_guard;
                if idx >= self.paths.len() {
                    return Ok(None);
                }
                (self.paths[idx].clone(), idx)
            };

            let reader =
                ParquetReader::new(py, path, self.generate_ids, self.batch_size, None, 0, None)?;

            let mut reader_guard = self
                .current_reader
                .lock()
                .map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
            *reader_guard = Some(reader);
            // continue loop to read from the newly opened reader
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
    writer: Mutex<Option<ArrowWriter<SharedWriter>>>,
    inner_writer: Mutex<Option<Arc<StdMutex<BoxedWriter>>>>,
    schema: Mutex<Option<SchemaRef>>,
}

#[pymethods]
impl ParquetWriter {
    #[new]
    pub fn new(path: String) -> Self {
        ParquetWriter {
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
                    let dt = infer_type(&val);
                    fields.push(Field::new(key.clone(), dt, true));
                } else {
                    fields.push(Field::new(key.clone(), DataType::Utf8, true));
                }
            }
            let schema = SchemaRef::new(Schema::new(fields));

            let controller = StorageController::new(&self.path).map_err(wrap_py_err)?;
            let boxed_writer = if self.path.starts_with("s3://") {
                BoxedWriter::Remote(RemoteWriter::new(
                    controller.store(),
                    ObjectPath::from(controller.path()),
                ))
            } else {
                let file = crate::io::create_local_file(&self.path).map_err(wrap_py_err)?;
                BoxedWriter::File(std::io::BufWriter::new(file))
            };

            let shared_writer = Arc::new(StdMutex::new(boxed_writer));
            let props = WriterProperties::builder()
                .set_max_row_group_size(8_192)
                .build();

            *writer_guard = Some(
                ArrowWriter::try_new(
                    SharedWriter(shared_writer.clone()),
                    schema.clone(),
                    Some(props),
                )
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
            .ok_or_else(|| PyRuntimeError::new_err("ParquetWriter failed to initialize"))?;
        let schema = schema_guard
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("ParquetWriter schema failed to initialize"))?;

        let batch = build_record_batch(py, schema, list)?;
        writer.write(&batch).map_err(wrap_py_err)?;
        Ok(())
    }

    pub fn close(&self) -> PyResult<()> {
        let mut writer_guard = self
            .writer
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
        if let Some(w) = writer_guard.take() {
            let _metadata = w.close().map_err(wrap_py_err)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::BoxedReader;
    use std::sync::Arc;

    fn make_test_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        let mut builder = arrow::array::Int32Builder::with_capacity(rows);
        for i in 0..rows {
            builder.append_value(i as i32);
        }
        let array = builder.finish();

        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn test_parquet_state_iteration() {
        let batch = make_test_batch(3); // 0, 1, 2

        let boxed_reader = BoxedReader::Cursor(std::io::Cursor::new(vec![]));
        let reader = SmartReader::new("", boxed_reader, |_| {
            std::iter::empty::<Result<RecordBatch, String>>()
        });

        let mut state = ParquetReaderState {
            reader,
            current_batch: Some((batch.clone(), 0)),
            position: 0,
            rows_to_skip: 0,
            rows_to_read: None,
            rows_read: 0,
        };

        // Row 0
        let res1 = state.next_record();
        assert!(res1.is_some());
        let (_b1, r1, p1) = res1.unwrap().unwrap();
        assert_eq!(r1, 0);
        assert_eq!(p1, 0);

        // Row 1
        let res2 = state.next_record();
        let (_, r2, p2) = res2.unwrap().unwrap();
        assert_eq!(r2, 1);
        assert_eq!(p2, 1);

        // Row 2
        let res3 = state.next_record();
        let (_, r3, p3) = res3.unwrap().unwrap();
        assert_eq!(r3, 2);
        assert_eq!(p3, 2);

        // Row 3 -> Should exhaust batch.
        assert!(state.next_record().is_none());
    }
}
