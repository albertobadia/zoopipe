use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::sync::{Mutex, OnceLock};
use pyo3::types::{PyAnyMethods, PyString, PyDict, PyList};
use tokio::runtime::Runtime;
use futures_util::StreamExt;
use sqlx::{Row, Column, AnyPool, query, any::AnyPoolOptions, QueryBuilder, Any};
use crate::error::PipeError;
use crate::utils::interning::InternedKeys;


fn init_drivers() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        sqlx::any::install_default_drivers();
    });
}

fn get_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        Runtime::new().expect("Failed to create Tokio runtime")
    })
}

fn ensure_parent_dir(uri: &str) {
    let path_part = if let Some(path) = uri.strip_prefix("sqlite:///") {
        path
    } else if let Some(path) = uri.strip_prefix("sqlite://") {
        path
    } else if let Some(path) = uri.strip_prefix("sqlite:") {
        path
    } else {
        return;
    };

    let path_only = path_part.split('?').next().unwrap_or(path_part);
    if !path_only.is_empty() && path_only != ":memory:"
        && let Some(parent) = std::path::Path::new(path_only).parent()
        && !parent.as_os_str().is_empty() {
            let _ = std::fs::create_dir_all(parent);
        }
}

#[derive(Clone, Debug)]
pub enum SQLValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Null,
}

enum SQLData {
    Metadata(Vec<String>),
    Row(Vec<SQLValue>),
    Error(String),
}

/// Native SQL reader that streams database records into the pipeline.
/// 
/// It utilizes a background thread to fetch rows asynchronously, ensuring 
/// that the main pipeline loop stays focused on processing.
#[pyclass]
pub struct SQLReader {
    uri: String,
    query: String,
    receiver: Mutex<Option<crossbeam_channel::Receiver<SQLData>>>,
    column_names: Mutex<Option<Vec<Py<PyString>>>>,
    position: Mutex<usize>,
    status_pending: Py<PyAny>,
    generate_ids: bool,
    keys: InternedKeys,
}

#[pymethods]
impl SQLReader {
    #[new]
    #[pyo3(signature = (uri, query, generate_ids=true))]
    fn new(
        py: Python<'_>,
        uri: String,
        query: String,
        generate_ids: bool,
    ) -> PyResult<Self> {
        init_drivers();
        let models = py.import("zoopipe.report")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(SQLReader {
            uri,
            query,
            receiver: Mutex::new(None),
            column_names: Mutex::new(None),
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
        
        for _ in 0..batch_size {
            if let Some(item) = self.next_internal(py)? {
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

impl SQLReader {
    fn next_internal<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let mut receiver_opt = self.receiver.lock().map_err(|_| PipeError::MutexLock)?;
        
        if receiver_opt.is_none() {
            let (tx, rx) = crossbeam_channel::bounded(1000);
            let uri = self.uri.clone();
            let query_str = self.query.clone();
            
            std::thread::spawn(move || {
                let rt = get_runtime();

                rt.block_on(async {
                    ensure_parent_dir(&uri);
                    let pool = match AnyPoolOptions::new().max_connections(1).connect(&uri).await {
                        Ok(p) => p,
                        Err(e) => {
                            let _ = tx.send(SQLData::Error(format!("Connection failed: {}", e)));
                            return;
                        }
                    };

                    let mut rows = query(&query_str).fetch(&pool);

                    let mut first = true;

                    while let Some(row_res) = rows.next().await {
                        let row = match row_res {
                            Ok(r) => r,
                            Err(e) => {
                                let _ = tx.send(SQLData::Error(format!("Query failed: {}", e)));
                                break;
                            }
                        };

                        if first {
                            let cols: Vec<String> = row.columns().iter().map(|c| c.name().to_string()).collect();
                            if tx.send(SQLData::Metadata(cols)).is_err() {
                                break;
                            }
                            first = false;
                        }

                        let mut record = Vec::with_capacity(row.columns().len());
                        for i in 0..row.columns().len() {
                            let val = match row.try_get::<String, usize>(i) {
                                Ok(s) => SQLValue::String(s),
                                Err(_) => match row.try_get::<i64, usize>(i) {
                                    Ok(v) => SQLValue::Int(v),
                                    Err(_) => match row.try_get::<f64, usize>(i) {
                                        Ok(v) => SQLValue::Float(v),
                                        Err(_) => match row.try_get::<bool, usize>(i) {
                                            Ok(v) => SQLValue::Bool(v),
                                            Err(_) => SQLValue::Null,
                                        },
                                    },
                                },
                            };
                            record.push(val);
                        }

                        if tx.send(SQLData::Row(record)).is_err() {
                            break;
                        }
                    }
                });
            });

            *receiver_opt = Some(rx);
        }

        let rx = receiver_opt.as_ref()
            .expect("Receiver should be initialized before reading rows").clone();
        
        drop(receiver_opt);
        
        loop {
            match rx.recv() {
                Ok(SQLData::Metadata(cols)) => {
                    let py_cols: Vec<Py<PyString>> = cols.into_iter()
                        .map(|s| PyString::new(py, &s).unbind())
                        .collect();
                    let mut column_names_opt = self.column_names.lock().map_err(|_| PipeError::MutexLock)?;
                    *column_names_opt = Some(py_cols);
                }
                Ok(SQLData::Row(row_values)) => {
                    let column_names_opt = self.column_names.lock().map_err(|_| PipeError::MutexLock)?;
                    let cols = column_names_opt.as_ref().expect("Column names should be received before rows");
                    
                    let mut pos = self.position.lock().map_err(|_| PipeError::MutexLock)?;
                    let current_pos = *pos;
                    *pos += 1;
                    
                    let raw_data = PyDict::new(py);
                    for (i, value) in row_values.into_iter().enumerate() {
                        let col_name = cols[i].bind(py);
                        match value {
                            SQLValue::String(s) => raw_data.set_item(col_name, s)?,
                            SQLValue::Int(v) => raw_data.set_item(col_name, v)?,
                            SQLValue::Float(v) => raw_data.set_item(col_name, v)?,
                            SQLValue::Bool(v) => raw_data.set_item(col_name, v)?,
                            SQLValue::Null => raw_data.set_item(col_name, py.None())?,
                        };
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

                    return Ok(Some(envelope.into_any()));
                }
                Ok(SQLData::Error(e)) => return Err(PyRuntimeError::new_err(e)),
                Err(_) => return Ok(None),
            }
        }
    }
}

/// Fast SQL writer that performs bulk insertions into database tables.
/// 
/// It manages connection pools and transactions automatically, supporting 
/// efficient batch writes to any SQLAlchemy-compatible database.
#[pyclass]
pub struct SQLWriter {
    uri: String,
    table_name: String,
    mode: String,
    table_created: Mutex<bool>,
    fieldnames: Mutex<Option<Vec<Py<PyString>>>>,
    batch_size: usize,
}

#[pymethods]
impl SQLWriter {
    #[new]
    #[pyo3(signature = (uri, table_name, mode="replace", batch_size=500))]
    pub fn new(
        uri: String,
        table_name: String,
        mode: &str,
        batch_size: usize,
    ) -> PyResult<Self> {
        init_drivers();
        if mode != "replace" && mode != "append" && mode != "fail" {
            return Err(PyRuntimeError::new_err(
                "mode must be 'replace', 'append', or 'fail'"
            ));
        }

        Ok(SQLWriter {
            uri,
            table_name,
            mode: mode.to_string(),
            table_created: Mutex::new(false),
            fieldnames: Mutex::new(None),
            batch_size,
        })
    }

    pub fn write(&self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        let entries = PyList::new(py, [data])?;
        self.write_batch(py, entries.into_any())
    }

    pub fn write_batch(&self, py: Python<'_>, entries: Bound<'_, PyAny>) -> PyResult<()> {
        let mut table_created = self.table_created.lock()
            .map_err(|_| PipeError::MutexLock)?;
        let mut fieldnames = self.fieldnames.lock()
            .map_err(|_| PipeError::MutexLock)?;

        let mut it = entries.try_iter()?;
        
        let first_entry = if let Some(res) = it.next() {
            let entry = res?;
            let record = entry.cast::<PyDict>()?;
            if !*table_created {
                self.ensure_table_created(py, record, &mut table_created, &mut fieldnames)?;
            }
            Some(record.clone().unbind())
        } else {
            return Ok(());
        };

        let mut all_records = Vec::new();
        if let Some(first) = first_entry {
            all_records.push(first);
        }
        for entry_res in it {
            let entry = entry_res?;
            all_records.push(entry.cast::<PyDict>()?.clone().unbind());
        }

        let field_names = fieldnames.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Fieldnames not initialized"))?;
        let fields: Vec<String> = field_names
            .iter()
            .map(|f| Ok(f.bind(py).to_str()?.to_string()))
            .collect::<PyResult<Vec<_>>>()?;
        let uri = self.uri.clone();
        let table_name = self.table_name.clone();

        let rt = get_runtime();
        rt.block_on(async {
            let pool = AnyPool::connect(&uri).await.map_err(|e| PyRuntimeError::new_err(format!("Failed to connect: {}", e)))?;
            let mut tx = pool.begin().await.map_err(|e| PyRuntimeError::new_err(format!("Failed to start transaction: {}", e)))?;
            for chunk in all_records.chunks(self.batch_size) {
                let mut query_builder: QueryBuilder<Any> = QueryBuilder::new(format!("INSERT INTO {} ({}) ", table_name, fields.join(", ")));
                
                query_builder.push_values(chunk, |mut b, record_py| {
                    let py_inner = record_py.bind(py);
                    for field in &fields {
                        let val_opt = py_inner.get_item(field).unwrap_or(None);
                        if let Some(val) = val_opt {
                            if val.is_none() {
                                b.push_bind(None::<String>);
                            } else if let Ok(s) = val.extract::<String>() {
                                b.push_bind(s);
                            } else if let Ok(i) = val.extract::<i64>() {
                                b.push_bind(i);
                            } else if let Ok(f) = val.extract::<f64>() {
                                b.push_bind(f);
                            } else if let Ok(bool_val) = val.extract::<bool>() {
                                b.push_bind(bool_val);
                            } else {
                                b.push_bind(val.to_string());
                            }
                        } else {
                            b.push_bind(None::<String>);
                        }
                    }
                });

                let query = query_builder.build();
                query.execute(&mut *tx).await.map_err(|e| PyRuntimeError::new_err(format!("Batch insert failed: {}", e)))?;
            }

            tx.commit().await.map_err(|e| PyRuntimeError::new_err(format!("Failed to commit transaction: {}", e)))?;
            Ok::<(), PyErr>(())
        })?;

        Ok(())
    }

    pub fn flush(&self) -> PyResult<()> { Ok(()) }
    pub fn close(&self) -> PyResult<()> { self.flush() }
}

impl SQLWriter {
    fn ensure_table_created(
        &self,
        py: Python<'_>,
        record: &Bound<'_, PyDict>,
        table_created: &mut bool,
        fieldnames: &mut Option<Vec<Py<PyString>>>,
    ) -> PyResult<()> {
        if fieldnames.is_none() {
            let keys_list = record.keys();
            let mut record_keys: Vec<Py<PyString>> = Vec::new();
            for k in keys_list.iter() {
                record_keys.push(k.cast::<PyString>()?.clone().unbind());
            }
            record_keys.sort_by(|a, b| {
                a.bind(py).to_str().unwrap_or_default()
                    .cmp(b.bind(py).to_str().unwrap_or_default())
            });
            *fieldnames = Some(record_keys);
        }

        let fields = fieldnames.as_ref()
            .expect("Fieldnames should be set by ensure_table_created()");
        let uri = self.uri.clone();
        let table_name = self.table_name.clone();
        let mode = self.mode.clone();

        let rt = get_runtime();
        rt.block_on(async {
            ensure_parent_dir(&uri);
            let pool = AnyPool::connect(&uri).await.map_err(|e| PyRuntimeError::new_err(format!("Failed to connect: {}", e)))?;
            
            if mode == "replace" {
                let _ = query(&format!("DROP TABLE IF EXISTS {}", table_name)).execute(&pool).await;
            }

            let mut columns = Vec::new();
            for f_py in fields {
                let f = f_py.bind(py);
                columns.push(format!("{} TEXT", f.to_str().unwrap_or("column")));
            }
            
            let create_sql = format!("CREATE TABLE IF NOT EXISTS {} ({})", table_name, columns.join(", "));
            query(&create_sql).execute(&pool).await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to create table: {}", e)))?;

            Ok::<(), PyErr>(())
        })?;

        *table_created = true;
        Ok(())
    }
}
