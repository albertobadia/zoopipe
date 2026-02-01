use futures_util::StreamExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use sqlx::{any::AnyPoolOptions, query};
use std::sync::Mutex;

use super::types::SQLData;
use super::utils::{ensure_parent_dir, init_drivers};
use crate::error::PipeError;
use crate::io::get_runtime_handle;
use crate::utils::interning::InternedKeys;

struct SQLReaderState {
    receiver: Option<crossbeam_channel::Receiver<SQLData>>,
    column_names: Option<Vec<Py<PyString>>>,
    position: usize,
}

#[pyclass]
pub struct SQLReader {
    uri: String,
    query: String,
    state: Mutex<SQLReaderState>,
    status_pending: Py<PyAny>,
    generate_ids: bool,
    keys: InternedKeys,
}

#[pymethods]
impl SQLReader {
    #[new]
    #[pyo3(signature = (uri, query, generate_ids=true))]
    fn new(py: Python<'_>, uri: String, query: String, generate_ids: bool) -> PyResult<Self> {
        init_drivers();
        let models = py.import("zoopipe.structs")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(SQLReader {
            uri,
            query,
            state: Mutex::new(SQLReaderState {
                receiver: None,
                column_names: None,
                position: 0,
            }),
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

impl SQLReader {
    fn ensure_receiver(
        &self,
        state: &mut SQLReaderState,
    ) -> PyResult<crossbeam_channel::Receiver<SQLData>> {
        if let Some(rx) = state.receiver.as_ref() {
            return Ok(rx.clone());
        }

        let (tx, rx) = crossbeam_channel::bounded(1000);
        let uri = self.uri.clone();
        let query_str = self.query.clone();

        std::thread::spawn(move || {
            let rt = get_runtime_handle();
            rt.block_on(async {
                if let Err(e) = Self::spawn_fetcher(uri, query_str, tx).await {
                    eprintln!("SQL fetcher thread error: {}", e);
                }
            });
        });

        let rx_clone = rx.clone();
        state.receiver = Some(rx);
        Ok(rx_clone)
    }

    async fn spawn_fetcher(
        uri: String,
        query_str: String,
        tx: crossbeam_channel::Sender<SQLData>,
    ) -> Result<(), sqlx::Error> {
        ensure_parent_dir(&uri);
        let pool = match AnyPoolOptions::new().max_connections(1).connect(&uri).await {
            Ok(p) => p,
            Err(e) => {
                let _ = tx.send(SQLData::Error(format!("Connection failed: {}", e)));
                return Err(e);
            }
        };

        let mut rows = query(&query_str).fetch(&pool);
        let mut first = true;

        while let Some(row_res) = rows.next().await {
            let row = match row_res {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(SQLData::Error(format!("Query failed: {}", e)));
                    return Err(e);
                }
            };

            if first {
                if tx.send(SQLData::metadata_from_row(&row)).is_err() {
                    break;
                }
                first = false;
            }

            if tx.send(SQLData::row_from_row(&row)).is_err() {
                break;
            }
        }
        Ok(())
    }

    fn next_internal<'py>(
        &self,
        py: Python<'py>,
        state: &mut SQLReaderState,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        let rx = self.ensure_receiver(state)?;

        loop {
            match rx.recv() {
                Ok(SQLData::Metadata(cols)) => {
                    let py_cols: Vec<Py<PyString>> = cols
                        .into_iter()
                        .map(|s| PyString::new(py, &s).unbind())
                        .collect();
                    state.column_names = Some(py_cols);
                }
                Ok(SQLData::Row(row_values)) => {
                    let cols = match state.column_names.as_ref() {
                        Some(c) => c,
                        None => {
                            return Err(PyRuntimeError::new_err(
                                "Column names not received before rows",
                            ));
                        }
                    };

                    let current_pos = state.position;
                    state.position += 1;

                    let raw_data = PyDict::new(py);
                    for (i, value) in row_values.into_iter().enumerate() {
                        let key = cols.get(i).ok_or_else(|| {
                            PyRuntimeError::new_err("Value index out of bounds for column names")
                        })?;
                        raw_data.set_item(key.bind(py), value.into_pyobject(py)?)?;
                    }

                    return Ok(Some(crate::utils::wrap_in_envelope(
                        py,
                        &self.keys,
                        raw_data.into_any(),
                        self.status_pending.bind(py).clone(),
                        current_pos,
                        self.generate_ids,
                    )?));
                }
                Ok(SQLData::Error(e)) => return Err(PyRuntimeError::new_err(e)),
                Err(_) => return Ok(None),
            }
        }
    }
}
