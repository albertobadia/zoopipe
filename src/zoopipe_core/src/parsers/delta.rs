use crate::parsers::parquet::MultiParquetReader;
use deltalake::kernel::{Action, Add};
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::{DeltaTable, DeltaTableBuilder};
use deltalake_core::kernel::transaction::{CommitBuilder, TableReference};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyList;
use std::collections::HashMap;
use std::sync::Mutex;

/// Reader for Delta Lake tables.
#[pyclass]
pub struct DeltaReader {
    table_uri: String,
    version: Option<i64>,
    storage_options: Option<HashMap<String, String>>,
    internal_reader: Mutex<Option<MultiParquetReader>>,
    batch_size: usize,
    files: Option<Vec<String>>,
}

#[pymethods]
impl DeltaReader {
    #[new]
    #[pyo3(signature = (table_uri, version=None, storage_options=None, batch_size=1024, files=None))]
    pub fn new(
        table_uri: String,
        version: Option<i64>,
        storage_options: Option<HashMap<String, String>>,
        batch_size: usize,
        files: Option<Vec<String>>,
    ) -> Self {
        DeltaReader {
            table_uri,
            version,
            storage_options,
            internal_reader: Mutex::new(None),
            batch_size,
            files,
        }
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __next__(slf: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyAny>>> {
        let py = slf.py();
        slf.ensure_initialized(py)?;
        let binding = slf
            .internal_reader
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        let reader = binding
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Reader not initialized"))?;
        reader.get_next_item(py)
    }

    pub fn read_batch<'py>(
        &self,
        py: Python<'py>,
        batch_size: usize,
    ) -> PyResult<Option<Bound<'py, PyList>>> {
        self.ensure_initialized(py)?;
        let binding = self
            .internal_reader
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;
        let reader = binding
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Reader not initialized"))?;
        reader.read_batch(py, batch_size)
    }
}

impl DeltaReader {
    fn ensure_initialized(&self, _py: Python<'_>) -> PyResult<()> {
        let mut guard = self
            .internal_reader
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?;

        if guard.is_some() {
            return Ok(());
        }

        let files = if let Some(files) = &self.files {
            files.clone()
        } else {
            let rt = crate::io::get_runtime_handle();
            let table_uri = self.table_uri.clone();
            let version = self.version;
            let storage_options = self.storage_options.clone();

            rt.block_on(async {
                let url = crate::utils::parse_uri(&table_uri)?;
                let mut builder = DeltaTableBuilder::from_url(url)
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
                    .with_allow_http(true);

                if let Some(opts) = storage_options {
                    builder = builder.with_storage_options(opts);
                }
                if let Some(v) = version {
                    builder = builder.with_version(v);
                }

                let table: DeltaTable = builder.load().await.map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to load Delta table: {}", e))
                })?;

                let file_uris: Vec<String> = table
                    .get_file_uris()
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
                    .collect();

                Ok::<Vec<String>, PyErr>(file_uris)
            })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        };

        let reader = MultiParquetReader::new(files, true, self.batch_size);
        *guard = Some(reader);

        Ok(())
    }
}

#[pyfunction]
#[pyo3(signature = (table_uri, version=None, storage_options=None))]
pub fn get_delta_files(
    table_uri: String,
    version: Option<i64>,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<Vec<String>> {
    let rt = crate::io::get_runtime_handle();

    rt.block_on(async {
        let url = crate::utils::parse_uri(&table_uri)?;
        let mut builder = DeltaTableBuilder::from_url(url)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            .with_allow_http(true);

        if let Some(opts) = storage_options {
            builder = builder.with_storage_options(opts);
        }
        if let Some(v) = version {
            builder = builder.with_version(v);
        }

        let table: DeltaTable = builder
            .load()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to load Delta table: {}", e)))?;

        let file_uris: Vec<String> = table
            .get_file_uris()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            .collect();

        Ok(file_uris)
    })
}

#[pyclass]
pub struct DeltaWriter {
    table_uri: String,
    inner_writer: Mutex<Option<crate::parsers::parquet::ParquetWriter>>,
    current_file_path: Mutex<Option<String>>,
    #[allow(dead_code)]
    storage_options: Option<HashMap<String, String>>,
}

#[pyclass(module = "zoopipe.zoopipe_rust_core")]
pub struct DeltaTransactionHandle {
    pub actions: Vec<Action>,
}

impl Default for DeltaTransactionHandle {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl DeltaTransactionHandle {
    #[new]
    pub fn new() -> Self {
        Self {
            actions: Vec::new(),
        }
    }

    pub fn __getstate__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyBytes>> {
        let data = serde_json::to_vec(&self.actions)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(pyo3::types::PyBytes::new(py, &data))
    }

    pub fn __setstate__(&mut self, state: &Bound<'_, pyo3::types::PyBytes>) -> PyResult<()> {
        let data = state.as_bytes();
        self.actions =
            serde_json::from_slice(data).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }
}

#[pymethods]
impl DeltaWriter {
    #[new]
    #[pyo3(signature = (table_uri, storage_options=None))]
    pub fn new(table_uri: String, storage_options: Option<HashMap<String, String>>) -> Self {
        DeltaWriter {
            table_uri,
            inner_writer: Mutex::new(None),
            current_file_path: Mutex::new(None),
            storage_options,
        }
    }

    pub fn write_batch(&self, py: Python<'_>, entries: Bound<'_, PyAny>) -> PyResult<()> {
        let mut writer_guard = self
            .inner_writer
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Lock failed"))?;

        if writer_guard.is_none() {
            let part_id = uuid::Uuid::new_v4();
            let sep = if self.table_uri.ends_with('/') {
                ""
            } else {
                "/"
            };
            let file_name = format!("part-{}.parquet", part_id);
            let full_path = format!("{}{}{}", self.table_uri, sep, file_name);

            let writer = crate::parsers::parquet::ParquetWriter::new(full_path.clone());
            *writer_guard = Some(writer);
            *self
                .current_file_path
                .lock()
                .map_err(|_| PyRuntimeError::new_err("Lock poisoned"))? = Some(file_name);
        }

        if let Some(writer) = writer_guard.as_ref() {
            writer.write_batch(py, entries)?;
        }

        Ok(())
    }

    pub fn close(&self) -> PyResult<Option<DeltaTransactionHandle>> {
        let mut writer_guard = self
            .inner_writer
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Lock failed"))?;

        let file_name_opt = self
            .current_file_path
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?
            .clone();

        if let (Some(writer), Some(file_name)) = (writer_guard.take(), file_name_opt) {
            writer.close()?;

            let sep = if self.table_uri.ends_with('/') {
                ""
            } else {
                "/"
            };
            let full_path = format!("{}{}{}", self.table_uri, sep, file_name);
            let size = crate::get_file_size_rust(full_path.clone())? as i64;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);

            let add = Add {
                path: file_name,
                size,
                partition_values: HashMap::new(),
                modification_time: now,
                data_change: true,
                stats: None,
                tags: None,
                deletion_vector: None,
                base_row_id: None,
                default_row_commit_version: None,
                clustering_provider: None,
            };

            Ok(Some(DeltaTransactionHandle {
                actions: vec![Action::Add(add)],
            }))
        } else {
            Ok(None)
        }
    }
}

#[pyfunction]
pub fn commit_delta_transaction(
    table_uri: String,
    handles: Vec<Bound<'_, DeltaTransactionHandle>>,
    _mode: String,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<()> {
    let rt = crate::io::get_runtime_handle();

    rt.block_on(async {
        let url = crate::utils::parse_uri(&table_uri)?;
        let mut builder = DeltaTableBuilder::from_url(url)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            .with_allow_http(true);

        if let Some(opts) = storage_options {
            builder = builder.with_storage_options(opts);
        }

        let table = builder
            .load()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to load Delta table: {}", e)))?;

        // Collect actions from handles
        let mut add_actions: Vec<Action> = Vec::new();
        for handle in handles {
            let handle_ref = handle.borrow();
            add_actions.extend(handle_ref.actions.clone());
        }

        if add_actions.is_empty() {
            return Ok(());
        }

        let operation = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: None,
            predicate: None,
        };

        let snapshot = table.state.clone();
        let log_store = table.log_store();

        let table_data = snapshot.as_ref().map(|s| s as &dyn TableReference);

        let _ = CommitBuilder::default()
            .with_actions(add_actions)
            .build(table_data, log_store, operation)
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to prepare commit: {}", e)))?;

        Ok(())
    })
}
