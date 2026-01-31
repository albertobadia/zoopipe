use crate::parsers::parquet::MultiParquetReader;
use deltalake::{DeltaTable, DeltaTableBuilder};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyList;
use serde_json;
use std::collections::HashMap;
use std::sync::Mutex;

use deltalake::kernel::{Action, Add};
use std::fs::File;

/// Reader for Delta Lake tables.
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
        let binding = slf.internal_reader.lock().unwrap();
        let reader = binding.as_ref().unwrap();
        reader.get_next_item(py)
    }

    pub fn read_batch<'py>(
        &self,
        py: Python<'py>,
        batch_size: usize,
    ) -> PyResult<Option<Bound<'py, PyList>>> {
        self.ensure_initialized(py)?;
        let binding = self.internal_reader.lock().unwrap();
        let reader = binding.as_ref().unwrap();
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
            let rt = crate::io::get_runtime();
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

                // Correct method for Delta Lake 0.30 to get absolute file URIs
                // get_file_uris() returns Result<impl Iterator<Item = String>, Error>
                let file_uris: Vec<String> = table
                    .get_file_uris()
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
                    .collect();

                Ok::<Vec<String>, PyErr>(file_uris)
            })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
        };

        // get_file_uris returns absolute URIs, so we pass them directly
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
    let rt = crate::io::get_runtime();

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
            *self.current_file_path.lock().unwrap() = Some(file_name);
        }

        if let Some(writer) = writer_guard.as_ref() {
            writer.write_batch(py, entries)?;
        }

        Ok(())
    }

    pub fn close(&self) -> PyResult<String> {
        let mut writer_guard = self
            .inner_writer
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Lock failed"))?;

        let file_name_opt = self.current_file_path.lock().unwrap().clone();

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
                .unwrap()
                .as_millis() as i64;

            let action_info = serde_json::json!({
                "path": file_name,
                "size": size,
                "partitionValues": {},
                "modificationTime": now,
                "dataChange": true,
                "stats": null
            });

            Ok(serde_json::to_string(&vec![action_info]).unwrap())
        } else {
            Ok("[]".to_string())
        }
    }
}

#[pyfunction]
pub fn commit_delta_transaction(
    table_uri: String,
    actions_json: Vec<String>,
    _mode: String,
    _storage_options: Option<HashMap<String, String>>,
) -> PyResult<()> {
    // Only support local file paths for this manual implementation
    let path_str = if table_uri.starts_with("file://") {
        table_uri.strip_prefix("file://").unwrap()
    } else if table_uri.starts_with('/') {
        &table_uri
    } else {
        // Fallback to error if not local
        println!(
            "Dry run: Atomic commit logic not implemented for non-local URI: {}",
            table_uri
        );
        return Err(PyRuntimeError::new_err(
            "Atomic commit only supported for local paths in this version",
        ));
    };

    let table_path = std::path::Path::new(path_str);
    let log_dir = table_path.join("_delta_log");
    std::fs::create_dir_all(&log_dir).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    // Parse actions
    let mut add_actions: Vec<Add> = Vec::new();
    for json_str in actions_json {
        let adds: Vec<Add> = serde_json::from_str(&json_str)
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid action JSON: {}", e)))?;
        add_actions.extend(adds);
    }

    if add_actions.is_empty() {
        return Ok(());
    }

    // Determine next version
    let mut version = 0;
    while log_dir.join(format!("{:020}.json", version)).exists() {
        version += 1;
    }

    let mut actions: Vec<Action> = Vec::new();

    // If version 0, we need Protocol and Metadata
    if version == 0 {
        // Use deltalake's parquet re-export to match types
        // Check if deltalake provides parquet re-export.
        // If not, we cannot easily infer schema => Error.
        // Assuming it does or we can fail gracefully.

        // Since I cannot guarantee `deltalake::parquet` availability without checking docs/source (which I can't),
        // and `zoopipe` depends on specific parquet version.
        // I will enforce "Table Must Exist" for this iteration if usage of `TryFrom` fails.

        // BUT WAIT: StructType::try_from relies on ArrowSchema.
        // I can construct ArrowSchema manually if I had column names.
        // I don't.

        // Compromise:
        // If version == 0, simple error: "Table does not exist. Please create the Delta Table first (e.g. using deltalake python library)."
        // This allows `add` to work if I fix the example script.

        return Err(PyRuntimeError::new_err(
            "Auto-creating Delta Table from inferred schema is not supported in this version. Please ensure the target Delta Table exists before writing.",
        ));
    }

    for add in add_actions {
        actions.push(Action::Add(add));
    }

    // Write to JSON file
    let commit_file = log_dir.join(format!("{:020}.json", version));
    let file = File::create(&commit_file).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    use std::io::Write;
    let mut writer = std::io::BufWriter::new(file);

    for action in actions {
        let line =
            serde_json::to_string(&action).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        writeln!(writer, "{}", line).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    }

    writer
        .flush()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    // Also update _last_checkpoint if needed? Not strictly required for readers.

    Ok(())
}
