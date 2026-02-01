use crate::io::SharedWriter;
use crate::parsers::arrow_utils::{build_record_batch, infer_type};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use iceberg::spec::{
    FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
    TableMetadataBuilder, Type,
};

use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

use crate::utils::wrap_py_err;

#[derive(Serialize, Deserialize)]
pub struct SerializableDataFile {
    pub file_path: String,
    pub record_count: u64,
    pub file_size_in_bytes: u64,
    pub schema_json: Option<String>,
}

#[pyclass]
pub struct IcebergWriter {
    table_location: String,
    writer: Mutex<Option<ArrowWriter<SharedWriter>>>,
    schema: Mutex<Option<SchemaRef>>,
    record_count: Mutex<u64>,
    current_file_path: Mutex<Option<String>>,
}

#[pymethods]
impl IcebergWriter {
    #[new]
    pub fn new(table_location: String) -> Self {
        IcebergWriter {
            table_location,
            writer: Mutex::new(None),
            schema: Mutex::new(None),
            record_count: Mutex::new(0),
            current_file_path: Mutex::new(None),
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
            // Infer schema from first batch
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
            let arrow_schema = SchemaRef::new(ArrowSchema::new(fields));

            // Generate a unique file name for this shard
            let file_id = uuid::Uuid::new_v4();
            let sep = if self.table_location.ends_with('/') {
                ""
            } else {
                "/"
            };
            let file_path = format!("{}{sep}data/{}.parquet", self.table_location, file_id);

            // Use get_writer which handles both local (BufWriter<File>) and cloud (RemoteWriter)
            let boxed_writer = crate::io::get_writer(&file_path).map_err(wrap_py_err)?;

            let shared_writer =
                SharedWriter(std::sync::Arc::new(std::sync::Mutex::new(boxed_writer)));

            let props = WriterProperties::builder()
                .set_compression(parquet::basic::Compression::SNAPPY)
                .set_max_row_group_size(8_192)
                .build();

            let writer = ArrowWriter::try_new(shared_writer, arrow_schema.clone(), Some(props))
                .map_err(wrap_py_err)?;

            *writer_guard = Some(writer);
            *schema_guard = Some(arrow_schema);

            let mut pg = self
                .current_file_path
                .lock()
                .map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
            *pg = Some(file_path);
        }

        let writer = writer_guard
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("Writer not initialized"))?;
        let arrow_schema = schema_guard
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Schema not initialized"))?;

        let batch = build_record_batch(py, arrow_schema, list)?;
        writer.write(&batch).map_err(wrap_py_err)?;

        let count = list.len() as u64;
        let mut count_guard = self
            .record_count
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
        *count_guard += count;

        Ok(())
    }

    pub fn close(&self) -> PyResult<String> {
        let mut writer_guard = self
            .writer
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
        let mut path_guard = self
            .current_file_path
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Lock failed"))?;
        let schema_guard = self
            .schema
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Lock failed"))?;

        if let (Some(writer), Some(path)) = (writer_guard.take(), path_guard.take()) {
            writer.close().map_err(wrap_py_err)?;

            // Get file size using StorageController for cloud support
            let size = if crate::io::storage::is_cloud_path(&path) {
                let controller =
                    crate::io::storage::StorageController::new(&path).map_err(wrap_py_err)?;
                crate::io::get_runtime_handle()
                    .block_on(async { controller.get_size().await })
                    .map_err(wrap_py_err)?
            } else {
                std::fs::metadata(&path).map_err(wrap_py_err)?.len()
            };

            let schema_json = if let Some(schema) = schema_guard.as_ref() {
                serde_json::to_string(
                    &schema
                        .fields()
                        .iter()
                        .map(|f: &arrow::datatypes::FieldRef| {
                            (f.name().clone(), f.data_type().to_string())
                        })
                        .collect::<Vec<(String, String)>>(),
                )
                .ok()
            } else {
                None
            };

            let record_count = *self
                .record_count
                .lock()
                .map_err(|_| PyRuntimeError::new_err("Lock failed"))?;

            let meta = SerializableDataFile {
                file_path: path,
                record_count,
                file_size_in_bytes: size,
                schema_json,
            };

            let json = serde_json::to_string(&vec![meta]).map_err(wrap_py_err)?;
            Ok(json)
        } else {
            Ok("[]".to_string())
        }
    }
}

#[pyfunction]
pub fn get_iceberg_data_files(table_location: String) -> PyResult<Vec<String>> {
    use std::fs;
    use std::path::Path;

    if crate::io::storage::is_cloud_path(&table_location) {
        let controller =
            crate::io::storage::StorageController::new(&table_location).map_err(wrap_py_err)?;
        let store = controller.store();

        // Data directory is "data/" relative to the table location prefix
        let prefix = object_store::path::Path::from(controller.path());
        let data_prefix = prefix.child("data");

        let files = crate::io::get_runtime_handle()
            .block_on(async {
                use futures_util::stream::StreamExt;
                let mut files = Vec::new();
                let mut stream = store.list(Some(&data_prefix));

                while let Some(item) = stream.next().await {
                    if let Ok(meta) = item {
                        let path_str = meta.location.to_string();
                        if path_str.ends_with(".parquet") {
                            // Reconstruct full URI
                            let url =
                                url::Url::parse(&table_location).map_err(std::io::Error::other)?;
                            let scheme = url.scheme();
                            let host = url.host_str().unwrap_or("");

                            let full_path = format!("{}://{}/{}", scheme, host, path_str);
                            files.push(full_path);
                        }
                    }
                }
                Ok::<Vec<String>, std::io::Error>(files)
            })
            .map_err(wrap_py_err)?;

        return Ok(files);
    }

    // Local filesystem fallback
    let data_dir = Path::new(&table_location).join("data");
    if !data_dir.exists() {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();
    for entry in fs::read_dir(data_dir).map_err(wrap_py_err)? {
        let entry = entry.map_err(wrap_py_err)?;
        let path = entry.path();
        if path.is_file()
            && path.extension().and_then(|s| s.to_str()) == Some("parquet")
            && let Some(p) = path.to_str()
        {
            files.push(p.to_string());
        }
    }
    Ok(files)
}

#[pyfunction]
pub fn commit_iceberg_transaction(
    table_location: String,
    catalog_properties: std::collections::HashMap<String, String>,
    data_files_json: Vec<String>,
) -> PyResult<()> {
    let mut all_data_files = Vec::new();
    for json in data_files_json {
        let files: Vec<SerializableDataFile> = serde_json::from_str(&json)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to parse data files: {}", e)))?;
        all_data_files.extend(files);
    }

    if all_data_files.is_empty() {
        return Ok(());
    }

    // 1. Infer schema and fields
    let first_file = &all_data_files[0];
    let schema_json = first_file
        .schema_json
        .as_ref()
        .cloned()
        .unwrap_or_else(|| "[]".to_string());

    let schema_tuples: Vec<(String, String)> =
        serde_json::from_str(&schema_json).unwrap_or_default();

    // We will build a TableMetadata using the iceberg crate to ensure correct structure
    let mut fields = Vec::with_capacity(schema_tuples.len());
    for (idx, (name, dt)) in schema_tuples.into_iter().enumerate() {
        let field_type = match dt.to_lowercase().as_str() {
            "int64" | "int32" => Type::Primitive(PrimitiveType::Long),
            "float64" | "float32" => Type::Primitive(PrimitiveType::Double),
            "boolean" => Type::Primitive(PrimitiveType::Boolean),
            _ => Type::Primitive(PrimitiveType::String),
        };
        fields.push(std::sync::Arc::new(NestedField::optional(
            idx as i32 + 1,
            name,
            field_type,
        )));
    }

    let iceberg_schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .build()
        .map_err(wrap_py_err)?;

    let schema_ref = std::sync::Arc::new(iceberg_schema.clone());
    let partition_spec = PartitionSpec::builder(schema_ref)
        .with_spec_id(0)
        .build()
        .map_err(wrap_py_err)?;

    let sort_order = SortOrder::builder()
        .with_order_id(0)
        .build(&iceberg_schema)
        .map_err(wrap_py_err)?;

    let builder = TableMetadataBuilder::new(
        iceberg_schema,
        partition_spec,
        sort_order,
        table_location.clone(),
        FormatVersion::V1,
        catalog_properties,
    )
    .map_err(wrap_py_err)?;

    let build_result = builder.build().map_err(wrap_py_err)?;
    let json_content = serde_json::to_string_pretty(&build_result.metadata).map_err(wrap_py_err)?;
    let version_hint_content = "1";

    let mut retry_count = 0;
    let max_retries = 3;

    loop {
        let res = if crate::io::storage::is_cloud_path(&table_location) {
            // Cloud Path Logic
            let controller =
                crate::io::storage::StorageController::new(&table_location).map_err(wrap_py_err)?;
            let store = controller.store();
            let prefix = object_store::path::Path::from(controller.path());
            let metadata_dir = prefix.child("metadata");
            let metadata_file = metadata_dir.child("v1.metadata.json");
            let version_hint_file = metadata_dir.child("version-hint.text");

            crate::io::get_runtime_handle().block_on(async {
                use object_store::ObjectStoreExt;
                let exists = store.head(&metadata_file).await.is_ok();

                if !exists {
                    let payload = object_store::PutPayload::from(json_content.clone());
                    store
                        .put(&metadata_file, payload)
                        .await
                        .map_err(std::io::Error::other)?;

                    let hint_payload = object_store::PutPayload::from(version_hint_content);
                    store
                        .put(&version_hint_file, hint_payload)
                        .await
                        .map_err(std::io::Error::other)?;
                }
                Ok::<(), std::io::Error>(())
            })
        } else {
            // Local Filesystem Logic
            let metadata_dir = std::path::Path::new(&table_location).join("metadata");
            if !metadata_dir.exists() {
                std::fs::create_dir_all(&metadata_dir)
                    .map_err(wrap_py_err)
                    .map(|_| ())?;
            }

            let metadata_file = metadata_dir.join("v1.metadata.json");
            if !metadata_file.exists() {
                std::fs::write(&metadata_file, &json_content)
                    .map_err(wrap_py_err)
                    .map(|_| ())?;
                let version_hint_file = metadata_dir.join("version-hint.text");
                std::fs::write(&version_hint_file, version_hint_content)
                    .map_err(wrap_py_err)
                    .map(|_| ())?;
            }
            Ok(())
        };

        match res {
            Ok(_) => break,
            Err(e) => {
                if retry_count < max_retries {
                    retry_count += 1;
                    std::thread::sleep(std::time::Duration::from_millis(100 * retry_count));
                    continue;
                } else {
                    return Err(wrap_py_err(e));
                }
            }
        }
    }

    Ok(())
}
