use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::PyDict;
use futures_util::SinkExt;

use crate::io::get_runtime;
use super::super::backend::SqlBackend;

pub struct PostgresCopyBackend {
    uri: String,
}

impl PostgresCopyBackend {
    pub fn new(uri: String) -> Self {
        Self { uri }
    }
}

impl SqlBackend for PostgresCopyBackend {
    fn write_batch(
        &self,
        py: Python<'_>,
        records: &[Py<PyDict>],
        fields: &[String],
        table_name: &str,
    ) -> PyResult<()> {
        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(Vec::new());

        for record_py in records {
            let py_inner = record_py.bind(py);
            let row = super::super::types::SQLValue::from_py_dict(py_inner, fields)?
                .into_iter()
                .map(|v| match v {
                    super::super::types::SQLValue::Bool(true) => "t".to_string(),
                    super::super::types::SQLValue::Bool(false) => "f".to_string(),
                    super::super::types::SQLValue::Null => String::new(),
                    super::super::types::SQLValue::String(s) => s,
                    _ => v.to_string(),
                })
                .collect::<Vec<String>>();
                
            wtr.write_record(&row).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        }

        let csv_data = wtr.into_inner().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let uri = self.uri.clone();
        let copy_query = format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL '')",
            table_name,
            fields.join(", ")
        );

        get_runtime().block_on(async {
            let (client, connection) = tokio_postgres::connect(&uri, tokio_postgres::NoTls).await
                .map_err(|e| PyRuntimeError::new_err(format!("PostgreSQL connection failed: {}", e)))?;

            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("PostgreSQL connection error: {}", e);
                }
            });

            let sink = client.copy_in(&copy_query).await
                .map_err(|e| PyRuntimeError::new_err(format!("COPY init failed: {}", e)))?;

            let mut writer = std::pin::pin!(sink);
            writer.send(bytes::Bytes::from(csv_data)).await
                .map_err(|e| PyRuntimeError::new_err(format!("COPY send failed: {}", e)))?;
            
            writer.close().await
                .map_err(|e| PyRuntimeError::new_err(format!("COPY close failed: {}", e)))?;

            Ok(())
        })
    }
}
