use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::PyDict;
use sqlx::{AnyPool, query};

use crate::io::get_runtime;
use super::super::backend::SqlBackend;

pub struct GenericInsertBackend {
    uri: String,
    batch_size: usize,
}

impl GenericInsertBackend {
    pub fn new(uri: String, batch_size: usize) -> Self {
        Self { uri, batch_size }
    }
}

impl SqlBackend for GenericInsertBackend {
    fn write_batch(
        &self,
        py: Python<'_>,
        records: &[Py<PyDict>],
        fields: &[String],
        table_name: &str,
    ) -> PyResult<()> {
        let all_values: Vec<String> = records.iter().map(|record_py| {
            let py_inner = record_py.bind(py);
            let row_values = super::super::types::SQLValue::from_py_dict(py_inner, fields)?
                .into_iter()
                .map(|v| v.to_sql_literal())
                .collect::<Vec<String>>();
            Ok(format!("({})", row_values.join(", ")))
        }).collect::<PyResult<Vec<String>>>()?;

        let uri = self.uri.clone();
        let table_name = table_name.to_string();
        let fields = fields.to_vec();
        let batch_size = self.batch_size;

        let rt = get_runtime();
        rt.block_on(async {
            let pool = AnyPool::connect(&uri).await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to connect: {}", e)))?;
            let mut tx = pool.begin().await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to start transaction: {}", e)))?;
            
            for chunk in all_values.chunks(batch_size) {
                let insert_sql = format!(
                    "INSERT INTO {} ({}) VALUES {}",
                    table_name,
                    fields.join(", "),
                    chunk.join(", ")
                );
                query(&insert_sql).execute(&mut *tx).await
                    .map_err(|e| PyRuntimeError::new_err(format!("Batch insert failed: {}", e)))?;
            }

            tx.commit().await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to commit transaction: {}", e)))?;
            Ok::<(), PyErr>(())
        })?;

        Ok(())
    }
}
