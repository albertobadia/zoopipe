use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyList, PyString};
use sqlx::{AnyPool, query};
use std::sync::Mutex;

use super::backend::SqlBackend;
use super::backends::{GenericInsertBackend, PostgresCopyBackend};
use super::utils::{ensure_parent_dir, init_drivers, is_postgres};
use crate::error::PipeError;
use crate::io::get_runtime;

#[pyclass]
pub struct SQLWriter {
    uri: String,
    table_name: String,
    mode: String,
    table_created: Mutex<bool>,
    fieldnames: Mutex<Option<Vec<Py<PyString>>>>,
    backend: Box<dyn SqlBackend>,
}

#[pymethods]
impl SQLWriter {
    #[new]
    #[pyo3(signature = (uri, table_name, mode="replace", batch_size=500))]
    pub fn new(uri: String, table_name: String, mode: &str, batch_size: usize) -> PyResult<Self> {
        init_drivers();
        match mode {
            "replace" | "append" | "fail" => (),
            _ => {
                return Err(PyRuntimeError::new_err(
                    "mode must be 'replace', 'append', or 'fail'",
                ));
            }
        }

        let backend: Box<dyn SqlBackend> = if is_postgres(&uri) {
            Box::new(PostgresCopyBackend::new(uri.clone()))
        } else {
            Box::new(GenericInsertBackend::new(uri.clone(), batch_size))
        };

        Ok(SQLWriter {
            uri,
            table_name,
            mode: mode.to_string(),
            table_created: Mutex::new(false),
            fieldnames: Mutex::new(None),
            backend,
        })
    }

    pub fn write(&self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        let entries = PyList::new(py, [data])?;
        self.write_batch(py, entries.into_any())
    }

    pub fn write_batch(&self, py: Python<'_>, entries: Bound<'_, PyAny>) -> PyResult<()> {
        let mut table_created = self
            .table_created
            .lock()
            .map_err(|_| PipeError::MutexLock)?;
        let mut fieldnames = self.fieldnames.lock().map_err(|_| PipeError::MutexLock)?;

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

        let mut all_records = first_entry.into_iter().collect::<Vec<_>>();
        for entry_res in it {
            all_records.push(entry_res?.cast::<PyDict>()?.clone().unbind());
        }

        let field_names = fieldnames
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Fieldnames not initialized"))?;
        let fields: Vec<String> = field_names
            .iter()
            .map(|f| Ok(f.bind(py).to_str()?.to_string()))
            .collect::<PyResult<Vec<_>>>()?;

        self.backend
            .write_batch(py, &all_records, &fields, &self.table_name)?;

        Ok(())
    }

    pub fn flush(&self) -> PyResult<()> {
        Ok(())
    }
    pub fn close(&self) -> PyResult<()> {
        self.flush()
    }
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
                a.bind(py)
                    .to_str()
                    .unwrap_or_default()
                    .cmp(b.bind(py).to_str().unwrap_or_default())
            });
            *fieldnames = Some(record_keys);
        }

        let fields = fieldnames.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("Fieldnames should be set by ensure_table_created()")
        })?;
        let uri = self.uri.clone();
        let table_name = self.table_name.clone();
        let mode = self.mode.clone();

        let rt = get_runtime();
        rt.block_on(async {
            ensure_parent_dir(&uri);
            let pool = AnyPool::connect(&uri)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to connect: {}", e)))?;

            if mode == "replace" {
                let _ = query(&format!("DROP TABLE IF EXISTS {}", table_name))
                    .execute(&pool)
                    .await;
            }

            let columns: Vec<String> = fields
                .iter()
                .map(|f| format!("{} TEXT", f.bind(py).to_str().unwrap_or("column")))
                .collect();

            let create_sql = format!(
                "CREATE TABLE IF NOT EXISTS {} ({})",
                table_name,
                columns.join(", ")
            );
            query(&create_sql)
                .execute(&pool)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to create table: {}", e)))?;

            Ok::<(), PyErr>(())
        })?;

        *table_created = true;
        Ok(())
    }
}
