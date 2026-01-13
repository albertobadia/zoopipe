use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::sync::Mutex;
use pyo3::types::{PyAnyMethods, PyString, PyDict, PyList};
use duckdb::Connection;

use duckdb::types::Value;

type DuckDBRow = Vec<(String, Value)>;

#[pyclass]
pub struct DuckDBReader {
    connection: Mutex<Connection>,
    query: String,
    rows: Mutex<Option<std::collections::VecDeque<DuckDBRow>>>,
    position: Mutex<usize>,
    status_pending: Py<PyAny>,
    generate_ids: bool,
}

#[pymethods]
impl DuckDBReader {
    #[new]
    #[pyo3(signature = (db_path, query, generate_ids=true))]
    fn new(
        py: Python<'_>,
        db_path: String,
        query: String,
        generate_ids: bool,
    ) -> PyResult<Self> {
        let connection = Connection::open(&db_path)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to open DuckDB database: {}", e)))?;

        let models = py.import("zoopipe.report")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(DuckDBReader {
            connection: Mutex::new(connection),
            query,
            rows: Mutex::new(None),
            position: Mutex::new(0),
            status_pending,
            generate_ids,
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __next__(slf: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyAny>>> {
        let mut rows = slf.rows.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut pos = slf.position.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;

        if rows.is_none() {
            let connection = slf.connection.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
            
            let mut stmt = connection.prepare(&slf.query)
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to prepare query: {}", e)))?;
            
            let mut query_rows = stmt.query([])
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to execute query: {}", e)))?;

            let column_names: Vec<String> = query_rows.as_ref()
                .ok_or_else(|| PyRuntimeError::new_err("Failed to get column metadata"))?
                .column_names();

            let mut collected_rows: std::collections::VecDeque<DuckDBRow> = std::collections::VecDeque::new();
            while let Some(row) = query_rows.next().map_err(|e| PyRuntimeError::new_err(format!("Failed to fetch row: {}", e)))? {
                let mut record: DuckDBRow = Vec::new();
                for (i, name) in column_names.iter().enumerate() {
                    let value: Value = row.get(i)
                        .map_err(|e| PyRuntimeError::new_err(format!("Failed to get value for column {}: {}", name, e)))?;
                    record.push((name.clone(), value));
                }
                collected_rows.push_back(record);
            }

            *rows = Some(collected_rows);
        }

        if let Some(all_rows) = rows.as_mut() {
            if let Some(row_data) = all_rows.pop_front() {
                let py = slf.py();
                let current_pos = *pos;
                *pos += 1;
                let raw_data = PyDict::new(py);
                for (col_name, value) in row_data.into_iter() {
                    match value {
                        Value::Null => raw_data.set_item(col_name, py.None())?,
                        Value::Boolean(b) => raw_data.set_item(col_name, b)?,
                        Value::TinyInt(i) => raw_data.set_item(col_name, i)?,
                        Value::SmallInt(i) => raw_data.set_item(col_name, i)?,
                        Value::Int(i) => raw_data.set_item(col_name, i)?,
                        Value::BigInt(i) => raw_data.set_item(col_name, i)?,
                        Value::UTinyInt(i) => raw_data.set_item(col_name, i)?,
                        Value::USmallInt(i) => raw_data.set_item(col_name, i)?,
                        Value::UInt(i) => raw_data.set_item(col_name, i)?,
                        Value::UBigInt(i) => raw_data.set_item(col_name, i)?,
                        Value::Float(f) => raw_data.set_item(col_name, f)?,
                        Value::Double(f) => raw_data.set_item(col_name, f)?,
                        Value::Text(s) => raw_data.set_item(col_name, s)?,
                        Value::Timestamp(_, i) => raw_data.set_item(col_name, i)?,
                        Value::Date32(i) => raw_data.set_item(col_name, i)?,
                        Value::Time64(_, i) => raw_data.set_item(col_name, i)?,
                        Value::Blob(b) => raw_data.set_item(col_name, b)?,
                        _ => raw_data.set_item(col_name, py.None())?,
                    };
                }

                let envelope = PyDict::new(py);
                
                let id = if slf.generate_ids {
                    uuid::Uuid::new_v4().to_string()
                } else {
                    String::new()
                };

                envelope.set_item(pyo3::intern!(py, "id"), id)?;
                envelope.set_item(pyo3::intern!(py, "status"), slf.status_pending.bind(py))?;
                envelope.set_item(pyo3::intern!(py, "raw_data"), raw_data)?;
                envelope.set_item(pyo3::intern!(py, "metadata"), PyDict::new(py))?;
                envelope.set_item(pyo3::intern!(py, "position"), current_pos)?;
                envelope.set_item(pyo3::intern!(py, "errors"), PyList::empty(py))?;

                Ok(Some(envelope.into_any()))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

#[pyclass]
pub struct DuckDBWriter {
    connection: Mutex<Connection>,
    table_name: String,
    mode: String,
    table_created: Mutex<bool>,
    fieldnames: Mutex<Option<Vec<String>>>,
}

#[pymethods]
impl DuckDBWriter {
    #[new]
    #[pyo3(signature = (db_path, table_name, mode="replace"))]
    pub fn new(
        db_path: String,
        table_name: String,
        mode: &str,
    ) -> PyResult<Self> {
        let connection = Connection::open(&db_path)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to open DuckDB database: {}", e)))?;

        if mode != "replace" && mode != "append" && mode != "fail" {
            return Err(PyRuntimeError::new_err(
                "mode must be 'replace', 'append', or 'fail'"
            ));
        }

        Ok(DuckDBWriter {
            connection: Mutex::new(connection),
            table_name,
            mode: mode.to_string(),
            table_created: Mutex::new(false),
            fieldnames: Mutex::new(None),
        })
    }

    pub fn write(&self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        let mut connection = self.connection.lock()
            .map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut table_created = self.table_created.lock()
            .map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut fieldnames = self.fieldnames.lock()
            .map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;

        self.write_internal(py, data, &mut connection, &mut table_created, &mut fieldnames)
    }

    pub fn write_batch(&self, py: Python<'_>, entries: Bound<'_, PyAny>) -> PyResult<()> {
        let mut connection = self.connection.lock()
            .map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut table_created = self.table_created.lock()
            .map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        let mut fieldnames = self.fieldnames.lock()
            .map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;

        let iterator = entries.try_iter()?;
        let mut batch_data = Vec::new();

        for entry in iterator {
            let entry = entry?;
            let record = entry.cast::<PyDict>()?;
            
            // Ensure table is created using the first record if not already done
            if !*table_created {
                self.ensure_table_created(py, record, &mut connection, &mut table_created, &mut fieldnames)?;
            }
            
            batch_data.push(record.clone().unbind());
        }

        if batch_data.is_empty() {
            return Ok(());
        }

        // Use Appender for high-speed bulk loading
        let mut appender = connection.appender(&self.table_name)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create appender: {}", e)))?;

        let fields = fieldnames.as_ref().unwrap();
        let py_bool = py.get_type::<pyo3::types::PyBool>();
        let py_int = py.get_type::<pyo3::types::PyInt>();
        let py_float = py.get_type::<pyo3::types::PyFloat>();

        for record_ref in batch_data {
            let record = record_ref.bind(py);
            let mut row_params: Vec<Box<dyn duckdb::ToSql>> = Vec::with_capacity(fields.len());
            
            for field in fields {
                if let Some(value) = record.get_item(field)? {
                    if value.is_instance(&py_bool)? {
                        row_params.push(Box::new(value.extract::<bool>()?));
                    } else if value.is_instance(&py_int)? {
                        row_params.push(Box::new(value.extract::<i64>()?));
                    } else if value.is_instance(&py_float)? {
                        row_params.push(Box::new(value.extract::<f64>()?));
                    } else if let Ok(s) = value.cast::<PyString>() {
                        row_params.push(Box::new(s.to_str()?.to_string()));
                    } else {
                        row_params.push(Box::new(value.to_string()));
                    }
                } else {
                    row_params.push(Box::new(Option::<String>::None));
                }
            }

            let params_refs: Vec<&dyn duckdb::ToSql> = row_params
                .iter()
                .map(|p| p.as_ref())
                .collect();

            appender.append_row(params_refs.as_slice())
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to append row: {}", e)))?;
        }

        appender.flush()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to flush appender: {}", e)))?;

        Ok(())
    }

    pub fn flush(&self) -> PyResult<()> {
        Ok(())
    }

    pub fn close(&self) -> PyResult<()> {
        self.flush()
    }
}

impl DuckDBWriter {
    fn ensure_table_created(
        &self,
        _py: Python<'_>,
        record: &Bound<'_, PyDict>,
        connection: &mut Connection,
        table_created: &mut bool,
        fieldnames: &mut Option<Vec<String>>,
    ) -> PyResult<()> {
        if fieldnames.is_none() {
            let keys_list = record.keys();
            let mut record_keys: Vec<String> = Vec::new();
            for k in keys_list.iter() {
                let key_str = k.cast::<PyString>()?.to_str()?.to_string();
                record_keys.push(key_str);
            }
            record_keys.sort();
            *fieldnames = Some(record_keys);
        }

        let fields = fieldnames.as_ref().unwrap();
        
        if self.mode == "replace" {
            connection.execute(&format!("DROP TABLE IF EXISTS {}", self.table_name), [])
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to drop table: {}", e)))?;
        } else if self.mode == "fail" {
            let table_exists: bool = connection
                .query_row(
                    "SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_name = ?",
                    [&self.table_name],
                    |row| row.get(0),
                )
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to check if table exists: {}", e)))?;
            
            if table_exists {
                return Err(PyRuntimeError::new_err(format!("Table {} already exists", self.table_name)));
            }
        }

        let py_bool = _py.get_type::<pyo3::types::PyBool>();
        let py_int = _py.get_type::<pyo3::types::PyInt>();
        let py_float = _py.get_type::<pyo3::types::PyFloat>();

        let columns = fields
            .iter()
            .map(|f| {
                let duckdb_type = if let Ok(Some(value)) = record.get_item(f) {
                    if value.is_instance(&py_bool).unwrap_or(false) {
                        "BOOLEAN"
                    } else if value.is_instance(&py_int).unwrap_or(false) {
                        "BIGINT"
                    } else if value.is_instance(&py_float).unwrap_or(false) {
                        "DOUBLE"
                    } else {
                        "VARCHAR"
                    }
                } else {
                    "VARCHAR"
                };
                format!("{} {}", f, duckdb_type)
            })
            .collect::<Vec<_>>()
            .join(", ");
        
        let create_sql = format!("CREATE TABLE IF NOT EXISTS {} ({})", self.table_name, columns);
        connection.execute(&create_sql, [])
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create table: {}", e)))?;

        *table_created = true;
        Ok(())
    }

    fn write_internal(
        &self,
        py: Python<'_>,
        data: Bound<'_, PyAny>,
        connection: &mut Connection,
        table_created: &mut bool,
        fieldnames: &mut Option<Vec<String>>,
    ) -> PyResult<()> {
        let record = data.cast::<PyDict>()?;

        if !*table_created {
            self.ensure_table_created(py, record, connection, table_created, fieldnames)?;
        }

        if let Some(fields) = fieldnames.as_ref() {
            let placeholders = (0..fields.len())
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", ");
            
            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                self.table_name,
                fields.join(", "),
                placeholders
            );

            let py_bool = py.get_type::<pyo3::types::PyBool>();
            let py_int = py.get_type::<pyo3::types::PyInt>();
            let py_float = py.get_type::<pyo3::types::PyFloat>();

            let mut params: Vec<Box<dyn duckdb::ToSql>> = Vec::new();
            for field in fields {
                if let Some(value) = record.get_item(field)? {
                    if value.is_instance(&py_bool)? {
                        params.push(Box::new(value.extract::<bool>()?));
                    } else if value.is_instance(&py_int)? {
                        params.push(Box::new(value.extract::<i64>()?));
                    } else if value.is_instance(&py_float)? {
                        params.push(Box::new(value.extract::<f64>()?));
                    } else if let Ok(s) = value.cast::<PyString>() {
                        params.push(Box::new(s.to_str()?.to_string()));
                    } else {
                        params.push(Box::new(value.to_string()));
                    }
                } else {
                    params.push(Box::new(Option::<String>::None));
                }
            }

            let params_refs: Vec<&dyn duckdb::ToSql> = params
                .iter()
                .map(|p| p.as_ref())
                .collect();

            connection.execute(&insert_sql, params_refs.as_slice())
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to insert row: {}", e)))?;
        }

        Ok(())
    }
}
