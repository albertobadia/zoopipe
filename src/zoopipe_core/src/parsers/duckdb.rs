use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::sync::Mutex;
use pyo3::types::{PyAnyMethods, PyString, PyDict, PyList};
use duckdb::Connection;
use duckdb::types::Value;
use crate::error::PipeError;

enum DuckDBData {
    Metadata(Vec<String>),
    Row(Vec<Value>),
}

#[pyclass]
pub struct DuckDBReader {
    db_path: String,
    query: String,
    receiver: Mutex<Option<crossbeam_channel::Receiver<DuckDBData>>>,
    column_names: Mutex<Option<Vec<Py<PyString>>>>,
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
        let models = py.import("zoopipe.report")?;
        let status_enum = models.getattr("EntryStatus")?;
        let status_pending = status_enum.getattr("PENDING")?.into();

        Ok(DuckDBReader {
            db_path,
            query,
            receiver: Mutex::new(None),
            column_names: Mutex::new(None),
            position: Mutex::new(0),
            status_pending,
            generate_ids,
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __next__(slf: PyRef<'_, Self>) -> PyResult<Option<Bound<'_, PyAny>>> {
        let mut receiver_opt = slf.receiver.lock().map_err(|_| PipeError::MutexLock)?;
        
        if receiver_opt.is_none() {
            let (tx, rx) = crossbeam_channel::bounded(1000);
            let db_path = slf.db_path.clone();
            let query = slf.query.clone();
            
            std::thread::spawn(move || {
                let conn = match Connection::open(&db_path) {
                    Ok(c) => c,
                    Err(_) => return,
                };

                let meta_query = format!("SELECT * FROM ({}) LIMIT 0", query);
                let col_count = {
                    let meta_conn = match Connection::open(&db_path) {
                        Ok(c) => c,
                        Err(_) => return,
                    };
                    let mut meta_stmt = match meta_conn.prepare(&meta_query) {
                        Ok(s) => s,
                        Err(_) => return,
                    };
                    let _ = meta_stmt.query([]);
                    let cols: Vec<String> = meta_stmt.column_names().iter().map(|s| s.to_string()).collect();
                    let count = cols.len();
                    if tx.send(DuckDBData::Metadata(cols)).is_err() {
                        return;
                    }
                    count
                };

                let mut stmt = match conn.prepare(&query) {
                    Ok(s) => s,
                    Err(_) => return,
                };
                
                let mut rows = match stmt.query([]) {
                    Ok(r) => r,
                    Err(_) => return,
                };

                while let Ok(Some(row)) = rows.next() {
                    let mut record = Vec::with_capacity(col_count);
                    for i in 0..col_count {
                        record.push(row.get::<usize, Value>(i).unwrap_or(Value::Null));
                    }
                    if tx.send(DuckDBData::Row(record)).is_err() {
                        break;
                    }
                }
            });

            *receiver_opt = Some(rx);
        }

        let rx = receiver_opt.as_ref()
            .expect("Receiver should be initialized before reading rows");
        
        loop {
            match rx.recv() {
                Ok(DuckDBData::Metadata(cols)) => {
                    let py = slf.py();
                    let py_cols: Vec<Py<PyString>> = cols.into_iter()
                        .map(|s| PyString::new(py, &s).unbind())
                        .collect();
                    let mut column_names_opt = slf.column_names.lock().map_err(|_| PipeError::MutexLock)?;
                    *column_names_opt = Some(py_cols);
                }
                Ok(DuckDBData::Row(row_values)) => {
                    let py = slf.py();
                    let column_names_opt = slf.column_names.lock().map_err(|_| PipeError::MutexLock)?;
                    let cols = column_names_opt.as_ref().expect("Column names should be received before rows");
                    
                    let mut pos = slf.position.lock().map_err(|_| PipeError::MutexLock)?;
                    let current_pos = *pos;
                    *pos += 1;
                    
                    let raw_data = PyDict::new(py);
                    for (i, value) in row_values.into_iter().enumerate() {
                        let col_name = cols[i].bind(py);
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

                    return Ok(Some(envelope.into_any()));
                }
                Err(_) => return Ok(None),
            }
        }
    }
}

#[pyclass]
pub struct DuckDBWriter {
    connection: Mutex<Connection>,
    table_name: String,
    mode: String,
    table_created: Mutex<bool>,
    fieldnames: Mutex<Option<Vec<Py<PyString>>>>,
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
        let entries = PyList::new(py, [data])?;
        self.write_batch(py, entries.into_any())
    }

    pub fn write_batch(&self, py: Python<'_>, entries: Bound<'_, PyAny>) -> PyResult<()> {
        let mut connection = self.connection.lock()
            .map_err(|_| PipeError::MutexLock)?;
        let mut table_created = self.table_created.lock()
            .map_err(|_| PipeError::MutexLock)?;
        let mut fieldnames = self.fieldnames.lock()
            .map_err(|_| PipeError::MutexLock)?;

        let mut it = entries.try_iter()?;
        
        let py_bool = py.get_type::<pyo3::types::PyBool>();
        let py_int = py.get_type::<pyo3::types::PyInt>();
        let py_float = py.get_type::<pyo3::types::PyFloat>();

        let first_entry = if let Some(res) = it.next() {
            let entry = res?;
            let record = entry.cast::<PyDict>()?;
            if !*table_created {
                self.ensure_table_created(py, record, &mut connection, &mut table_created, &mut fieldnames)?;
            }
            Some(record.clone())
        } else {
            return Ok(());
        };

        let mut appender = connection.appender(&self.table_name)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create appender: {}", e)))?;

        let fields = fieldnames.as_ref()
            .expect("Fieldnames should be initialized by first record");

        fn append_dict(record: &Bound<'_, PyDict>, fields: &[Py<PyString>], app: &mut duckdb::Appender, py_bool: &Bound<'_, pyo3::types::PyType>, py_int: &Bound<'_, pyo3::types::PyType>, py_float: &Bound<'_, pyo3::types::PyType>, py: Python<'_>) -> PyResult<()> {
            let mut row_values: Vec<Value> = Vec::with_capacity(fields.len());
            for field_py in fields {
                let field = field_py.bind(py);
                if let Some(value) = record.get_item(field)? {
                    if value.is_instance(py_bool)? {
                        row_values.push(Value::Boolean(value.extract::<bool>()?));
                    } else if value.is_instance(py_int)? {
                        row_values.push(Value::BigInt(value.extract::<i64>()?));
                    } else if value.is_instance(py_float)? {
                        row_values.push(Value::Double(value.extract::<f64>()?));
                    } else if let Ok(s) = value.cast::<PyString>() {
                        row_values.push(Value::Text(s.to_str()?.to_string()));
                    } else {
                        row_values.push(Value::Text(value.to_string()));
                    }
                } else {
                    row_values.push(Value::Null);
                }
            }
            let params_refs: Vec<&dyn duckdb::ToSql> = row_values.iter().map(|p| p as &dyn duckdb::ToSql).collect();
            app.append_row(params_refs.as_slice())
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to append row: {}", e)))?;
            Ok(())
        }

        if let Some(record) = first_entry {
            append_dict(&record, fields, &mut appender, &py_bool, &py_int, &py_float, py)?;
        }

        for entry_res in it {
            let entry = entry_res?;
            let record = entry.cast::<PyDict>()?;
            append_dict(record, fields, &mut appender, &py_bool, &py_int, &py_float, py)?;
        }

        appender.flush()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to flush appender: {}", e)))?;
        Ok(())
    }

    pub fn flush(&self) -> PyResult<()> { Ok(()) }
    pub fn close(&self) -> PyResult<()> { self.flush() }
}

impl DuckDBWriter {
    fn ensure_table_created(
        &self,
        _py: Python<'_>,
        record: &Bound<'_, PyDict>,
        connection: &mut Connection,
        table_created: &mut bool,
        fieldnames: &mut Option<Vec<Py<PyString>>>,
    ) -> PyResult<()> {
        if fieldnames.is_none() {
            let py = record.py();
            let keys_list = record.keys();
            let mut record_keys: Vec<Py<PyString>> = Vec::new();
            for k in keys_list.iter() {
                record_keys.push(k.cast::<PyString>()?.clone().unbind());
            }
            record_keys.sort_by(|a, b| {
                a.bind(py).to_str().unwrap_or("")
                    .cmp(b.bind(py).to_str().unwrap_or(""))
            });
            *fieldnames = Some(record_keys);
        }

        let fields = fieldnames.as_ref()
            .expect("Fieldnames should be set before table creation");
        
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

        let columns = fields.iter().map(|f_py| {
            let f = f_py.bind(_py);
            let duckdb_type = if let Ok(Some(value)) = record.get_item(f) {
                if value.is_instance(&py_bool).unwrap_or(false) { "BOOLEAN" }
                else if value.is_instance(&py_int).unwrap_or(false) { "BIGINT" }
                else if value.is_instance(&py_float).unwrap_or(false) { "DOUBLE" }
                else { "VARCHAR" }
            } else { "VARCHAR" };
            format!("{} {}", f.to_str().unwrap_or(""), duckdb_type)
        }).collect::<Vec<_>>().join(", ");
        
        let create_sql = format!("CREATE TABLE IF NOT EXISTS {} ({})", self.table_name, columns);
        connection.execute(&create_sql, [])
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create table: {}", e)))?;

        *table_created = true;
        Ok(())
    }

}
