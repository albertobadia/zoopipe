use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PipeError {
    #[error("CSV parsing error: {0}")]
    CsvParse(#[from] csv::Error),
    
    #[error("JSON parsing error: {0}")]
    JsonParse(#[from] serde_json::Error),
    
    #[error("SQL error: {0}")]
    Sql(#[from] sqlx::Error),
    
    #[error("DuckDB error: {0}")]
    DuckDb(#[from] duckdb::Error),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Field conversion error: {field}")]
    FieldConversion { field: String },
    
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
    
    #[error("Mutex lock failed")]
    MutexLock,
}

impl From<PipeError> for PyErr {
    fn from(err: PipeError) -> PyErr {
        PyRuntimeError::new_err(err.to_string())
    }
}

pub type PipeResult<T> = Result<T, PipeError>;
