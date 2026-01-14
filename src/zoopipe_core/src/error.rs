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
    
    #[error("Error: {0}")]
    Other(String),
}

impl From<PipeError> for PyErr {
    fn from(err: PipeError) -> PyErr {
        PyRuntimeError::new_err(err.to_string())
    }
}

pub type PipeResult<T> = Result<T, PipeError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_conversion_error() {
        let error = PipeError::FieldConversion {
            field: "user_id".to_string(),
        };
        assert_eq!(error.to_string(), "Field conversion error: user_id");
    }

    #[test]
    fn test_invalid_config_error() {
        let error = PipeError::InvalidConfig("missing batch_size".to_string());
        assert_eq!(error.to_string(), "Invalid configuration: missing batch_size");
    }

    #[test]
    fn test_mutex_lock_error() {
        let error = PipeError::MutexLock;
        assert_eq!(error.to_string(), "Mutex lock failed");
    }

    #[test]
    fn test_other_error() {
        let error = PipeError::Other("unexpected error".to_string());
        assert_eq!(error.to_string(), "Error: unexpected error");
    }

    #[test]
    fn test_io_error_conversion() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let pipe_error = PipeError::from(io_error);
        assert!(pipe_error.to_string().contains("file not found"));
    }

    #[test]
    fn test_json_error_conversion() {
        let json_str = "{invalid json}";
        let json_error = serde_json::from_str::<serde_json::Value>(json_str).unwrap_err();
        let pipe_error = PipeError::from(json_error);
        assert!(pipe_error.to_string().contains("JSON parsing error"));
    }

    #[test]
    fn test_field_conversion_error_empty_field() {
        let error = PipeError::FieldConversion {
            field: String::new(),
        };
        assert_eq!(error.to_string(), "Field conversion error: ");
    }

    #[test]
    fn test_field_conversion_error_long_field_name() {
        let long_name = "a".repeat(100);
        let error = PipeError::FieldConversion {
            field: long_name.clone(),
        };
        assert!(error.to_string().contains(&long_name));
    }

    #[test]
    fn test_invalid_config_error_with_special_chars() {
        let config = "missing: 'value' with \"quotes\"";
        let error = PipeError::InvalidConfig(config.to_string());
        assert!(error.to_string().contains(config));
    }

    #[test]
    fn test_other_error_empty_message() {
        let error = PipeError::Other(String::new());
        assert_eq!(error.to_string(), "Error: ");
    }

    #[test]
    fn test_io_error_different_kinds() {
        let errors = vec![
            std::io::Error::new(std::io::ErrorKind::NotFound, "not found"),
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied"),
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused"),
        ];

        for io_error in errors {
            let pipe_error = PipeError::from(io_error);
            assert!(pipe_error.to_string().contains("IO error"));
        }
    }
}

