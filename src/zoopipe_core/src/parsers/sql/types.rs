use pyo3::BoundObject;
use pyo3::prelude::*;
use sqlx::TypeInfo;

#[derive(Clone, Debug)]
pub enum SQLValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Null,
}

impl std::fmt::Display for SQLValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SQLValue::String(s) => write!(f, "{}", s),
            SQLValue::Int(v) => write!(f, "{}", v),
            SQLValue::Float(v) => write!(f, "{}", v),
            SQLValue::Bool(v) => write!(f, "{}", v),
            SQLValue::Null => write!(f, "NULL"),
        }
    }
}

impl SQLValue {
    pub fn from_column(row: &sqlx::any::AnyRow, i: usize) -> Self {
        use sqlx::{Column, Row};
        let type_name = row.column(i).type_info().name().to_lowercase();

        // Normalize common type names for basic types across different backends
        match type_name.as_str() {
            "text" | "varchar" | "char" | "string" | "nvarchar" => row
                .try_get::<String, _>(i)
                .map(SQLValue::String)
                .unwrap_or(SQLValue::Null),
            "int" | "integer" | "bigint" | "smallint" | "int4" | "int8" | "mediumint"
            | "tinyint" => row
                .try_get::<i64, _>(i)
                .map(SQLValue::Int)
                .unwrap_or(SQLValue::Null),
            "float" | "double" | "real" | "float4" | "float8" | "numeric" | "decimal" => row
                .try_get::<f64, _>(i)
                .map(SQLValue::Float)
                .unwrap_or(SQLValue::Null),
            "bool" | "boolean" => row
                .try_get::<bool, _>(i)
                .map(SQLValue::Bool)
                .unwrap_or(SQLValue::Null),
            _ => {
                // Fallback: try all supported types if name is unrecognized
                row.try_get::<String, _>(i)
                    .map(SQLValue::String)
                    .or_else(|_| row.try_get::<i64, _>(i).map(SQLValue::Int))
                    .or_else(|_| row.try_get::<f64, _>(i).map(SQLValue::Float))
                    .or_else(|_| row.try_get::<bool, _>(i).map(SQLValue::Bool))
                    .unwrap_or(SQLValue::Null)
            }
        }
    }

    pub fn from_columns(row: &sqlx::any::AnyRow) -> Vec<Self> {
        use sqlx::Row;
        (0..row.columns().len())
            .map(|i| Self::from_column(row, i))
            .collect()
    }

    pub fn from_py(v: Bound<'_, PyAny>) -> PyResult<Self> {
        if v.is_none() {
            Ok(SQLValue::Null)
        } else if let Ok(b) = v.extract::<bool>() {
            Ok(SQLValue::Bool(b))
        } else if let Ok(i) = v.extract::<i64>() {
            Ok(SQLValue::Int(i))
        } else if let Ok(f) = v.extract::<f64>() {
            Ok(SQLValue::Float(f))
        } else {
            Ok(SQLValue::String(v.to_string()))
        }
    }

    pub fn from_py_dict(
        dict: &Bound<'_, pyo3::types::PyDict>,
        fields: &[String],
    ) -> PyResult<Vec<Self>> {
        fields
            .iter()
            .map(|field| {
                let val = dict
                    .get_item(field)?
                    .unwrap_or_else(|| dict.py().None().into_bound(dict.py()));
                Self::from_py(val)
            })
            .collect()
    }

    pub fn to_sql_literal(&self) -> String {
        match self {
            SQLValue::String(s) => format!("'{}'", s.replace('\'', "''")),
            SQLValue::Int(v) => v.to_string(),
            SQLValue::Float(v) => v.to_string(),
            SQLValue::Bool(true) => "TRUE".to_string(),
            SQLValue::Bool(false) => "FALSE".to_string(),
            SQLValue::Null => "NULL".to_string(),
        }
    }
}

impl<'py> IntoPyObject<'py> for SQLValue {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            SQLValue::String(s) => Ok(s.into_pyobject(py)?.into_bound().into_any()),
            SQLValue::Int(v) => Ok(v.into_pyobject(py)?.into_bound().into_any()),
            SQLValue::Float(v) => Ok(v.into_pyobject(py)?.into_bound().into_any()),
            SQLValue::Bool(v) => Ok(v.into_pyobject(py)?.into_bound().into_any()),
            SQLValue::Null => Ok(py.None().into_bound(py)),
        }
    }
}

pub enum SQLData {
    Metadata(Vec<String>),
    Row(Vec<SQLValue>),
    Error(String),
}

impl SQLData {
    pub fn metadata_from_row(row: &sqlx::any::AnyRow) -> Self {
        use sqlx::{Column, Row};
        let cols = row.columns().iter().map(|c| c.name().to_string()).collect();
        SQLData::Metadata(cols)
    }

    pub fn row_from_row(row: &sqlx::any::AnyRow) -> Self {
        SQLData::Row(SQLValue::from_columns(row))
    }
}
