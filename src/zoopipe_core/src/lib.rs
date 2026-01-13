use pyo3::prelude::*;

pub mod error;
pub mod io;
pub mod utils;
pub mod parsers;
pub mod pipeline;
pub mod executor;

use crate::parsers::sql::{SQLReader, SQLWriter};
use crate::parsers::pygen::{PyGeneratorReader, PyGeneratorWriter};
use crate::parsers::csv::{CSVReader, CSVWriter};
use crate::parsers::json::{JSONReader, JSONWriter};
use crate::parsers::duckdb::{DuckDBReader, DuckDBWriter};
use crate::parsers::arrow::{ArrowReader, ArrowWriter};
use crate::parsers::parquet::{ParquetReader, ParquetWriter};
use crate::pipeline::NativePipe;
use crate::executor::{SingleThreadExecutor, MultiThreadExecutor};

#[pyfunction]
fn get_version() -> PyResult<String> {
    Ok("2026.1.12".to_string())
}

#[pymodule]
fn zoopipe_rust_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<CSVReader>()?;
    m.add_class::<JSONReader>()?;
    m.add_class::<DuckDBReader>()?;
    m.add_class::<ArrowReader>()?;
    m.add_class::<SQLReader>()?;
    m.add_class::<CSVWriter>()?;
    m.add_class::<JSONWriter>()?;
    m.add_class::<DuckDBWriter>()?;
    m.add_class::<ArrowWriter>()?;
    m.add_class::<SQLWriter>()?;
    m.add_class::<ParquetReader>()?;
    m.add_class::<ParquetWriter>()?;
    m.add_class::<PyGeneratorReader>()?;
    m.add_class::<PyGeneratorWriter>()?;


    m.add_class::<NativePipe>()?;
    
    m.add_class::<SingleThreadExecutor>()?;
    m.add_class::<MultiThreadExecutor>()?;
    
    m.add_function(wrap_pyfunction!(get_version, m)?)?;
    
    Ok(())
}
