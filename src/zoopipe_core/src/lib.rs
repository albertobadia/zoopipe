use pyo3::prelude::*;

pub mod io;
pub mod utils;
pub mod parsers;
pub mod pipeline;
pub mod executor;

use crate::parsers::csv::{CSVReader, CSVWriter};
use crate::parsers::json::{JSONReader, JSONWriter};
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
    m.add_class::<CSVWriter>()?;
    m.add_class::<JSONWriter>()?;

    m.add_class::<NativePipe>()?;
    
    m.add_class::<SingleThreadExecutor>()?;
    m.add_class::<MultiThreadExecutor>()?;
    
    m.add_function(wrap_pyfunction!(get_version, m)?)?;
    
    Ok(())
}
