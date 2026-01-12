use pyo3::prelude::*;

pub mod io;
pub mod utils;
pub mod parsers;
pub mod encoding;
pub mod hooks;
pub mod validation;
pub mod engine;

use crate::parsers::{CSVReader, JSONReader, CSVWriter, JSONWriter};
use crate::encoding::{pack_chunk, unpack_chunk};
use crate::validation::NativeValidator;
use crate::engine::{RustPipeEngine, RustParallelEngine};

#[pyfunction]
fn get_version() -> PyResult<String> {
    Ok("0.1.0-native-modular".to_string())
}

#[pymodule]
fn zoopipe_rust_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<CSVReader>()?;
    m.add_class::<JSONReader>()?;
    m.add_class::<CSVWriter>()?;
    m.add_class::<JSONWriter>()?;

    m.add_class::<NativeValidator>()?;
    m.add_class::<RustPipeEngine>()?;
    m.add_class::<RustParallelEngine>()?;
    m.add_function(wrap_pyfunction!(get_version, m)?)?;
    m.add_function(wrap_pyfunction!(pack_chunk, m)?)?;
    m.add_function(wrap_pyfunction!(unpack_chunk, m)?)?;
    
    if m.getattr("RustPipeEngine").is_err() {
        return Err(pyo3::exceptions::PyRuntimeError::new_err("Failed to add RustPipeEngine"));
    }
    
    Ok(())
}
