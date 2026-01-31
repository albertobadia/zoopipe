#![allow(clippy::too_many_arguments)]

use pyo3::prelude::*;

pub mod error;
pub mod executor;
pub mod io;
pub mod parsers;
pub mod pipeline;
pub mod utils;

use crate::executor::{MultiThreadExecutor, SingleThreadExecutor};
use crate::io::get_runtime;
use crate::io::storage::StorageController;
use crate::parsers::arrow::{ArrowReader, ArrowWriter};
use crate::parsers::csv::{CSVReader, CSVWriter};
use crate::parsers::excel::{ExcelReader, ExcelWriter};
use crate::parsers::iceberg::{IcebergWriter, commit_iceberg_transaction, get_iceberg_data_files};
use crate::parsers::json::{JSONReader, JSONWriter};
use crate::parsers::kafka::{KafkaReader, KafkaWriter};
use crate::parsers::parquet::{MultiParquetReader, ParquetReader, ParquetWriter};
use crate::parsers::pygen::{PyGeneratorReader, PyGeneratorWriter};
use crate::parsers::sql::{SQLReader, SQLWriter};
use crate::pipeline::NativePipe;
use crate::utils::wrap_py_err;

#[pyfunction]
fn get_version() -> PyResult<String> {
    Ok(env!("CARGO_PKG_VERSION").to_string())
}

pub fn get_file_size_rust(path: String) -> PyResult<u64> {
    let controller = StorageController::new(&path).map_err(wrap_py_err)?;
    get_runtime().block_on(async { controller.get_size().await.map_err(wrap_py_err) })
}

#[pyfunction]
fn get_file_size(path: String) -> PyResult<u64> {
    get_file_size_rust(path)
}

#[pymodule]
fn zoopipe_rust_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<CSVReader>()?;
    m.add_class::<JSONReader>()?;
    m.add_class::<ArrowReader>()?;
    m.add_class::<SQLReader>()?;
    m.add_class::<ExcelReader>()?;
    m.add_class::<CSVWriter>()?;
    m.add_class::<JSONWriter>()?;
    m.add_class::<ArrowWriter>()?;
    m.add_class::<SQLWriter>()?;
    m.add_class::<ExcelWriter>()?;
    m.add_class::<ParquetReader>()?;
    m.add_class::<ParquetWriter>()?;
    m.add_class::<MultiParquetReader>()?;
    m.add_class::<PyGeneratorReader>()?;
    m.add_class::<PyGeneratorWriter>()?;
    m.add_class::<KafkaReader>()?;
    m.add_class::<KafkaWriter>()?;

    m.add_class::<NativePipe>()?;

    m.add_class::<SingleThreadExecutor>()?;
    m.add_class::<MultiThreadExecutor>()?;

    m.add_function(wrap_pyfunction!(get_version, m)?)?;
    m.add_function(wrap_pyfunction!(get_file_size, m)?)?;
    m.add_class::<IcebergWriter>()?;
    m.add_function(wrap_pyfunction!(commit_iceberg_transaction, m)?)?;
    m.add_function(wrap_pyfunction!(get_iceberg_data_files, m)?)?;

    m.add_class::<crate::parsers::delta::DeltaReader>()?;
    m.add_class::<crate::parsers::delta::DeltaWriter>()?;
    m.add_function(wrap_pyfunction!(
        crate::parsers::delta::commit_delta_transaction,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(crate::parsers::delta::get_delta_files, m)?)?;

    Ok(())
}
