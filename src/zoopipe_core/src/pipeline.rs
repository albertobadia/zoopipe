use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyAnyMethods};
use pyo3::exceptions::PyRuntimeError;

use crate::parsers::sql::{SQLReader, SQLWriter};
use crate::parsers::csv::{CSVReader, CSVWriter};
use crate::parsers::json::{JSONReader, JSONWriter};
use crate::parsers::duckdb::{DuckDBReader, DuckDBWriter};
use crate::parsers::arrow::{ArrowReader, ArrowWriter};
use crate::parsers::parquet::{ParquetReader, ParquetWriter};
use crate::parsers::pygen::{PyGeneratorReader, PyGeneratorWriter};
use std::sync::atomic::{AtomicUsize, Ordering};


#[derive(FromPyObject)]
pub enum PipeReader {
    CSV(Py<CSVReader>),
    JSON(Py<JSONReader>),
    DuckDB(Py<DuckDBReader>),
    Arrow(Py<ArrowReader>),
    SQL(Py<SQLReader>),
    Parquet(Py<ParquetReader>),
    PyGen(Py<PyGeneratorReader>),
}

#[derive(FromPyObject)]
pub enum PipeWriter {
    CSV(Py<CSVWriter>),
    JSON(Py<JSONWriter>),
    DuckDB(Py<DuckDBWriter>),
    Arrow(Py<ArrowWriter>),
    SQL(Py<SQLWriter>),
    Parquet(Py<ParquetWriter>),
    PyGen(Py<PyGeneratorWriter>),
}

#[derive(FromPyObject)]
pub enum PipeExecutor {
    SingleThread(Py<crate::executor::SingleThreadExecutor>),
    MultiThread(Py<crate::executor::MultiThreadExecutor>),
}

impl PipeExecutor {
    pub fn process_batches<'py>(
        &self,
        py: Python<'py>,
        batches: Vec<Bound<'py, PyList>>,
        processor: &Bound<'py, PyAny>,
    ) -> PyResult<Vec<Bound<'py, PyAny>>> {
        match self {
            PipeExecutor::SingleThread(e) => e.bind(py).borrow().process_batches(py, batches, processor),
            PipeExecutor::MultiThread(e) => e.bind(py).borrow().process_batches(py, batches, processor),
        }
    }

    pub fn get_batch_size(&self, py: Python<'_>) -> PyResult<usize> {
        match self {
            PipeExecutor::SingleThread(e) => Ok(e.bind(py).borrow().get_batch_size()),
            PipeExecutor::MultiThread(e) => Ok(e.bind(py).borrow().get_batch_size()),
        }
    }
}

pub struct PipeCounters {
    pub total_processed: AtomicUsize,
    pub success_count: AtomicUsize,
    pub error_count: AtomicUsize,
    pub batches_processed: AtomicUsize,
}

impl Default for PipeCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl PipeCounters {
    pub fn new() -> Self {
        Self {
            total_processed: AtomicUsize::new(0),
            success_count: AtomicUsize::new(0),
            error_count: AtomicUsize::new(0),
            batches_processed: AtomicUsize::new(0),
        }
    }
}

#[pyclass]
pub struct NativePipe {
    reader: PipeReader,
    writer: PipeWriter,
    error_writer: Option<PipeWriter>,
    batch_processor: Py<PyAny>,
    report: Py<PyAny>,
    status_failed: Py<PyAny>,
    status_validated: Py<PyAny>,
    counters: PipeCounters,
    report_update_interval: usize,
    executor: PipeExecutor,
}

#[pymethods]
impl NativePipe {
    #[new]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        reader: PipeReader,
        writer: PipeWriter,
        error_writer: Option<PipeWriter>,
        batch_processor: Py<PyAny>,
        report: Py<PyAny>,
        report_update_interval: usize,
        executor: PipeExecutor,
    ) -> PyResult<Self> {
        let models = py.import("zoopipe.report")?;
        let entry_status = models.getattr("EntryStatus")?;
        let status_failed = entry_status.getattr("FAILED")?.unbind();
        let status_validated = entry_status.getattr("VALIDATED")?.unbind();

        Ok(NativePipe {
            reader,
            writer,
            error_writer,
            batch_processor,
            report,
            status_failed,
            status_validated,
            counters: PipeCounters::new(),
            report_update_interval,
            executor,
        })
    }

    fn run(&self, py: Python<'_>) -> PyResult<()> {
        let report = self.report.bind(py);
        report.call_method0("_mark_running")?;

        let batch_size = self.executor.get_batch_size(py)?;
        let mut batch_entries = Vec::with_capacity(batch_size);
        
        loop {
            let next_item = match &self.reader {
                PipeReader::CSV(r) => CSVReader::__next__(r.bind(py).borrow()),
                PipeReader::JSON(r) => JSONReader::__next__(r.bind(py).borrow()),
                PipeReader::DuckDB(r) => DuckDBReader::__next__(r.bind(py).borrow()),
                PipeReader::Arrow(r) => ArrowReader::__next__(r.bind(py).borrow()),
                PipeReader::SQL(r) => SQLReader::__next__(r.bind(py).borrow()),
                PipeReader::Parquet(r) => ParquetReader::__next__(r.bind(py).borrow()),
                PipeReader::PyGen(r) => PyGeneratorReader::__next__(r.bind(py).borrow()),
            }?;

            match next_item {
                Some(entry) => {
                    batch_entries.push(entry);
                    if batch_entries.len() >= batch_size {
                        self.process_batch(py, &mut batch_entries, report)?;
                        batch_entries.clear();
                    }
                }
                None => break,
            }
        }

        if !batch_entries.is_empty() {
            self.process_batch(py, &mut batch_entries, report)?;
        }

        self.sync_report(py, report)?;
        report.call_method0("_mark_completed")?;

        match &self.writer {
            PipeWriter::CSV(w) => w.bind(py).borrow().close()?,
            PipeWriter::JSON(w) => w.bind(py).borrow().close()?,
            PipeWriter::DuckDB(w) => w.bind(py).borrow().close()?,
            PipeWriter::Arrow(w) => w.bind(py).borrow().close()?,
            PipeWriter::SQL(w) => w.bind(py).borrow().close()?,
            PipeWriter::Parquet(w) => w.bind(py).borrow().close()?,
            PipeWriter::PyGen(w) => w.bind(py).borrow().close()?,
        }

        if let Some(ref ew) = self.error_writer {
            match ew {
                PipeWriter::CSV(w) => w.bind(py).borrow().close()?,
                PipeWriter::JSON(w) => w.bind(py).borrow().close()?,
                PipeWriter::DuckDB(w) => w.bind(py).borrow().close()?,
                PipeWriter::Arrow(w) => w.bind(py).borrow().close()?,
                PipeWriter::SQL(w) => w.bind(py).borrow().close()?,
                PipeWriter::Parquet(w) => w.bind(py).borrow().close()?,
                PipeWriter::PyGen(w) => w.bind(py).borrow().close()?,
            }
        }

        Ok(())
    }
}

impl NativePipe {
    fn process_batch(
        &self,
        py: Python<'_>,
        batch: &mut Vec<Bound<'_, PyAny>>,
        report: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let py_list = PyList::new(py, batch.iter())?;
        
        let processed_entries = self.batch_processor.bind(py).call1((py_list,))?;
        let processed_list = processed_entries.cast::<PyList>()?;

        let mut success_data = Vec::with_capacity(processed_list.len());
        let mut error_list = Vec::new();

        let val_key = pyo3::intern!(py, "validated_data");
        let raw_key = pyo3::intern!(py, "raw_data");
        let status_key = pyo3::intern!(py, "status");
        
        let status_failed = self.status_failed.bind(py);
        let status_validated = self.status_validated.bind(py);

        for entry in processed_list.iter() {
            let dict = entry.cast::<PyDict>()?;
            let status = dict.get_item(status_key)?.ok_or_else(|| PyRuntimeError::new_err("Missing status in entry"))?;
            
            if status.eq(status_failed)? {
                error_list.push(dict.get_item(raw_key)?.ok_or_else(|| PyRuntimeError::new_err("Missing raw_data in error entry"))?);
            } else if status.eq(status_validated)? {
                success_data.push(dict.get_item(val_key)?.ok_or_else(|| PyRuntimeError::new_err("Missing validated_data in success entry"))?);
            } else {
                success_data.push(dict.get_item(raw_key)?.ok_or_else(|| PyRuntimeError::new_err("Missing raw_data in entry"))?);
            }
        }

        if !success_data.is_empty() {
            let data_list = PyList::new(py, success_data.iter())?;
            match &self.writer {
                PipeWriter::CSV(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
                PipeWriter::JSON(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
                PipeWriter::DuckDB(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
                PipeWriter::Arrow(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
                PipeWriter::SQL(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
                PipeWriter::Parquet(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
                PipeWriter::PyGen(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
            }
        }

        if !error_list.is_empty() && let Some(ref ew) = self.error_writer {
            let data_list = PyList::new(py, error_list.iter())?;
            match ew {
                PipeWriter::CSV(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
                PipeWriter::JSON(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
                PipeWriter::DuckDB(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
                PipeWriter::Arrow(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
                PipeWriter::SQL(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
                PipeWriter::Parquet(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
                PipeWriter::PyGen(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
            }
        }

        let batch_len = processed_list.len();
        let success_len = success_data.len();
        let error_len = error_list.len();
        
        self.counters.total_processed.fetch_add(batch_len, Ordering::Relaxed);
        self.counters.success_count.fetch_add(success_len, Ordering::Relaxed);
        self.counters.error_count.fetch_add(error_len, Ordering::Relaxed);
        let batches_count = self.counters.batches_processed.fetch_add(1, Ordering::Relaxed) + 1;

        let should_sync = self.report_update_interval > 0 && batches_count.is_multiple_of(self.report_update_interval);

        if should_sync {
            self.sync_report(py, report)?;
        }

        Ok(())
    }

    fn sync_report(&self, _py: Python<'_>, report: &Bound<'_, PyAny>) -> PyResult<()> {
        report.setattr("total_processed", self.counters.total_processed.load(Ordering::Relaxed))?;
        report.setattr("success_count", self.counters.success_count.load(Ordering::Relaxed))?;
        report.setattr("error_count", self.counters.error_count.load(Ordering::Relaxed))?;
        report.setattr("ram_bytes", get_process_ram_rss())?;
        
        Ok(())
    }
}

pub fn get_process_ram_rss() -> usize {
    if let Some(stats) = memory_stats::memory_stats() {
        stats.physical_mem
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipe_counters_new() {
        let counters = PipeCounters::new();
        assert_eq!(counters.total_processed.load(Ordering::Relaxed), 0);
        assert_eq!(counters.success_count.load(Ordering::Relaxed), 0);
        assert_eq!(counters.error_count.load(Ordering::Relaxed), 0);
        assert_eq!(counters.batches_processed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_pipe_counters_increment() {
        let counters = PipeCounters::new();
        
        counters.total_processed.fetch_add(10, Ordering::Relaxed);
        counters.success_count.fetch_add(8, Ordering::Relaxed);
        counters.error_count.fetch_add(2, Ordering::Relaxed);
        
        assert_eq!(counters.total_processed.load(Ordering::Relaxed), 10);
        assert_eq!(counters.success_count.load(Ordering::Relaxed), 8);
        assert_eq!(counters.error_count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_pipe_counters_batches() {
        let counters = PipeCounters::new();
        
        let batch_count = counters.batches_processed.fetch_add(1, Ordering::Relaxed);
        assert_eq!(batch_count, 0);
        assert_eq!(counters.batches_processed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_get_process_ram_rss() {
        let _ram = get_process_ram_rss();
    }

    #[test]
    fn test_pipe_counters_multiple_increments() {
        let counters = PipeCounters::new();
        
        for _ in 0..100 {
            counters.total_processed.fetch_add(1, Ordering::Relaxed);
            counters.success_count.fetch_add(1, Ordering::Relaxed);
        }
        
        assert_eq!(counters.total_processed.load(Ordering::Relaxed), 100);
        assert_eq!(counters.success_count.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_pipe_counters_mixed_operations() {
        let counters = PipeCounters::new();
        
        counters.total_processed.fetch_add(50, Ordering::Relaxed);
        counters.success_count.fetch_add(30, Ordering::Relaxed);
        counters.error_count.fetch_add(20, Ordering::Relaxed);
        
        assert_eq!(counters.total_processed.load(Ordering::Relaxed), 50);
        assert_eq!(counters.success_count.load(Ordering::Relaxed), 30);
        assert_eq!(counters.error_count.load(Ordering::Relaxed), 20);
    }

    #[test]
    fn test_pipe_counters_batch_processing() {
        let counters = PipeCounters::new();
        
        for _ in 0..10 {
            counters.batches_processed.fetch_add(1, Ordering::Relaxed);
        }
        
        assert_eq!(counters.batches_processed.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn test_pipe_counters_zero_operations() {
        let counters = PipeCounters::new();
        
        counters.total_processed.fetch_add(0, Ordering::Relaxed);
        
        assert_eq!(counters.total_processed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_pipe_counters_large_values() {
        let counters = PipeCounters::new();
        
        counters.total_processed.fetch_add(1_000_000, Ordering::Relaxed);
        
        assert_eq!(counters.total_processed.load(Ordering::Relaxed), 1_000_000);
    }
}

