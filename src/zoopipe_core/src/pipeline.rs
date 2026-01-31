use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyList};

use crate::parsers::arrow::{ArrowReader, ArrowWriter};
use crate::parsers::csv::{CSVReader, CSVWriter};
use crate::parsers::delta::{DeltaReader, DeltaWriter};
use crate::parsers::excel::{ExcelReader, ExcelWriter};
use crate::parsers::json::{JSONReader, JSONWriter};
use crate::parsers::kafka::{KafkaReader, KafkaWriter};
use crate::parsers::parquet::{MultiParquetReader, ParquetReader, ParquetWriter};
use crate::parsers::pygen::{PyGeneratorReader, PyGeneratorWriter};
use crate::parsers::sql::{SQLReader, SQLWriter};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(FromPyObject)]
pub enum PipeReader {
    CSV(Py<CSVReader>),
    JSON(Py<JSONReader>),
    Arrow(Py<ArrowReader>),
    SQL(Py<SQLReader>),
    Parquet(Py<ParquetReader>),
    PyGen(Py<PyGeneratorReader>),
    Excel(Py<ExcelReader>),
    Kafka(Py<KafkaReader>),
    MultiParquet(Py<MultiParquetReader>),
    Delta(Py<DeltaReader>),
}

impl PipeReader {
    pub fn read_batch<'py>(
        &self,
        py: Python<'py>,
        batch_size: usize,
    ) -> PyResult<Option<Bound<'py, PyList>>> {
        match self {
            PipeReader::CSV(r) => r.bind(py).borrow().read_batch(py, batch_size),
            PipeReader::JSON(r) => r.bind(py).borrow().read_batch(py, batch_size),
            PipeReader::Arrow(r) => r.bind(py).borrow().read_batch(py, batch_size),
            PipeReader::SQL(r) => r.bind(py).borrow().read_batch(py, batch_size),
            PipeReader::Parquet(r) => r.bind(py).borrow().read_batch(py, batch_size),
            PipeReader::Excel(r) => r.bind(py).borrow().read_batch(py, batch_size),
            PipeReader::PyGen(r) => r.bind(py).borrow().read_batch(py, batch_size),
            PipeReader::Kafka(_) => Ok(None),
            PipeReader::MultiParquet(r) => r.bind(py).borrow().read_batch(py, batch_size),
            PipeReader::Delta(r) => r.bind(py).borrow().read_batch(py, batch_size),
        }
    }

    pub fn next<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        match self {
            PipeReader::CSV(r) => CSVReader::__next__(r.bind(py).borrow()),
            PipeReader::JSON(r) => JSONReader::__next__(r.bind(py).borrow()),
            PipeReader::Arrow(r) => ArrowReader::__next__(r.bind(py).borrow()),
            PipeReader::SQL(r) => SQLReader::__next__(r.bind(py).borrow()),
            PipeReader::Parquet(r) => ParquetReader::__next__(r.bind(py).borrow()),
            PipeReader::PyGen(r) => PyGeneratorReader::__next__(r.bind(py).borrow()),
            PipeReader::Excel(r) => ExcelReader::__next__(r.bind(py).borrow()),
            PipeReader::Kafka(r) => KafkaReader::__next__(r.bind(py).borrow()),
            PipeReader::MultiParquet(r) => {
                crate::parsers::parquet::MultiParquetReader::__next__(r.bind(py).borrow())
            }
            PipeReader::Delta(r) => {
                crate::parsers::delta::DeltaReader::__next__(r.bind(py).borrow())
            }
        }
    }
}

#[derive(FromPyObject)]
pub enum PipeWriter {
    CSV(Py<CSVWriter>),
    JSON(Py<JSONWriter>),
    Arrow(Py<ArrowWriter>),
    SQL(Py<SQLWriter>),
    Parquet(Py<ParquetWriter>),
    PyGen(Py<PyGeneratorWriter>),
    Excel(Py<ExcelWriter>),
    Kafka(Py<KafkaWriter>),
    Iceberg(Py<crate::parsers::iceberg::IcebergWriter>),
    Delta(Py<DeltaWriter>),
}

impl PipeWriter {
    pub fn write_batch(&self, py: Python, entries: Bound<PyAny>) -> PyResult<()> {
        match self {
            PipeWriter::CSV(w) => w.bind(py).borrow().write_batch(py, entries),
            PipeWriter::JSON(w) => w.bind(py).borrow().write_batch(py, entries),
            PipeWriter::Arrow(w) => w.bind(py).borrow().write_batch(py, entries),
            PipeWriter::SQL(w) => w.bind(py).borrow().write_batch(py, entries),
            PipeWriter::Parquet(w) => w.bind(py).borrow().write_batch(py, entries),
            PipeWriter::PyGen(w) => w.bind(py).borrow().write_batch(py, entries),
            PipeWriter::Excel(w) => w.bind(py).borrow().write_batch(py, entries),
            PipeWriter::Kafka(w) => w.bind(py).borrow().write_batch(py, entries),
            PipeWriter::Iceberg(w) => w.bind(py).borrow().write_batch(py, entries),
            PipeWriter::Delta(w) => w.bind(py).borrow().write_batch(py, entries),
        }
    }

    pub fn close(&self, py: Python) -> PyResult<String> {
        match self {
            PipeWriter::CSV(w) => {
                w.bind(py).borrow().close()?;
                Ok("[]".into())
            }
            PipeWriter::JSON(w) => {
                w.bind(py).borrow().close()?;
                Ok("[]".into())
            }
            PipeWriter::Arrow(w) => {
                w.bind(py).borrow().close()?;
                Ok("[]".into())
            }
            PipeWriter::SQL(w) => {
                w.bind(py).borrow().close()?;
                Ok("[]".into())
            }
            PipeWriter::Parquet(w) => {
                w.bind(py).borrow().close()?;
                Ok("[]".into())
            }
            PipeWriter::PyGen(w) => {
                w.bind(py).borrow().close()?;
                Ok("[]".into())
            }
            PipeWriter::Excel(w) => {
                w.bind(py).borrow().close()?;
                Ok("[]".into())
            }
            PipeWriter::Kafka(w) => {
                w.bind(py).borrow().close()?;
                Ok("[]".into())
            }
            PipeWriter::Iceberg(w) => w.bind(py).borrow().close(),
            PipeWriter::Delta(w) => w.bind(py).borrow().close(),
        }
    }
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
            PipeExecutor::SingleThread(e) => {
                e.bind(py).borrow().process_batches(py, batches, processor)
            }
            PipeExecutor::MultiThread(e) => {
                e.bind(py).borrow().process_batches(py, batches, processor)
            }
        }
    }

    pub fn get_batch_size(&self, py: Python<'_>) -> PyResult<usize> {
        match self {
            PipeExecutor::SingleThread(e) => Ok(e.bind(py).borrow().get_batch_size()),
            PipeExecutor::MultiThread(e) => Ok(e.bind(py).borrow().get_batch_size()),
        }
    }

    pub fn get_concurrency(&self, py: Python<'_>) -> PyResult<usize> {
        match self {
            PipeExecutor::SingleThread(e) => Ok(e.bind(py).borrow().get_concurrency()),
            PipeExecutor::MultiThread(e) => Ok(e.bind(py).borrow().get_concurrency()),
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

/// Internal Rust implementation of the data pipeline.
///
/// NativePipe handles the heavy lifting of reading from sources, coordinating
/// batch processing (which calls back into Python), and writing to destinations.
/// It is designed for high performance and minimal GIL interaction during I/O.
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
        let models = py.import("zoopipe.structs")?;
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

    /// Executes the pipeline until the source is exhausted or an error occurs.
    fn run(&self, py: Python<'_>) -> PyResult<Option<String>> {
        let report = self.report.bind(py);
        report.call_method0("_mark_running")?;

        let batch_size = self.executor.get_batch_size(py)?;
        let concurrency = self.executor.get_concurrency(py)?;

        // Setup buffering for both row-wise accumulation and batch-wise accumulation
        let mut row_buffer = Vec::with_capacity(batch_size);
        let mut batch_buffer: Vec<Bound<'_, PyList>> = Vec::with_capacity(concurrency);

        loop {
            // Priority 1: Check if reader has a full batch ready (e.g. Arrow/Parquet/SQL chunks)
            if let Some(batch) = self.reader.read_batch(py, batch_size)? {
                batch_buffer.push(batch);
                if batch_buffer.len() >= concurrency {
                    self.process_buffered_batches(py, &mut batch_buffer, report)?;
                }
                continue;
            }

            // Priority 2: Streaming row-by-row (e.g. CSV/JSON/Generator)
            match self.reader.next(py)? {
                Some(entry) => {
                    row_buffer.push(entry);
                    if row_buffer.len() >= batch_size {
                        let py_list = PyList::new(py, row_buffer.iter())?;
                        batch_buffer.push(py_list);
                        row_buffer.clear();

                        if batch_buffer.len() >= concurrency {
                            self.process_buffered_batches(py, &mut batch_buffer, report)?;
                        }
                    }
                }
                None => break,
            }
        }

        // Flush remaining rows
        if !row_buffer.is_empty() {
            let py_list = PyList::new(py, row_buffer.iter())?;
            batch_buffer.push(py_list);
        }

        // Flush remaining batches
        if !batch_buffer.is_empty() {
            self.process_buffered_batches(py, &mut batch_buffer, report)?;
        }

        self.sync_report(py, report)?;
        report.call_method0("_mark_completed")?;

        let metadata = self.writer.close(py)?;
        if let Some(ref ew) = self.error_writer {
            ew.close(py)?;
        }

        Ok(Some(metadata))
    }
}

impl NativePipe {
    fn process_buffered_batches(
        &self,
        py: Python<'_>,
        batch_buffer: &mut Vec<Bound<'_, PyList>>,
        report: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let batch_processor = self.batch_processor.bind(py);

        let batches = std::mem::take(batch_buffer);
        let results = self
            .executor
            .process_batches(py, batches, batch_processor)?;

        for processed_entries in results {
            if let Ok(processed_list) = processed_entries.cast::<PyList>() {
                self.handle_processed_entries(py, processed_list, report)?;
            } else {
                return Err(PyRuntimeError::new_err("Batch processor returned non-list"));
            }
        }
        Ok(())
    }

    fn handle_processed_entries(
        &self,
        py: Python<'_>,
        processed_list: &Bound<'_, PyList>,
        report: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let mut success_data = Vec::with_capacity(processed_list.len());
        let mut error_list = Vec::new();

        let val_key = pyo3::intern!(py, "validated_data");
        let raw_key = pyo3::intern!(py, "raw_data");
        let status_key = pyo3::intern!(py, "status");

        let status_failed = self.status_failed.bind(py);
        let status_validated = self.status_validated.bind(py);

        for entry in processed_list.iter() {
            let dict = entry.cast::<PyDict>()?;

            // Fast status check using raw pointer comparison for interned objects
            let status = dict
                .get_item(status_key)?
                .ok_or_else(|| PyRuntimeError::new_err("Missing status in entry"))?;

            let is_failed = status.as_ptr() == status_failed.as_ptr();
            let is_validated = !is_failed && status.as_ptr() == status_validated.as_ptr();

            if is_failed {
                error_list.push(
                    dict.get_item(raw_key)?.ok_or_else(|| {
                        PyRuntimeError::new_err("Missing raw_data in error entry")
                    })?,
                );
            } else if is_validated {
                success_data.push(dict.get_item(val_key)?.ok_or_else(|| {
                    PyRuntimeError::new_err("Missing validated_data in success entry")
                })?);
            } else {
                success_data.push(
                    dict.get_item(raw_key)?
                        .ok_or_else(|| PyRuntimeError::new_err("Missing raw_data in entry"))?,
                );
            }
        }

        if !success_data.is_empty() {
            let data_list = PyList::new(py, success_data.iter())?;
            self.writer.write_batch(py, data_list.into_any())?;
        }

        if !error_list.is_empty()
            && let Some(ref ew) = self.error_writer
        {
            let data_list = PyList::new(py, error_list.iter())?;
            ew.write_batch(py, data_list.into_any())?;
        }

        let batch_len = processed_list.len();
        let success_len = success_data.len();
        let error_len = error_list.len();

        self.counters
            .total_processed
            .fetch_add(batch_len, Ordering::Relaxed);
        self.counters
            .success_count
            .fetch_add(success_len, Ordering::Relaxed);
        self.counters
            .error_count
            .fetch_add(error_len, Ordering::Relaxed);
        let batches_count = self
            .counters
            .batches_processed
            .fetch_add(1, Ordering::Relaxed)
            + 1;

        let should_sync = self.report_update_interval > 0
            && batches_count.is_multiple_of(self.report_update_interval);

        if should_sync {
            self.sync_report(py, report)?;
        }

        Ok(())
    }

    fn sync_report(&self, _py: Python<'_>, report: &Bound<'_, PyAny>) -> PyResult<()> {
        report.setattr(
            "total_processed",
            self.counters.total_processed.load(Ordering::Relaxed),
        )?;
        report.setattr(
            "success_count",
            self.counters.success_count.load(Ordering::Relaxed),
        )?;
        report.setattr(
            "error_count",
            self.counters.error_count.load(Ordering::Relaxed),
        )?;
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

        counters
            .total_processed
            .fetch_add(1_000_000, Ordering::Relaxed);

        assert_eq!(counters.total_processed.load(Ordering::Relaxed), 1_000_000);
    }
}
