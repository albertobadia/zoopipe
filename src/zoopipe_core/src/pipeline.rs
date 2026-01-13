use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyAnyMethods};
use pyo3::exceptions::PyRuntimeError;

use crate::parsers::csv::{CSVReader, CSVWriter};
use crate::parsers::json::{JSONReader, JSONWriter};
use crate::parsers::duckdb::{DuckDBReader, DuckDBWriter};
use std::sync::Mutex;



#[derive(FromPyObject)]
pub enum PipeReader {
    CSV(Py<CSVReader>),
    JSON(Py<JSONReader>),
    DuckDB(Py<DuckDBReader>),
}

#[derive(FromPyObject)]
pub enum PipeWriter {
    CSV(Py<CSVWriter>),
    JSON(Py<JSONWriter>),
    DuckDB(Py<DuckDBWriter>),
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

struct PipeCounters {
    total_processed: usize,
    success_count: usize,
    error_count: usize,
    batches_processed: usize,
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
    counters: Mutex<PipeCounters>,
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
            counters: Mutex::new(PipeCounters {
                total_processed: 0,
                success_count: 0,
                error_count: 0,
                batches_processed: 0,
            }),
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
        }

        if let Some(ref ew) = self.error_writer {
            match ew {
                PipeWriter::CSV(w) => w.bind(py).borrow().close()?,
                PipeWriter::JSON(w) => w.bind(py).borrow().close()?,
                PipeWriter::DuckDB(w) => w.bind(py).borrow().close()?,
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
            }
        }

        if !error_list.is_empty() && let Some(ref ew) = self.error_writer {
            let data_list = PyList::new(py, error_list.iter())?;
            match ew {
                PipeWriter::CSV(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
                PipeWriter::JSON(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
                PipeWriter::DuckDB(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
            }
        }

        let should_sync = {
            let mut counters = self.counters.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
            counters.total_processed += processed_list.len();
            counters.success_count += success_data.len();
            counters.error_count += error_list.len();
            counters.batches_processed += 1;
            
            self.report_update_interval > 0 && counters.batches_processed % self.report_update_interval == 0
        };

        if should_sync {
            self.sync_report(py, report)?;
        }

        Ok(())
    }

    fn sync_report(&self, _py: Python<'_>, report: &Bound<'_, PyAny>) -> PyResult<()> {
        let counters = self.counters.lock().map_err(|_| PyRuntimeError::new_err("Mutex lock failed"))?;
        
        report.setattr("total_processed", counters.total_processed)?;
        report.setattr("success_count", counters.success_count)?;
        report.setattr("error_count", counters.error_count)?;
        report.setattr("ram_bytes", get_process_ram_rss())?;
        
        Ok(())
    }
}

fn get_process_ram_rss() -> usize {
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/self/status") {
            for line in content.lines() {
                if line.starts_with("VmRSS:")
                    && let Some(kb_part) = line.split_whitespace().nth(1)
                        && let Ok(kb) = kb_part.parse::<usize>() {
                            return kb * 1024;
                        }
            }
        }
    }
    
    0
}
