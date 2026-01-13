use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyAnyMethods};
use pyo3::exceptions::PyRuntimeError;

use crate::parsers::csv::{CSVReader, CSVWriter};
use crate::parsers::json::{JSONReader, JSONWriter};

#[derive(FromPyObject)]
pub enum PipeReader {
    CSV(Py<CSVReader>),
    JSON(Py<JSONReader>),
}

#[derive(FromPyObject)]
pub enum PipeWriter {
    CSV(Py<CSVWriter>),
    JSON(Py<JSONWriter>),
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
        })
    }

    fn run(&self, py: Python<'_>, batch_size: usize) -> PyResult<()> {
        let report = self.report.bind(py);
        report.call_method0("_mark_running")?;

        let mut batch_entries = Vec::with_capacity(batch_size);
        
        loop {
            let next_item = match &self.reader {
                PipeReader::CSV(r) => CSVReader::__next__(r.bind(py).borrow()),
                PipeReader::JSON(r) => JSONReader::__next__(r.bind(py).borrow()),
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

        report.call_method0("_mark_completed")?;

        match &self.writer {
            PipeWriter::CSV(w) => w.bind(py).borrow().close()?,
            PipeWriter::JSON(w) => w.bind(py).borrow().close()?,
        }

        if let Some(ref ew) = self.error_writer {
            match ew {
                PipeWriter::CSV(w) => w.bind(py).borrow().close()?,
                PipeWriter::JSON(w) => w.bind(py).borrow().close()?,
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
            }
        }

        if !error_list.is_empty() {
            if let Some(ref ew) = self.error_writer {
                let data_list = PyList::new(py, error_list.iter())?;
                match ew {
                    PipeWriter::CSV(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
                    PipeWriter::JSON(w) => w.bind(py).borrow().write_batch(py, data_list.into_any())?,
                }
            }
        }

        let total_processed_val: usize = report.getattr("total_processed")?.extract()?;
        let success_count_val: usize = report.getattr("success_count")?.extract()?;
        let error_count_val: usize = report.getattr("error_count")?.extract()?;
        
        report.setattr("total_processed", total_processed_val + processed_list.len())?;
        report.setattr("success_count", success_count_val + success_data.len())?;
        report.setattr("error_count", error_count_val + error_list.len())?;

        Ok(())
    }
}
