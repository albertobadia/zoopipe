use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::exceptions::PyRuntimeError;
use crate::hooks::PyHookAdapter;
use crate::validation::NativeValidator;

#[pyclass]
pub struct RustPipeEngine {
    pre_hooks: Vec<PyHookAdapter>,
    post_hooks: Vec<PyHookAdapter>,
    validator: Option<Py<NativeValidator>>,
    output_adapter: Option<Py<PyAny>>,
}

#[pymethods]
impl RustPipeEngine {
    #[new]
    fn new(
        pre_hooks: Vec<Bound<'_, PyAny>>,
        post_hooks: Vec<Bound<'_, PyAny>>,
        validator: Option<Py<NativeValidator>>,
        output_adapter: Option<Bound<'_, PyAny>>,
    ) -> Self {
        RustPipeEngine {
            pre_hooks: pre_hooks.into_iter().map(PyHookAdapter::new).collect(),
            post_hooks: post_hooks.into_iter().map(PyHookAdapter::new).collect(),
            validator,
            output_adapter: output_adapter.map(|a| a.unbind()),
        }
    }

    fn run(
        &self,
        py: Python<'_>,
        reader: Bound<'_, PyAny>,
        report: Option<Bound<'_, PyAny>>,
        batch_size: usize,
    ) -> PyResult<()> {
        let store = PyDict::new(py);

        for hook in &self.pre_hooks {
            hook.setup(py, &store)?;
        }
        for hook in &self.post_hooks {
            hook.setup(py, &store)?;
        }

        if let Some(ref oa_py) = self.output_adapter {
            let oa = oa_py.bind(py);
            if oa.hasattr("open")? {
                oa.call_method0("open")?;
            }
        }

        let mut batch_entries = Vec::with_capacity(batch_size);
        let mut total_processed = 0;

        for item in reader.try_iter()? {
            let item_bound = item?;
            batch_entries.push(item_bound);

            if batch_entries.len() >= batch_size {
                self.process_batch(py, &mut batch_entries, &store, report.as_ref())?;
                total_processed += batch_entries.len();
                batch_entries.clear();
            }
        }

        if !batch_entries.is_empty() {
            let count = batch_entries.len();
            self.process_batch(py, &mut batch_entries, &store, report.as_ref())?;
            total_processed += count;
        }

        if let Some(ref r) = report {
            r.setattr("total_processed", total_processed)?;
        }

        if let Some(ref oa_py) = self.output_adapter {
            let oa = oa_py.bind(py);
            if oa.hasattr("close")? {
                oa.call_method0("close")?;
            }
        }

        for hook in &self.pre_hooks {
            hook.teardown(py, &store)?;
        }
        for hook in &self.post_hooks {
            hook.teardown(py, &store)?;
        }

        Ok(())
    }
}

impl RustPipeEngine {
    fn process_batch(
        &self,
        py: Python<'_>,
        batch: &mut Vec<Bound<'_, PyAny>>,
        store: &Bound<'_, PyDict>,
        report: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let py_list = PyList::new(py, batch.iter())?;
        let mut entries: Bound<'_, PyAny> = py_list.into_any();

        for hook in &self.pre_hooks {
            entries = hook.execute(py, entries, store)?;
        }

        if let Some(ref val_py) = self.validator {
            let val = val_py.bind(py);
            let ent_list = entries.cast::<PyList>()?;
            val.call_method1("validate_batch", (ent_list,))?;
        }

        for hook in &self.post_hooks {
            entries = hook.execute(py, entries, store)?;
        }

        if let Some(ref oa_py) = self.output_adapter {
            let oa = oa_py.bind(py);
            let ent_list = entries.cast::<PyList>()?;
            
            // Optimization: Extract data only once
            let data_list = PyList::empty(py);
            let val_key = pyo3::intern!(py, "validated_data");
            let raw_key = pyo3::intern!(py, "raw_data");

            for entry in ent_list.iter() {
                let dict = entry.cast::<PyDict>()?;
                let data = if let Ok(Some(val)) = dict.get_item(val_key) {
                    val
                } else {
                    dict.get_item(raw_key)?.ok_or_else(|| PyRuntimeError::new_err("Missing data in entry"))?
                };
                data_list.append(data)?;
            }

            if oa.hasattr("write_batch")? {
                oa.call_method1("write_batch", (data_list,))?;
            } else {
                for data in data_list.iter() {
                    oa.call_method1("write", (data,))?;
                }
            }
        }

        if let Some(r) = report {
            let current: usize = r.getattr("total_processed")?.extract()?;
            r.setattr("total_processed", current + batch.len())?;
        }

        Ok(())
    }
}
