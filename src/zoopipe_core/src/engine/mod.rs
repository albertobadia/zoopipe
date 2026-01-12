use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::exceptions::PyRuntimeError;
use crate::hooks::PyHookAdapter;
use crate::validation::NativeValidator;
use std::thread;
use crossbeam::channel;

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


#[pyclass]
pub struct RustParallelEngine {
    pre_hooks: Vec<PyHookAdapter>,
    post_hooks: Vec<PyHookAdapter>,
    validator: Option<Py<NativeValidator>>,
    output_adapter: Option<Py<PyAny>>,
    error_output_adapter: Option<Py<PyAny>>,
}

#[pymethods]
impl RustParallelEngine {
    #[new]
    fn new(
        pre_hooks: Vec<Bound<'_, PyAny>>,
        post_hooks: Vec<Bound<'_, PyAny>>,
        validator: Option<Py<NativeValidator>>,
        output_adapter: Option<Bound<'_, PyAny>>,
        error_output_adapter: Option<Bound<'_, PyAny>>,
    ) -> Self {
        RustParallelEngine {
            pre_hooks: pre_hooks.into_iter().map(PyHookAdapter::new).collect(),
            post_hooks: post_hooks.into_iter().map(PyHookAdapter::new).collect(),
            validator,
            output_adapter: output_adapter.map(|a| a.unbind()),
            error_output_adapter: error_output_adapter.map(|a| a.unbind()),
        }
    }

    fn run(
        &self,
        py: Python<'_>,
        reader: Bound<'_, PyAny>,
        report: Option<Bound<'_, PyAny>>,
        batch_size: usize,
        buffer_size: usize,
    ) -> PyResult<()> {
        let (tx_raw, rx_raw) = channel::bounded::<Vec<Py<PyAny>>>(buffer_size);
        let (tx_val, rx_val) = channel::bounded::<Vec<Py<PyAny>>>(buffer_size);

        let pre_hooks: Vec<PyHookAdapter> = self.pre_hooks.iter().map(|h| h.clone_ref(py)).collect();
        let post_hooks: Vec<PyHookAdapter> = self.post_hooks.iter().map(|h| h.clone_ref(py)).collect();
        let validator = self.validator.as_ref().map(|v| v.clone_ref(py));
        let output_adapter = self.output_adapter.as_ref().map(|oa| oa.clone_ref(py));
        let error_output_adapter = self.error_output_adapter.as_ref().map(|oa| oa.clone_ref(py));
        let report_obj = report.as_ref().map(|r| r.clone().unbind());
        let report_for_proc = report_obj.as_ref().map(|r| r.clone_ref(py));

        let processor_handle = thread::spawn(move || {
            Python::attach(|py| {
                let store = PyDict::new(py);
                
                for hook in &pre_hooks {
                    if let Err(e) = hook.setup(py, &store) {
                        eprintln!("Error in pre_hook setup: {}", e);
                        return;
                    }
                }
                for hook in &post_hooks {
                     if let Err(e) = hook.setup(py, &store) {
                        eprintln!("Error in post_hook setup: {}", e);
                         return;
                    }
                }

                'processor_loop: loop {
                    let batch_res = py.detach(|| rx_raw.recv());
                    
                    match batch_res {
                        Ok(batch_vec) => {
                            let py_list = PyList::new(py, batch_vec).unwrap(); 
                            let mut entries: Bound<'_, PyAny> = py_list.into_any();

                            for hook in &pre_hooks {
                                match hook.execute(py, entries, &store) {
                                    Ok(res) => entries = res,
                                    Err(e) => {
                                        eprintln!("Error in pre_hook: {}", e);
                                        continue 'processor_loop; 
                                    }
                                }
                            }
                            
                             if let Some(ref val_py) = validator {
                                let val: &Bound<'_, NativeValidator> = val_py.bind(py);
                                if let Ok(ent_list) = entries.cast::<PyList>() {
                                     if let Err(e) = val.call_method1("validate_batch", (ent_list,)) {
                                         eprintln!("Error in validation: {}", e);
                                     }
                                }
                            }

                            for hook in &post_hooks {
                                match hook.execute(py, entries, &store) {
                                     Ok(res) => entries = res,
                                     Err(e) => {
                                         eprintln!("Error in post_hook: {}", e);
                                         continue 'processor_loop;
                                     }
                                }
                            }
                            
                            if let Some(ref r) = report_for_proc {
                                if let Ok(ent_list) = entries.cast::<PyList>() {
                                    let count = ent_list.len();
                                     let r_bound = r.bind(py);
                                     if let Ok(current) = r_bound.getattr("total_processed").and_then(|v| v.extract::<usize>()) {
                                         let _ = r_bound.setattr("total_processed", current + count);
                                     }
                                }
                            }

                            if let Ok(ent_list) = entries.cast::<PyList>() {
                                let mut out_vec = Vec::with_capacity(ent_list.len());
                                for item in ent_list.iter() {
                                    out_vec.push(item.unbind());
                                }
                                
                                if py.detach(|| tx_val.send(out_vec)).is_err() {
                                    break;
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }

                for hook in &pre_hooks {
                    let _ = hook.teardown(py, &store);
                }
                for hook in &post_hooks {
                    let _ = hook.teardown(py, &store);
                }
            });
        });

        let writer_handle = thread::spawn(move || {
             Python::attach(|py| {
                if let Some(ref oa_py) = output_adapter {
                    let oa = oa_py.bind(py);
                    if let Ok(true) = oa.hasattr("open") {
                         if let Err(e) = oa.call_method0("open") {
                             eprintln!("Output open failed: {}", e);
                             return;
                         }
                    }
                }
                if let Some(ref eoa_py) = error_output_adapter {
                    let eoa = eoa_py.bind(py);
                    if let Ok(true) = eoa.hasattr("open") {
                         if let Err(e) = eoa.call_method0("open") {
                             eprintln!("Error Output open failed: {}", e);
                         }
                    }
                }

                let status_failed = match py.import("zoopipe.models.core") {
                    Ok(m) => match m.getattr("EntryStatus") {
                        Ok(cls) => match cls.getattr("FAILED") {
                             Ok(val) => Some(val.unbind()),
                             Err(_) => None,
                        },
                        Err(_) => None,
                    },
                    Err(_) => None,
                };

                    loop {
                         let batch_res = py.detach(|| rx_val.recv());
                         match batch_res {
                             Ok(batch_vec) => {
                                 let success_list = PyList::empty(py);
                                 let error_list = PyList::empty(py);
                                 
                                 let val_key = pyo3::intern!(py, "validated_data");
                                 let raw_key = pyo3::intern!(py, "raw_data");
                                 let status_key = pyo3::intern!(py, "status");

                                 let mut success_count = 0;
                                 let mut error_count = 0;

                                 for entry in batch_vec {
                                     let entry_bound = entry.bind(py);
                                     if let Ok(dict) = entry_bound.cast::<PyDict>() {
                                         let is_failed = if let Some(ref failed_val) = status_failed {
                                             if let Ok(Some(status)) = dict.get_item(status_key) {
                                                 status.eq(failed_val).unwrap_or(false)
                                             } else { false }
                                         } else { false };

                                         let data = if let Ok(Some(val)) = dict.get_item(val_key) {
                                             val
                                          } else if let Ok(Some(raw)) = dict.get_item(raw_key) {
                                              raw
                                          } else {
                                              continue;
                                          };
                                          
                                          if is_failed {
                                              let _ = error_list.append(data);
                                              error_count += 1;
                                          } else {
                                              let _ = success_list.append(data);
                                              success_count += 1;
                                          }
                                     }
                                 }

                                 if !success_list.is_empty() {
                                     if let Some(ref oa_py) = output_adapter {
                                         let oa = oa_py.bind(py);
                                         if let Ok(true) = oa.hasattr("write_batch") {
                                             if let Err(e) = oa.call_method1("write_batch", (success_list,)) {
                                                  eprintln!("write_batch failed: {}", e);
                                             }
                                         } else {
                                             for data in success_list.iter() {
                                                 let _ = oa.call_method1("write", (data,));
                                             }
                                         }
                                     }
                                     
                                     if let Some(ref r_py) = report_obj {
                                         let r = r_py.bind(py);
                                         if let Ok(curr) = r.getattr("success_count").and_then(|v| v.extract::<usize>()) {
                                             let _ = r.setattr("success_count", curr + success_count);
                                         }
                                     }
                                 }

                                 if !error_list.is_empty() {
                                      if let Some(ref eoa_py) = error_output_adapter {
                                         let eoa = eoa_py.bind(py);
                                         if let Ok(true) = eoa.hasattr("write_batch") {
                                             let _ = eoa.call_method1("write_batch", (error_list,));
                                         } else {
                                             for data in error_list.iter() {
                                                  let _ = eoa.call_method1("write", (data,));
                                             }
                                         }
                                      }

                                     if let Some(ref r_py) = report_obj {
                                         let r = r_py.bind(py);
                                         if let Ok(curr) = r.getattr("error_count").and_then(|v| v.extract::<usize>()) {
                                             let _ = r.setattr("error_count", curr + error_count);
                                         }
                                     }
                                 }
                             }
                             Err(_) => break,
                         }
                    }

                    if let Some(ref oa_py) = output_adapter {
                        let oa = oa_py.bind(py);
                        if let Ok(true) = oa.hasattr("close") {
                             let _ = oa.call_method0("close");
                        }
                    }
                     if let Some(ref eoa_py) = error_output_adapter {
                        let eoa = eoa_py.bind(py);
                        if let Ok(true) = eoa.hasattr("close") {
                             let _ = eoa.call_method0("close");
                        }
                    }
             });
        });

        let mut batch_entries = Vec::with_capacity(batch_size);
        
        for item in reader.try_iter()? {
            if let Some(ref r) = report {
                if let Ok(is_finished) = r.getattr("is_finished").and_then(|v| v.extract::<bool>()) {
                    if is_finished {
                        break;
                    }
                }
            }
            batch_entries.push(item?.unbind());
            
            if batch_entries.len() >= batch_size {
                let batch_to_send = std::mem::replace(&mut batch_entries, Vec::with_capacity(batch_size));
                
                if py.detach(|| tx_raw.send(batch_to_send)).is_err() {
                    return Err(PyRuntimeError::new_err("Pipeline broken (Processor stopped)"));
                }
            }
        }
        
        if !batch_entries.is_empty() {
             let _ = py.detach(|| tx_raw.send(batch_entries));
        }

        drop(tx_raw);

        py.detach(|| {
            let _ = processor_handle.join();
            let _ = writer_handle.join();
        });

        Ok(())
    }
}
