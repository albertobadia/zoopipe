use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::exceptions::PyRuntimeError;
use crate::hooks::PyHookAdapter;
use crate::validation::NativeValidator;
use crossbeam::channel;
use std::thread;

#[pyclass]
pub struct RustParallelEngine {
    pre_hooks: Vec<PyHookAdapter>,
    post_hooks: Vec<PyHookAdapter>,
    validator: Option<Py<NativeValidator>>,
    output_adapter: Option<Py<PyAny>>,
}

#[pymethods]
impl RustParallelEngine {
    #[new]
    fn new(
        pre_hooks: Vec<Bound<'_, PyAny>>,
        post_hooks: Vec<Bound<'_, PyAny>>,
        validator: Option<Py<NativeValidator>>,
        output_adapter: Option<Bound<'_, PyAny>>,
    ) -> Self {
        RustParallelEngine {
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
        buffer_size: usize,
    ) -> PyResult<()> {
        // Create channels
        let (tx_raw, rx_raw) = channel::bounded::<Vec<Py<PyAny>>>(buffer_size);
        let (tx_val, rx_val) = channel::bounded::<Vec<Py<PyAny>>>(buffer_size);

        // Clone state for threads
        let pre_hooks = self.pre_hooks.clone();
        let post_hooks = self.post_hooks.clone();
        let validator = self.validator.clone();
        let output_adapter = self.output_adapter.clone();
        // Report should be safe to clone (Py<PyAny>)
        let report_obj = report.map(|r| r.unbind());
        let report_for_proc = report_obj.clone();

        // --- Processor Thread ---
        let processor_handle = thread::spawn(move || {
            Python::with_gil(|py| {
                let store = PyDict::new(py);
                
                // 1. Setup
                for hook in &pre_hooks {
                    if let Err(e) = hook.setup(py, &store) {
                        eprintln!("Error in pre_hook setup: {}", e);
                        return; // Abort thread
                    }
                }
                for hook in &post_hooks {
                     if let Err(e) = hook.setup(py, &store) {
                        eprintln!("Error in post_hook setup: {}", e);
                         return;
                    }
                }

                // 2. Loop
                loop {
                    // Receive batch without holding GIL
                    let batch_res = py.allow_threads(|| rx_raw.recv());
                    
                    match batch_res {
                        Ok(batch_vec) => {
                            // Process batch (Needs GIL)
                            // Convert Vec<Py<PyAny>> to PyList for consistency with hooks
                            let py_list = PyList::new(py, batch_vec).unwrap(); 
                            let mut entries: Bound<'_, PyAny> = py_list.into_any();

                            // Exec Hooks
                            for hook in &pre_hooks {
                                match hook.execute(py, entries, &store) {
                                    Ok(res) => entries = res,
                                    Err(e) => {
                                        eprintln!("Error in pre_hook: {}", e);
                                        // TODO: How to handle error? Continue or abort?
                                        // For now, abort/break? or just continue with current entries?
                                        // If hook fails, pipeline might be broken.
                                        break; 
                                    }
                                }
                            }
                            
                            // Validate
                             if let Some(ref val_py) = validator {
                                let val = val_py.bind(py);
                                if let Ok(ent_list) = entries.cast::<PyList>() {
                                     if let Err(e) = val.call_method1("validate_batch", (ent_list,)) {
                                         eprintln!("Error in validation: {}", e);
                                     }
                                }
                            }

                            // Post Hooks
                            for hook in &post_hooks {
                                match hook.execute(py, entries, &store) {
                                     Ok(res) => entries = res,
                                     Err(e) => eprintln!("Error in post_hook: {}", e),
                                }
                            }
                            
                            // Update Report
                            if let Some(ref r) = report_for_proc {
                                if let Ok(ent_list) = entries.cast::<PyList>() {
                                    let count = ent_list.len();
                                     let r_bound = r.bind(py);
                                     if let Ok(current) = r_bound.getattr("total_processed").and_then(|v| v.extract::<usize>()) {
                                         let _ = r_bound.setattr("total_processed", current + count);
                                     }
                                }
                            }

                            // Send to Writer (Convert back to Vec<Py<PyAny>> or just send Bound?)
                            // We need to send owned PyObjects.
                            if let Ok(ent_list) = entries.cast::<PyList>() {
                                let mut out_vec = Vec::with_capacity(ent_list.len());
                                for item in ent_list.iter() {
                                    out_vec.push(item.unbind());
                                }
                                
                                // Send without GIL
                                if py.allow_threads(|| tx_val.send(out_vec)).is_err() {
                                    break; // Receiver closed
                                }
                            }
                        }
                        Err(_) => break, // Channel closed (EOF)
                    }
                }

                // 3. Teardown
                for hook in &pre_hooks {
                    let _ = hook.teardown(py, &store);
                }
                for hook in &post_hooks {
                    let _ = hook.teardown(py, &store);
                }
            });
        });

        // --- Writer Thread ---
        let writer_handle = thread::spawn(move || {
             Python::with_gil(|py| {
                if let Some(ref oa_py) = output_adapter {
                    let oa = oa_py.bind(py);
                    // Open
                    if let Ok(true) = oa.hasattr("open") {
                         if let Err(e) = oa.call_method0("open") {
                             eprintln!("Output open failed: {}", e);
                             return;
                         }
                    }

                    // Loop
                    loop {
                         let batch_res = py.allow_threads(|| rx_val.recv());
                         match batch_res {
                             Ok(batch_vec) => {
                                 // Write Logic (matches RustPipeEngine)
                                 let data_list = PyList::empty(py);
                                 let val_key = pyo3::intern!(py, "validated_data");
                                 let raw_key = pyo3::intern!(py, "raw_data");

                                 for entry in batch_vec {
                                     let entry_bound = entry.bind(py);
                                     if let Ok(dict) = entry_bound.cast::<PyDict>() {
                                         let data = if let Ok(Some(val)) = dict.get_item(val_key) {
                                             val
                                         } else if let Ok(Some(raw)) = dict.get_item(raw_key) {
                                             raw
                                         } else {
                                             continue; // Skip invalid
                                         };
                                         let _ = data_list.append(data);
                                     }
                                 }

                                 if let Ok(true) = oa.hasattr("write_batch") {
                                     if let Err(e) = oa.call_method1("write_batch", (data_list,)) {
                                          eprintln!("write_batch failed: {}", e);
                                     }
                                 } else {
                                     for data in data_list.iter() {
                                         let _ = oa.call_method1("write", (data,));
                                     }
                                 }
                             }
                             Err(_) => break, // Channel closed
                         }
                    }

                    // Close
                    if let Ok(true) = oa.hasattr("close") {
                         let _ = oa.call_method0("close");
                    }
                } else {
                     // Just drain if no adapter
                     loop {
                         if py.allow_threads(|| rx_val.recv()).is_err() { break; }
                     }
                }
             });
        });

        // --- Reader Loop (Main Thread) ---
        let mut batch_entries = Vec::with_capacity(batch_size);
        
        // We hold GIL here (py).
        for item in reader.try_iter()? {
            batch_entries.push(item?.unbind());
            
            if batch_entries.len() >= batch_size {
                let batch_to_send = std::mem::replace(&mut batch_entries, Vec::with_capacity(batch_size));
                
                // Send without GIL
                if py.allow_threads(|| tx_raw.send(batch_to_send)).is_err() {
                    return Err(PyRuntimeError::new_err("Pipeline broken (Processor stopped)"));
                }
            }
        }
        
        if !batch_entries.is_empty() {
             let _ = py.allow_threads(|| tx_raw.send(batch_entries));
        }

        // Close Reader channel
        drop(tx_raw);

        // Join threads
        // We must release GIL to join! Otherwise if thread is waiting for GIL, we deadlock.
        py.allow_threads(|| {
            let _ = processor_handle.join();
            let _ = writer_handle.join();
        });

        Ok(())
    }
}
