# ADR-002: Batch-based Pipeline Processing

## Status
Accepted

## Context
Processing data row-by-row in Python is slow due to function call overhead and the GIL. Conversely, processing an entire multi-gigabyte file in memory at once is not feasible for many environments.

## Decision
ZooPipe uses a **batch-based (chunked) processing model**. Data is read from the input source in batches (defaulting to 1000-5000 records), processed as a block throughout the pipeline, and written to the output in batches.

### Implementation Details
-   The Rust core fetches a chunk of raw data.
-   This chunk is passed to the Python layer for validation (Pydantic) and transformation (Hooks) as a list of dictionaries.
-   The results are returned to Rust for final output writing.
-   Memory management is optimized by reusing buffers where possible and minimizing copies between Python and Rust.

## Consequences
-   **Benefit**: Amortizes the cost of crossing the Python-Rust boundary across multiple records.
-   **Benefit**: Predictable and low memory usage regardless of total dataset size.
-   **Benefit**: Enables efficient batch inserts for databases (SQL) and batch writes for files (Parquet/Arrow).
-   **Drawback**: Latency per record might be slightly higher than a pure streaming approach, though overall throughput is much higher.
-   **Drawback**: Error handling must manage batch failures and record-level granularity within a batch.
