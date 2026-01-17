# ADR-001: Hybrid Python-Rust Architecture

## Status
Accepted

## Context
ZooPipe aims to provide a high-performance data processing engine for Python developers. Python is excellent for flexibility, ease of use, and integration with the data science ecosystem (e.g., Pydantic, Arrow). However, Pure Python can be slow for heavy I/O and CPU-intensive orchestration loops, especially when dealing with millions of records.

## Decision
We utilize a hybrid architecture:
1.  **Rust Core**: The execution engine (`NativePipe`), data adapters (CSV, JSON, SQL), and the main orchestration loop are implemented in Rust using `PyO3`. This ensures maximum throughput for I/O and data parsing/serialization.
2.  **Python Interface**: Configuration, schema definition (Pydantic), and custom business logic (Hooks) are handled in Python. This provides a user-friendly API and leverages Python's flexibility for custom enrichment.

### Key Components
-   **NativePipe (Rust)**: Manages the fetching of data from Input Adapters and routing to Output Adapters.
-   **Adapters (Rust/Python)**: High-performance implementations in Rust for standard formats, with Python interfaces for extensibility.
-   **Python Layer**: Wraps the Rust core, providing the `Pipe` and `PipeManager` classes.

## Consequences
-   **Benefit**: Significant performance gains (3x-8x over pure Python/Pandas in "heavy ETL" scenarios).
-   **Benefit**: Low memory footprint by keeping heavy data operations in Rust.
-   **Benefit**: Access to Python's rich ecosystem for data validation and transformation.
-   **Drawback**: Increased complexity in building and distributing the package (requires `maturin`).
-   **Drawback**: Debugging across the Python/Rust boundary can be more challenging.
