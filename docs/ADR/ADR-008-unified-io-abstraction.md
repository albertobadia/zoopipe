# ADR-008: Unified I/O Abstraction Layer

## Status
Accepted

## Context
ZooPipe requires the capability to read from and write to various data sources, including local file systems and cloud object stores (S3, GCS, Azure), while supporting multiple file formats (CSV, JSON, Parquet, Arrow).
Implementing storage logic (authentication, retries, protocol handling) and compression handling (Gzip, Zstd) separately within each format parser (e.g., `CSVReader`, `ParquetReader`) would lead to:
1.  **Code Duplication**: Repeating connection logic for S3/GCS in every parser.
2.  **Inconsistency**: Different parsers might handle retries or credentials differently.
3.  **Maintenance Burden**: Adding a new storage backend (e.g., Azure) would require updating every parser.
4.  **Testing Complexity**: Mocking storage for tests becomes fragmented.

## Decision
We implement a **Unified I/O Abstraction Layer** within the Rust core (`zoopipe_core::io`).

### Key Components

1.  **`BoxedReader` and `BoxedWriter`**:
    -   Enums that wrap concrete implementations for Local Files, In-Memory Cursors, and Remote Streams.
    -   They implement standard `std::io::Read`, `std::io::BufRead`, `std::io::Seek`, and `std::io::Write` traits.
    -   This allows downstream parsers (like `parquet::arrow::arrow_reader`) to operate on a standard trait object without knowing the underlying storage.

2.  **`StorageController`**:
    -   A centralized factory that parses URI schemes (e.g., `s3://`, `gs://`, `file://`) and initializes the appropriate `ObjectStore` backend.
    -   Handles configuration injection via environment variables (e.g., `AWS_ACCESS_KEY_ID`, `GOOGLE_APPLICATION_CREDENTIALS`).

3.  **Transparent Compression**:
    -   The layer automatically detects compression based on file extensions (`.gz`, `.zst`).
    -   It wraps the underlying stream in the appropriate decoder/encoder transparently to the parser.

4.  **Remote Seeking via Buffering**:
    -   For formats requiring random access (Parquet), `RemoteReader` implements `Seek` by buffering chunks of data. It invalidates the buffer if a seek lands outside the current range, fetching a new chunk from the object store.

## Consequences

### Benefits
-   **Polymorphism**: Parsers are agnostic to the storage backend. A `CSVReader` works identically on a local file or an S3 object.
-   **Centralized Config**: Auth and retry policies are defined in one place (`io/storage.rs`).
-   **Performance**: Optimized implementations for buffering and chunk fetching can be tuned globally.
-   **Extensibility**: Adding support for a new cloud provider only requires changes in the `io` module.

### Drawbacks
-   **Complexity**: The `RemoteReader` implementation is complex, requiring careful state management for buffering and seeking to avoid excessive API calls.
-   **Double Buffering Risk**: If a parser wraps a `BoxedReader` in another `BufReader`, it might lead to redundant memory usage.
-   **Abstraction Leaks**: Some storage-specific errors might be masked or genericized, making debugging specific S3/Azure issues slightly harder from the Python layer.
