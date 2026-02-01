# ADR-009: Shared Memory IPC for Status Reporting

## Status
Accepted

## Context
When executing "Heavy ETL" workloads using the `PipeManager` (Multi-process strategy, see ADR-006), the coordinator process needs real-time visibility into the status of each worker (processed count, errors, RAM usage).
Standard Python Inter-Process Communication (IPC) mechanisms like `multiprocessing.Queue`, `Pipe`, or `Manager` objects typically involve:
1.  **Serialization (Pickling)**: High CPU overhead.
2.  **Locking**: Potential contention when many workers report frequently.
3.  **Context Switching**: Increased latency.

For a high-performance system processing millions of rows, this reporting overhead can become a bottleneck or cause jitter.

## Decision
We utilize **Memory-Mapped Files (`mmap`)** for high-frequency, low-latency status reporting between worker processes and the coordinator.

### Implementation

1.  **Structure**:
    -   We define a fixed binary layout for the status block using `struct` (Format: `qqqqii`).
    -   Fields: `total_processed` (i64), `success_count` (i64), `error_count` (i64), `ram_bytes` (i64), `is_finished` (i32/bool), `has_error` (i32/bool).
    -   Size: Fixed (e.g., 40 bytes).

2.  **Workflow**:
    -   **Coordinator**: Creates a temporary directory and initializes a zero-filled binary file for each worker (e.g., `pipe_0.stats`, `pipe_1.stats`).
    -   **Worker**: Opens its assigned file in `r+b` mode and maps it into memory. It updates the counters in-place using atomic-like writes (or simple overwrites since it's a single writer per file).
    -   **Coordinator (Monitor)**: Periodically reads these files to aggregate the total progress for the CLI progress bar or API report.

3.  **Isolation**:
    -   Each worker writes to its own file, eliminating write contention and the need for cross-process locks.

## Consequences

### Benefits
-   **Zero Serialization**: Metrics are written as raw bytes, avoiding Pickle completely.
-   **Non-Blocking**: Workers update stats without waiting for the coordinator to consume them.
-   **Performance**: Negligible impact on the pipeline's throughput, even with frequent updates.
-   **Simplicity**: Avoids complex `asyncio` or thread management required for queue consumers.

### Drawbacks
-   **Disk I/O**: Relies on the OS to handle the memory-to-disk sync (though usually stays in page cache for temp files).
-   **Cleanup**: Requires careful cleanup of temporary files/directories (`/tmp/zoopipe_...`) to avoid clutter, especially after crashes.
-   **Fixed Schema**: Adding new metrics requires changing the binary struct layout and ensuring both Reader and Writer are updated.
