# ADR-006: Multi-process Parallelism (PipeManager)

## Status
Accepted

## Context
Python's Global Interpreter Lock (GIL) prevents true multi-threaded CPU-bound execution. For large datasets, a single worker process often becomes the bottleneck, especially when heavy Python Hooks are involved.

## Decision
We implement a multi-process execution strategy via the `PipeManager`.

### Mechanism
-   Instead of threading, `PipeManager` spawns multiple independent worker processes.
-   For file-based inputs (like CSV), it leverages file offsets to allow multiple processes to read different parts of the same file concurrently without overlapping.
-   Each process has its own Python interpreter and memory space, effectively bypassing the GIL.
-   The Rust core in each process handles its own I/O, maintaining high performance.

## Consequences
-   **Benefit**: Linear scalability for CPU-bound tasks on multi-core systems.
-   **Benefit**: Drastic reduction in processing time for large-scale "Heavy ETL" workloads.
-   **Drawback**: Higher memory overhead due to multiple Python processes.
-   **Drawback**: Inter-process communication (IPC) and coordination overhead (though minimized by the design).
-   **Drawback**: Complexity in managing shared state or resources across processes.
