# ZooSync Engine

The `ZoosyncPoolEngine` is a specialized execution engine designed for **"Heavy ETL"** workloads where keeping track of granular progress (rows/sec, memory usage) across multiple processes is critical but typically expensive.

Unlike the default `MultiProcessEngine` which relies on standard Python `multiprocessing.Queue` (and thus Pickle serialization) for status reporting, **ZooSync uses shared memory (`mmap`)** to provide zero-latency, zero-serialization observability.

## When to use ZooSync?

- **High-Frequency Reporting**: You want real-time progress bars without slowing down the workers.
- **Chaos Engineering**: You are running complex "chaotic" pipelines where you expect frequent failures and need immediate visibility.
- **Maximum Performance**: You want to squeeze every bit of CPU by removing IPC (Inter-Process Communication) overhead.

## Installation

ZooSync is an optional dependency. Install it with:

```bash
uv add "zoopipe[zoosyncmp]"
```

## Usage

```python
from zoopipe import CSVInputAdapter, JSONOutputAdapter, Pipe, PipeManager
from zoopipe.engines.zoosync import ZoosyncPoolEngine

pipe = Pipe(
    input_adapter=CSVInputAdapter("large_dataset.csv"),
    output_adapter=JSONOutputAdapter("output.jsonl")
)

# Run with 4 workers using ZooSync
with PipeManager.parallelize_pipe(
    pipe, 
    workers=4, 
    engine=ZoosyncPoolEngine()
) as manager:
    manager.run()
```

## How it Works (Architecture)

1. **Shared Memory Layout**: 
   The engine creates a small, fixed-size binary file in a temporary directory for each worker. This file is memory-mapped (`mmap`) into both the Coordinator and the Worker process.

2. **Binary Protocol**:
   Instead of pickling Python objects, workers write raw C-structs (`long long` integers) directly to the memory map.
   - `total_processed`
   - `success_count`
   - `error_count`
   - `ram_bytes`
   
3. **Lock-Free Monitoring**:
   The Coordinator reads these memory maps periodically. Since the layout is fixed and writes are atomic-aligned, no locks are required. This means the Coordinator can poll status 100 times a second without impacting the Worker's performance.

## API Reference

### `ZoosyncPoolEngine`

```python
class ZoosyncPoolEngine(n_workers: int | None = None)
```

**Parameters:**
- `n_workers` (int, optional): Number of worker processes in the pool. If not provided, it defaults to the number of pipes passed to `start()`.

**Methods:**
- `start(pipes)`: Spawns the worker pool and initializes shared memory files.
- `wait(timeout)`: Waits for completion.
- `shutdown()`: Cleans up processes and temporary `mmap` files.
