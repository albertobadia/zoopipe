# Multithread Executor Usage Examples

This document demonstrates how to use the Single Thread and MultiThread executors in ZooPipe.

## Basic Usage

### SingleThreadExecutor (Default)

```python
from zoopipe import CSVInputAdapter, CSVOutputAdapter, Pipe, SingleThreadExecutor

pipe = Pipe(
    input_adapter=CSVInputAdapter("input.csv"),
    output_adapter=CSVOutputAdapter("output.csv"),
    executor=SingleThreadExecutor(batch_size=1000),  # Default batch_size
)

with pipe:
    pipe.wait()
```

### MultiThreadExecutor

```python
from zoopipe import CSVInputAdapter, JSONOutputAdapter, MultiThreadExecutor, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=JSONOutputAdapter("users.jsonl", format="jsonl"),
    executor=MultiThreadExecutor(
        max_workers=8,        # Number of threads (default: CPU count)
        batch_size=2000,      # Batch size per thread (default: 1000)
    ),
)

with pipe:
    pipe.wait()

print(f"Processed {pipe.report.total_processed} records")
```

## Two-Tier Parallel Model

ZooPipe uses a dual-layer approach to maximize performance:

1.  **Orchestyration Tier (Engines)**: Used to scale **out** across multiple processes or eventually nodes (Ray, Dask). Managed via `PipeManager`.
2.  **Execution Tier (Executors)**: Used to scale **up** within a single node by utilizing multiple Rust threads. Managed via the `executor` parameter in `Pipe`.

### ⚡ Performance Context: ZooPipe vs DataFrames

ZooPipe's executors are designed for **"Row-Complex"** workloads (hashing, API calls, Pydantic validation), where vectorized engines struggle.

- **Vectorized Engines (Polars/Pandas)**: Excellent for bulk math (SUM, AVG) but incur huge serialization overhead when you need a custom Python function (`map_elements`).
- **ZooPipe Executors**: Run Python logic in parallel native streaming batches. This makes them significantly faster (and lighter on RAM) for "chaotic" ETL tasks that require custom logic per row.

### Comparison at a Glance

| Level | Component | Scaling Type | Parallelism |
| :--- | :--- | :--- | :--- |
| **Cluster/Node** | `Engine` | Scaling Out | Python Processes / Distributed |
| **Process** | `Executor` | Scaling Up | Rust Native Threads |

For most high-throughput local workloads, the most powerful pattern is combining both: **A `PipeManager` with 4 workers, where each worker uses a `MultiThreadExecutor`**.

## When to Use Each Executor

### SingleThreadExecutor

Use when:
- Data processing is I/O-bound
- Processing simple transformations
- Debugging or development
- Order preservation is naturally maintained (single stream)

> [!NOTE]
> Even with `SingleThreadExecutor`, ZooPipe utilizes background threads for S3 sources via its **Hybrid I/O Strategy** to prevent GIL blocking during network I/O, while keeping the processing logic on the main thread.

### MultiThreadExecutor

Use when:
- Validation/transformation is CPU-intensive (e.g., complex Pydantic models)
- Maximum throughput is needed across multiple CPU cores
- Order of records in the output is not a critical requirement

> [!IMPORTANT]
> The `MultiThreadExecutor` processes batches in parallel. While throughput is significantly higher, the order of records in the destination may differ from the source.

## Performance Tuning

### Batch Size

Larger batch sizes reduce overhead but increase memory usage:

```python
# For small records, larger batches
executor = MultiThreadExecutor(max_workers=4, batch_size=5000)

# For large records, smaller batches
executor = MultiThreadExecutor(max_workers=4, batch_size=500)
```

### Thread Count

Match thread count to your workload:

```python
import os

# Use all available cores
executor = MultiThreadExecutor(max_workers=None)  # Auto-detect

# Conservative (50% of cores)
executor = MultiThreadExecutor(max_workers=os.cpu_count() // 2)

# Aggressive (2x cores for I/O-bound work)
executor = MultiThreadExecutor(max_workers=os.cpu_count() * 2)
```

## Complete Example with Schema

```python
from pydantic import BaseModel, ConfigDict
from zoopipe import CSVInputAdapter, CSVOutputAdapter, MultiThreadExecutor, Pipe

class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    age: int
    email: str

pipe = Pipe(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=CSVOutputAdapter("validated_users.csv"),
    error_output_adapter=CSVOutputAdapter("errors.csv"),
    schema_model=UserSchema,
    executor=MultiThreadExecutor(
        max_workers=8,
        batch_size=2000,
    ),
)

with pipe:
    pipe.wait()

report = pipe.report
print(f"✅ Success: {report.success_count}")
print(f"❌ Errors: {report.error_count}")
print(f"⚡ Speed: {report.items_per_second:.0f} items/sec")
```
