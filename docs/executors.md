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

## When to Use Each Executor

### SingleThreadExecutor

Use when:
- Data processing is I/O-bound
- Processing simple transformations
- Debugging or development
- Order preservation is critical

### MultiThreadExecutor

Use when:
- Validation/transformation is CPU-intensive
- Large datasets with complex Pydantic models
- Multiple independent transformations
- Maximum throughput is needed

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
