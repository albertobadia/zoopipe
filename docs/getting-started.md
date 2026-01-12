# Getting Started with Pipe

## Installation

### Using uv (Recommended)

If you are using [uv](https://github.com/astral-sh/uv):

```bash
uv add zoopipe
```

### Using pip

```bash
pip install zoopipe
```

## Requirements

- Python >= 3.13
- Pydantic >= 2.12.5

## Basic Example

Here is how you can process a CSV file, validate it against a model, and save the results:

```python
from pydantic import BaseModel, ConfigDict
from zoopipe.core import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter

class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    last_name: str
    age: int

pipe = Pipe(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=CSVOutputAdapter("processed_users.csv"),
    error_output_adapter=CSVOutputAdapter("errors.csv"),
    executor=SyncFifoExecutor(UserSchema),
)

# Wait for completion
with pipe:
    report = pipe.start()
    report.wait()

print(f"Processed: {report.total_processed}")
print(f"Success: {report.success_count}")
print(f"Errors: {report.error_count}")
print(f"Duration: {report.duration:.2f}s")
```

## Lifecycle Management
It is recommended to use the `Pipe` instance as a context manager (using `with`). This ensures that resources (background threads, executors) are properly cleaned up even if an error occurs. (However, if you forget, Pipe will also try to shut down gracefully when the object is deleted).

```python
with Pipe(...) as flow:
    report = pipe.start()
    report.wait()
# Resources are automatically cleaned up here
```

## Performance

ZooPipe uses native Rust implementations for CSV and JSON adapters, providing:
- **High throughput**: Native parsers and writers optimized for speed
- **Low memory usage**: Efficient streaming processing of large files
- **Batch optimization**: Optimized batch writing reduces overhead

The native implementation is completely transparent to users - no configuration changes needed. Simply use `CSVInputAdapter`, `CSVOutputAdapter`, `JSONInputAdapter`, and `JSONOutputAdapter` as shown in the examples above.

## Next Steps
- Learn about [different executors](executors.md) for parallel processing
- Explore [input and output adapters](adapters.md)
- Check out more [examples](examples.md)
- Understand the [architecture and design](RFC.md)
