# Getting Started with FlowSchema

## Installation

### Using uv (Recommended)

If you are using [uv](https://github.com/astral-sh/uv):

```bash
uv add flowschema
```

### Using pip

```bash
pip install flowschema
```

## Requirements

- Python >= 3.13
- Pydantic >= 2.12.5

## Basic Example

Here is how you can process a CSV file, validate it against a model, and save the results:

```python
from pydantic import BaseModel, ConfigDict
from flowschema.core import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter

class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    last_name: str
    age: int

flow = FlowSchema(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=CSVOutputAdapter("processed_users.csv"),
    error_output_adapter=CSVOutputAdapter("errors.csv"),
    executor=SyncFifoExecutor(UserSchema),
)

# Wait for completion
with flow:
    report = flow.start()
    report.wait()

print(f"Processed: {report.total_processed}")
print(f"Success: {report.success_count}")
print(f"Errors: {report.error_count}")
print(f"Duration: {report.duration:.2f}s")
```

## Lifecycle Management
It is recommended to use the `FlowSchema` instance as a context manager (using `with`). This ensures that resources (background threads, executors) are properly cleaned up even if an error occurs. (However, if you forget, FlowSchema will also try to shut down gracefully when the object is deleted).

```python
with FlowSchema(...) as flow:
    report = flow.start()
    report.wait()
# Resources are automatically cleaned up here
```

## Next Steps
- Learn about [different executors](executors.md) for parallel processing
- Explore [input and output adapters](adapters.md)
- Check out more [examples](examples.md)
- Understand the [architecture and design](RFC.md)
