# Memory Output Adapter

The `MemoryOutputAdapter` stores all processed entries in an in-memory list, making it ideal for testing, debugging, and small-scale data analysis.

## Features

- **Simple Storage**: Accumulates all entries in a Python list
- **No I/O Overhead**: Perfect for unit tests and benchmarks
- **Easy Access**: Direct access to results via `.results` attribute
- **No Configuration**: Zero-config adapter, just instantiate and use

## Usage

### Basic Example

```python
from zoopipe import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.json import JSONInputAdapter
from zoopipe.output_adapter.memory import MemoryOutputAdapter
from pydantic import BaseModel

class ProductSchema(BaseModel):
    name: str
    price: float

output_adapter = MemoryOutputAdapter()

pipe = Pipe(
    input_adapter=JSONInputAdapter("products.json"),
    output_adapter=output_adapter,
    executor=SyncFifoExecutor(ProductSchema)
)

with pipe:
    report = pipe.start()
    report.wait()

print(f"Processed {len(output_adapter.results)} items")
for entry in output_adapter.results:
    print(entry["validated_data"])
```

### Unit Testing

```python
from zoopipe.output_adapter.memory import MemoryOutputAdapter

def test_validation_flow():
    output_adapter = MemoryOutputAdapter()
    
    pipe = Pipe(
        input_adapter=test_input_adapter,
        output_adapter=output_adapter,
        executor=SyncFifoExecutor(MySchema)
    )
    
    with pipe:
        report = pipe.start()
        report.wait()
    
    assert len(output_adapter.results) == 10
    assert all(entry["status"] == "VALIDATED" for entry in output_adapter.results)
```

### Analyzing Results

```python
from zoopipe.output_adapter.memory import MemoryOutputAdapter

output_adapter = MemoryOutputAdapter()

pipe = Pipe(
    input_adapter=input_adapter,
    output_adapter=output_adapter,
    executor=executor
)

with pipe:
    report = pipe.start()
    report.wait()

validated_data = [
    entry["validated_data"] 
    for entry in output_adapter.results 
    if entry["status"] == "VALIDATED"
]

print(f"Success rate: {len(validated_data) / len(output_adapter.results) * 100:.1f}%")
```

## Constructor

The `MemoryOutputAdapter` requires no parameters:

```python
from zoopipe.output_adapter.memory import MemoryOutputAdapter

output_adapter = MemoryOutputAdapter()
```

## Attributes

### `results`
- **Type**: `list[EntryTypedDict]`
- **Description**: List containing all entries that have been written to the adapter
- **Access**: Read-only (do not modify directly during pipeexecution)

## Entry Structure

Each entry in the `results` list is an `EntryTypedDict` containing:

```python
{
    "id": uuid.UUID,
    "position": int,
    "status": EntryStatus,
    "raw_data": dict[str, Any],
    "validated_data": dict[str, Any] | None,
    "errors": list[str],
    "metadata": dict[str, Any]
}
```

## Use Cases

### Unit Testing
Perfect for testing validation logic without I/O overhead:

```python
def test_email_validation():
    adapter = MemoryOutputAdapter()
    
    pipe = Pipe(
        input_adapter=test_data_adapter,
        output_adapter=adapter,
        executor=SyncFifoExecutor(EmailSchema)
    )
    
    with pipe:
        report = pipe.start()
        report.wait()
    
    for entry in adapter.results:
        assert "@" in entry["validated_data"]["email"]
```

### Debugging
Inspect intermediate results during development:

```python
output_adapter = MemoryOutputAdapter()

pipe = Pipe(
    input_adapter=input_adapter,
    output_adapter=output_adapter,
    executor=executor
)

with pipe:
    report = pipe.start()
    report.wait()

import pprint
pprint.pprint(output_adapter.results[:5])
```

### Small Dataset Analysis
Useful for prototyping and exploration with small datasets:

```python
from zoopipe.output_adapter.memory import MemoryOutputAdapter
import statistics

output_adapter = MemoryOutputAdapter()

pipe = Pipe(
    input_adapter=JSONInputAdapter("sample_data.json"),
    output_adapter=output_adapter,
    executor=SyncFifoExecutor(DataSchema)
)

with pipe:
    report = pipe.start()
    report.wait()

ages = [entry["validated_data"]["age"] for entry in output_adapter.results]
print(f"Average age: {statistics.mean(ages):.1f}")
```

## Notes

- All entries are kept in memory, so this adapter is not suitable for large datasets
- The `results` list grows with each processed entry
- Memory is only freed when the adapter object is garbage collected
- For large datasets, use file-based adapters (CSV, JSON, Arrow) instead

## Performance Considerations

The `MemoryOutputAdapter` has minimal overhead:
- No file I/O operations
- No serialization/deserialization
- Direct list append operations

This makes it ideal for benchmarking executor performance without adapter overhead.
