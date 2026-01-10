# Generator Output Adapter

The `GeneratorOutputAdapter` provides an iterator interface to processed entries, enabling real-time consumption of validation results as they complete.

## Features

- **Iterator Pattern**: Implements Python iterator protocol for streaming results
- **Queue-Based**: Uses thread-safe queue for communication between pipeand consumer
- **Backpressure**: Optional queue size limit for memory control
- **Real-Time Processing**: Consume results as they're validated, without waiting for completion
- **Sentinel Pattern**: Automatic completion signaling via stop sentinel

## Usage

### Basic Example

```python
from zoopipe import Pipe
from zoopipe.executor.multiprocessing import MultiprocessingExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.generator import GeneratorOutputAdapter
from pydantic import BaseModel

class UserSchema(BaseModel):
    name: str
    email: str
    age: int

output_adapter = GeneratorOutputAdapter()

pipe = Pipe(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=output_adapter,
    executor=MultiprocessingExecutor(UserSchema, num_workers=4)
)

report = pipe.start()

for entry in output_adapter:
    if entry["status"] == "VALIDATED":
        print(f"Validated: {entry['validated_data']}")
    else:
        print(f"Failed: {entry['errors']}")

report.wait()
```

### Real-Time Processing with Backpressure

```python
from zoopipe.output_adapter.generator import GeneratorOutputAdapter

output_adapter = GeneratorOutputAdapter(max_queue_size=100)

pipe = Pipe(
    input_adapter=input_adapter,
    output_adapter=output_adapter,
    executor=executor
)

report = pipe.start()

for entry in output_adapter:
    process_immediately(entry["validated_data"])

report.wait()
```

### Streaming to External System

```python
from zoopipe.output_adapter.generator import GeneratorOutputAdapter
import requests

output_adapter = GeneratorOutputAdapter(max_queue_size=50)

pipe = Pipe(
    input_adapter=input_adapter,
    output_adapter=output_adapter,
    executor=executor
)

report = pipe.start()

for entry in output_adapter:
    if entry["status"] == "VALIDATED":
        requests.post(
            "https://api.example.com/events",
            json=entry["validated_data"]
        )

report.wait()
```

### Progress Monitoring

```python
from zoopipe.output_adapter.generator import GeneratorOutputAdapter

output_adapter = GeneratorOutputAdapter()

pipe = Pipe(
    input_adapter=input_adapter,
    output_adapter=output_adapter,
    executor=executor
)

report = pipe.start()

processed = 0
for entry in output_adapter:
    processed += 1
    if processed % 100 == 0:
        print(f"Processed {processed} entries...")

report.wait()
print(f"Total: {processed} entries")
```

## Constructor Parameters

### `max_queue_size`
- **Type**: `int`
- **Default**: `0` (unlimited)
- **Description**: Maximum number of entries to buffer in the queue. Use `0` for unlimited queue size, or set a limit to control memory usage

## Methods

### `write(entry: EntryTypedDict) -> None`
Internal method called by Pipe to add entries to the queue (not typically called directly).

### `__iter__() -> Generator[EntryTypedDict, None, None]`
Returns the adapter itself as an iterator. Yields entries from the queue until the stop sentinel is received.

### `close() -> None`
Signals completion by adding the stop sentinel to the queue.

## Use Cases

### Real-Time Data Processing
Process validation results immediately without waiting for the entire pipeto complete:

```python
output_adapter = GeneratorOutputAdapter()

pipe = Pipe(
    input_adapter=large_input_adapter,
    output_adapter=output_adapter,
    executor=MultiprocessingExecutor(Schema, num_workers=8)
)

report = pipe.start()

for entry in output_adapter:
    send_to_kafka(entry["validated_data"])

report.wait()
```

### Progressive UI Updates
Update user interface as data is processed:

```python
output_adapter = GeneratorOutputAdapter(max_queue_size=20)

pipe = Pipe(
    input_adapter=input_adapter,
    output_adapter=output_adapter,
    executor=executor
)

report = pipe.start()

for entry in output_adapter:
    update_progress_bar()
    display_result(entry)

report.wait()
```

### Memory-Constrained Environments
Limit memory usage by processing and discarding results immediately:

```python
output_adapter = GeneratorOutputAdapter(max_queue_size=100)

pipe = Pipe(
    input_adapter=massive_input_adapter,
    output_adapter=output_adapter,
    executor=executor
)

report = pipe.start()

for entry in output_adapter:
    write_to_database(entry)

report.wait()
```

### Filtered Aggregation
Collect specific entries based on runtime criteria:

```python
output_adapter = GeneratorOutputAdapter()

pipe = Pipe(
    input_adapter=input_adapter,
    output_adapter=output_adapter,
    executor=executor
)

report = pipe.start()

high_value_items = []
for entry in output_adapter:
    if entry["validated_data"]["value"] > 1000:
        high_value_items.append(entry["validated_data"])

report.wait()
print(f"Found {len(high_value_items)} high-value items")
```

## Implementation Details

### Queue Mechanics
The adapter uses a `queue.Queue` internally to safely transfer entries from the Pipe background thread to the consumer iterator:

1. Pipe calls `write()` for each processed entry
2. Entries are added to the queue
3. Iterator `__iter__()` blocks on `queue.get()` until entries are available
4. When pipecompletes, `close()` adds a stop sentinel
5. Iterator exits when sentinel is received

### Thread Safety
The underlying `queue.Queue` is thread-safe, allowing the Pipe background thread to write while the main thread iterates.

### Backpressure
When `max_queue_size` is set:
- If the queue is full, `write()` blocks until space is available
- This creates backpressure on the executor, preventing memory overflow
- Useful when the consumer is slower than the producer

## Notes

- The iterator pattern means you must consume all entries before the `report.wait()` will complete
- If you don't iterate through all entries, the pipewill block indefinitely
- Setting `max_queue_size` too low may reduce throughput due to queue contention
- Use `max_queue_size=0` (unlimited) for maximum throughput when memory is not a constraint
- The adapter cannot be reused after iteration completes (stop sentinel has been sent)

## Comparison with Other Adapters

| Feature | GeneratorOutputAdapter | MemoryOutputAdapter | File Adapters |
|---------|----------------------|-------------------|--------------|
| Memory Usage | Low (queue only) | High (all entries) | Low (disk) |
| Access Pattern | Sequential (once) | Random (multiple) | External |
| Processing | Real-time | Post-completion | Post-completion |
| Backpressure | Yes (configurable) | No | No |
| Use Case | Streaming | Testing | Persistence |
