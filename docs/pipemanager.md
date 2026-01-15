# PipeManager

`PipeManager` orchestrates multiple `Pipe` instances to run in parallel, each in its own process. This enables true parallelism for running multiple independent data processing workflows concurrently.

## When to Use PipeManager

Use `PipeManager` when you need to:

- Process multiple independent data sources simultaneously
- Run the same pipeline on different data partitions in parallel
- Maximize CPU utilization across multiple cores
- Orchestrate complex multi-stage workflows

## Basic Usage

```python
from zoopipe import Pipe, PipeManager, CSVInputAdapter, JSONOutputAdapter
from pydantic import BaseModel

class UserSchema(BaseModel):
    user_id: str
    username: str
    email: str

manager = PipeManager(
    pipes=[
        Pipe(
            input_adapter=CSVInputAdapter("data_part_1.csv"),
            output_adapter=JSONOutputAdapter("output_1.jsonl"),
            schema_model=UserSchema,
        ),
        Pipe(
            input_adapter=CSVInputAdapter("data_part_2.csv"),
            output_adapter=JSONOutputAdapter("output_2.jsonl"),
            schema_model=UserSchema,
        ),
        Pipe(
            input_adapter=CSVInputAdapter("data_part_3.csv"),
            output_adapter=JSONOutputAdapter("output_3.jsonl"),
            schema_model=UserSchema,
        ),
    ]
)

manager.start()
manager.wait()

print(f"Total processed: {manager.report.total_processed}")
```

## Monitoring Progress

PipeManager provides both aggregated and per-pipe reporting:

```python
import time

manager.start()

while not manager.report.is_finished:
    print(f"Total: {manager.report.total_processed} | "
          f"Speed: {manager.report.items_per_second:.2f} rows/s | "
          f"RAM: {manager.report.ram_bytes / 1024 / 1024:.2f} MB")
    
    for i, pipe_report in enumerate(manager.pipe_reports):
        print(f"  Pipe {i}: {pipe_report.total_processed} processed, "
              f"finished: {pipe_report.is_finished}")
    
    time.sleep(1)

print(manager.report)
```

## Context Manager Support

PipeManager can be used as a context manager for automatic resource cleanup:

```python
with PipeManager(pipes=[pipe1, pipe2, pipe3]) as manager:
    while not manager.report.is_finished:
        print(f"Progress: {manager.report.total_processed}")
        time.sleep(1)
```

## API Reference

### PipeManager

#### Constructor

```python
PipeManager(pipes: list[Pipe])
```

Parameters:
- `pipes`: List of `Pipe` instances to run in parallel

#### Methods

##### `start() -> None`

Starts all pipes in separate processes. Raises `RuntimeError` if already running.

##### `wait(timeout: float | None = None) -> bool`

Waits for all pipes to complete.

Parameters:
- `timeout`: Maximum time to wait in seconds (optional)

Returns:
- `True` if all pipes finished, `False` if timeout occurred

##### `shutdown(timeout: float = 5.0) -> None`

Gracefully shuts down all running pipes.

Parameters:
- `timeout`: Maximum time to wait for graceful shutdown before forcing termination

##### `get_pipe_report(index: int) -> PipeReport`

Gets the report for a specific pipe.

Parameters:
- `index`: Zero-based index of the pipe

Returns:
- `PipeReport` with metrics for the specified pipe

#### Properties

##### `pipes -> list[Pipe]`

Returns the list of pipes managed by this instance.

##### `pipe_count -> int`

Returns the number of pipes being managed.

##### `is_running -> bool`

Returns `True` if any pipe is still running.

##### `pipe_reports -> list[PipeReport]`

Returns a list of `PipeReport` objects, one for each pipe.

##### `report -> FlowReport`

Returns an aggregated `FlowReport` combining metrics from all pipes.

### PipeReport

Individual pipe report with the following fields:

- `pipe_index`: Zero-based index of the pipe
- `total_processed`: Total records processed by this pipe
- `success_count`: Number of successfully processed records
- `error_count`: Number of failed records
- `ram_bytes`: Current RAM usage in bytes
- `is_finished`: Whether the pipe has completed
- `has_error`: Whether the pipe encountered an error
- `is_alive`: Whether the pipe process is still alive

## Process Model

Each pipe runs in its own process using Python's `multiprocessing` module with the `fork` start method. This provides:

- **True parallelism**: Each pipe runs on a separate CPU core
- **Memory isolation**: Each pipe has its own memory space
- **Fault isolation**: If one pipe crashes, others continue running

> [!NOTE]
> On macOS, the default multiprocessing start method is `spawn`, but ZooPipe forces `fork` for better performance. This is safe for ZooPipe's use case but may cause issues if you're using libraries that aren't fork-safe.

## Performance Considerations

### When Parallel Execution Helps

- **I/O-bound workloads**: Reading from or writing to multiple files/databases simultaneously
- **Multiple data sources**: Processing partitioned data in parallel
- **Independent pipelines**: Running completely separate data transformations

### When Parallel Execution May Not Help

- **Single large file**: Use a single pipe with `MultiThreadExecutor` instead
- **Shared resources**: Multiple pipes writing to the same database may cause contention
- **Memory-constrained systems**: Each process has its own memory overhead

## Best Practices

1. **Partition your data appropriately**: Split large datasets into balanced chunks for better load distribution

2. **Monitor individual pipes**: Use `pipe_reports` to identify bottlenecks or failed pipes

3. **Handle errors gracefully**: Check `has_error` flag in individual pipe reports to detect failures

4. **Use context managers**: Ensure proper cleanup with `with` statement

5. **Consider memory usage**: Each pipe process duplicates Python interpreter and loaded modules

## Examples

### Processing Partitioned Data

```python
import glob

pipes = []
for csv_file in glob.glob("data_parts/*.csv"):
    output_file = csv_file.replace("data_parts", "output").replace(".csv", ".jsonl")
    pipes.append(
        Pipe(
            input_adapter=CSVInputAdapter(csv_file),
            output_adapter=JSONOutputAdapter(output_file),
            schema_model=UserSchema,
        )
    )

with PipeManager(pipes=pipes) as manager:
    manager.wait()
```

### Different Pipelines in Parallel

```python
manager = PipeManager(
    pipes=[
        Pipe(
            input_adapter=CSVInputAdapter("users.csv"),
            output_adapter=DuckDBOutputAdapter("analytics.duckdb", table_name="users"),
            schema_model=UserSchema,
        ),
        Pipe(
            input_adapter=CSVInputAdapter("orders.csv"),
            output_adapter=DuckDBOutputAdapter("analytics.duckdb", table_name="orders"),
            schema_model=OrderSchema,
        ),
        Pipe(
            input_adapter=CSVInputAdapter("products.csv"),
            output_adapter=DuckDBOutputAdapter("analytics.duckdb", table_name="products"),
            schema_model=ProductSchema,
        ),
    ]
)

manager.start()
manager.wait()
```

## Related Documentation

- [Executors Guide](executors.md) - For parallelizing a single pipe
- [CSV Adapters](csv.md) - Common use case for parallel processing
