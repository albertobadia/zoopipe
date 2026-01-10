# File Partitioner

The `FilePartitioner` is a specialized input adapter that partitions large files into byte ranges, enabling distributed processing of files too large to fit in memory or that benefit from parallel reading.

## Features

- **Byte-Range Partitioning**: Splits files into ranges based on byte offsets
- **Distributed Processing**: Each partition can be processed by a different worker
- **Memory Efficient**: Only metadata is passed, not the entire file
- **Flexible Partition Count**: Automatically adjusts if file is smaller than expected
- **Worker Integration**: Designed for use with multiprocessing and distributed executors

## Usage

### Basic Example

```python
from zoopipe import Pipe
from zoopipe.executor.multiprocessing import MultiprocessingExecutor
from zoopipe.input_adapter.partitioner import FilePartitioner
from zoopipe.output_adapter.memory import MemoryOutputAdapter
from pydantic import BaseModel

class LineModel(BaseModel):
    model_config = {"extra": "allow"}

pipe = Pipe(
    input_adapter=FilePartitioner("large_file.jsonl", num_partitions=4),
    output_adapter=MemoryOutputAdapter(),
    executor=MultiprocessingExecutor(LineModel, max_workers=4)
)

with pipe:
    report = pipe.start()
    report.wait()
```

### With Custom Hook for Processing

```python
from zoopipe import Pipe, PartitionedReaderHook
from zoopipe.input_adapter.partitioner import FilePartitioner
from zoopipe.executor.multiprocessing import MultiprocessingExecutor

class JsonlReaderHook(PartitionedReaderHook):
    def process_line(self, line: bytes, store):
        pass

pipe = Pipe(
    input_adapter=FilePartitioner("data.jsonl", num_partitions=8),
    output_adapter=MemoryOutputAdapter(),
    executor=MultiprocessingExecutor(Schema, max_workers=8),
    pre_validation_hooks=[JsonlReaderHook()]
)
```

### With Ray Executor

```python
from zoopipe import Pipe
from zoopipe.executor.ray import RayExecutor
from zoopipe.input_adapter.partitioner import FilePartitioner

pipe = Pipe(
    input_adapter=FilePartitioner("huge_file.csv", num_partitions=16),
    output_adapter=output_adapter,
    executor=RayExecutor(Schema, num_workers=16)
)
```

## Constructor Parameters

### `file_path`
- **Type**: `str`
- **Required**: Yes
- **Description**: Path to the file to partition

### `num_partitions`
- **Type**: `int`
- **Required**: Yes
- **Description**: Number of partitions to create. Actual partitions may be fewer if file is smaller than expected

## How It Works

The `FilePartitioner` divides a file into equal byte ranges:

1. **File Size Detection**: Determines total file size in bytes
2. **Range Calculation**: Divides file into `num_partitions` equal ranges
3. **Entry Generation**: Creates one entry per partition with metadata:
   - `path`: Original file path
   - `start`: Starting byte offset
   - `end`: Ending byte offset
   - `partition_id`: Partition number (0-indexed)

Each worker receives partition metadata and can read its assigned byte range independently.

## Entry Structure

Each partition generates an entry with the following `raw_data`:

```python
{
    "path": "/path/to/file.txt",
    "start": 0,
    "end": 1048576,
    "partition_id": 0
}
```

## Use Cases

### Processing Large JSONL Files

```python
from zoopipe import Pipe, PartitionedReaderHook
from zoopipe.input_adapter.partitioner import FilePartitioner
from zoopipe.executor.multiprocessing import MultiprocessingExecutor

class JsonlProcessor(PartitionedReaderHook):
    def process_line(self, line: bytes, store):
        import json
        try:
            data = json.loads(line)
            store.append(data)
        except json.JSONDecodeError:
            pass

pipe = Pipe(
    input_adapter=FilePartitioner("events.jsonl", num_partitions=8),
    output_adapter=MemoryOutputAdapter(),
    executor=MultiprocessingExecutor(Schema, max_workers=8),
    pre_validation_hooks=[JsonlProcessor()]
)
```

### Distributed CSV Processing

```python
from zoopipe import Pipe, PartitionedReaderHook
from zoopipe.input_adapter.partitioner import FilePartitioner
from zoopipe.executor.ray import RayExecutor

class CsvPartitionProcessor(PartitionedReaderHook):
    def process_partition(self, file_path: str, start: int, end: int, store):
        import csv
        with open(file_path, 'rb') as f:
            f.seek(start)
            data = f.read(end - start)
            
        for row in csv.DictReader(data.decode().splitlines()):
            store.append(row)

pipe = Pipe(
    input_adapter=FilePartitioner("massive.csv", num_partitions=32),
    output_adapter=output_adapter,
    executor=RayExecutor(Schema, num_workers=32),
    pre_validation_hooks=[CsvPartitionProcessor()]
)
```

### Log File Analysis

```python
from zoopipe import Pipe
from zoopipe.input_adapter.partitioner import FilePartitioner
from zoopipe.executor.multiprocessing import MultiprocessingExecutor

pipe = Pipe(
    input_adapter=FilePartitioner("app.log", num_partitions=4),
    output_adapter=output_adapter,
    executor=MultiprocessingExecutor(LogSchema, max_workers=4),
    pre_validation_hooks=[LogParserHook()]
)
```

## Integration with Hooks

The `FilePartitioner` is designed to work with `PartitionedReaderHook`:

```python
from zoopipe import PartitionedReaderHook

class MyPartitionHook(PartitionedReaderHook):
    def process_line(self, line: bytes, store):
        # Process each line from the partition
        pass
    
    def process_partition(self, file_path: str, start: int, end: int, store):
        # Or process the entire partition at once
        with open(file_path, 'rb') as f:
            f.seek(start)
            chunk = f.read(end - start)
        
        for line in chunk.splitlines():
            self.process_line(line, store)
```

## Performance Considerations

### Optimal Partition Count

- **Multiprocessing**: Set `num_partitions` equal to `num_workers`
- **Ray/Dask**: Can use more partitions than workers for better load balancing
- **File Size**: Smaller files may result in fewer actual partitions

### Memory Usage

- Only partition metadata is kept in memory initially
- Each worker loads only its assigned byte range
- Ideal for files larger than available memory

### Line-Boundary Handling

When using text files with line-based data (JSONL, CSV, logs):
- Partitions are byte ranges and may split lines
- Use `PartitionedReaderHook` to handle partial lines at boundaries
- The hook provides utilities for proper line processing

## Notes

- If the file size is smaller than `num_partitions`, actual partitions will be reduced
- Empty files produce zero partitions
- Each partition gets a unique `partition_id` starting from 0
- The last partition may be slightly larger to account for remainder bytes
- File must exist at the specified path or `FileNotFoundError` is raised

## Example Output

For a 1GB file with `num_partitions=4`:

```
Partition 0: bytes 0 to 268,435,456
Partition 1: bytes 268,435,456 to 536,870,912
Partition 2: bytes 536,870,912 to 805,306,368
Partition 3: bytes 805,306,368 to 1,073,741,824
```

## Related Adapters

- [CSVInputAdapter](csv-input.md) - For small to medium CSV files
- [JSONInputAdapter](json-input.md) - For small to medium JSON files
- [ArrowInputAdapter](arrow-input.md) - For Parquet files with built-in partitioning

## When to Use

✅ **Use FilePartitioner when:**
- Processing files larger than memory
- Distributing work across multiple workers
- Need byte-level control over file reading
- Working with line-delimited formats (JSONL, logs)

❌ **Don't use when:**
- File is small and fits easily in memory
- Format has built-in partitioning (use ArrowInputAdapter for Parquet)
- Need random access to file contents
