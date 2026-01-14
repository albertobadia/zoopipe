# JSON Adapters

ZooPipe provides Rust-based JSON readers and writers optimized for both JSONL (newline-delimited JSON) and JSON array formats.

## JSONInputAdapter

Read JSONL files with automatic line-by-line streaming.

### Basic Usage

```python
from zoopipe import CSVOutputAdapter, JSONInputAdapter, Pipe

pipe = Pipe(
    input_adapter=JSONInputAdapter("data.jsonl"),
    output_adapter=CSVOutputAdapter("output.csv"),
)
```

### Parameters

- **source** (`str | pathlib.Path`): Path to the JSONL file to read
  - Each line must contain a valid JSON object
  - Blank lines are skipped automatically

### JSONL Format

The adapter expects newline-delimited JSON (JSONL) format:

```jsonl
{"user_id": "1", "name": "Alice", "email": "alice@example.com"}
{"user_id": "2", "name": "Bob", "email": "bob@example.com"}
{"user_id": "3", "name": "Charlie", "email": "charlie@example.com"}
```

Each line is parsed independently, enabling streaming of arbitrarily large files.

### Performance Characteristics

- **Streaming**: Constant memory usage regardless of file size
- **Line-by-line Parsing**: Each JSON object is parsed independently
- **Error Handling**: Invalid JSON lines trigger clear error messages
- **Throughput**: Similar to CSV (~200k+ rows/s)

## JSONOutputAdapter

Write data to JSON files in either JSONL or pretty-printed array format.

### Basic Usage (JSONL)

```python
from zoopipe import CSVInputAdapter, JSONOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("data.csv"),
    output_adapter=JSONOutputAdapter("output.jsonl", format="jsonl"),
)
```

### Parameters

- **output** (`str | pathlib.Path`): Path to the JSON file to write
  - Parent directories are automatically created if they don't exist

- **format** (`str`, default=`"array"`): Output format
  - `"jsonl"`: Newline-delimited JSON (one object per line)
  - `"array"`: JSON array with all objects in a single array

- **indent** (`int | None`, default=`None`): Indentation level for pretty-printing
  - `None`: Compact, single-line output
  - `2`, `4`, etc.: Pretty-printed with specified indent

### JSONL Format (Recommended)

```python
adapter = JSONOutputAdapter("output.jsonl", format="jsonl")
```

Output:
```jsonl
{"user_id":"1","name":"Alice","email":"alice@example.com"}
{"user_id":"2","name":"Bob","email":"bob@example.com"}
```

JSONL is ideal for:
- Large datasets (streaming-friendly)
- Log files and data pipelines
- Line-by-line processing tools

### Array Format

```python
adapter = JSONOutputAdapter("output.json", format="array")
```

Output:
```json
[{"user_id":"1","name":"Alice","email":"alice@example.com"},{"user_id":"2","name":"Bob","email":"bob@example.com"}]
```

### Pretty-Printed Array

```python
adapter = JSONOutputAdapter("output.json", format="array", indent=2)
```

Output:
```json
[
  {
    "user_id": "1",
    "name": "Alice",
    "email": "alice@example.com"
  },
  {
    "user_id": "2",
    "name": "Bob",
    "email": "bob@example.com"
  }
]
```

## Complete Examples

### CSV to JSONL Conversion

```python
import time
from pydantic import BaseModel, ConfigDict
from zoopipe import CSVInputAdapter, JSONOutputAdapter, MultiThreadExecutor, Pipe

class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str

pipe = Pipe(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=JSONOutputAdapter("users.jsonl", format="jsonl"),
    schema_model=UserSchema,
    executor=MultiThreadExecutor(max_workers=4),
)

pipe.start()

while not pipe.report.is_finished:
    print(f"Processed: {pipe.report.total_processed} rows")
    time.sleep(0.5)
```

### JSONL to Pretty JSON

```python
from zoopipe import JSONInputAdapter, JSONOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=JSONInputAdapter("data.jsonl"),
    output_adapter=JSONOutputAdapter("data.json", format="array", indent=2),
)

with pipe:
    pipe.wait()
```

### JSONL Validation and Filtering

```python
from pydantic import BaseModel, field_validator

class ValidatedRecord(BaseModel):
    id: str
    value: float

    @field_validator('value')
    def positive_value(cls, v):
        if v <= 0:
            raise ValueError("value must be positive")
        return v

pipe = Pipe(
    input_adapter=JSONInputAdapter("raw_data.jsonl"),
    output_adapter=JSONOutputAdapter("valid_data.jsonl", format="jsonl"),
    error_output_adapter=JSONOutputAdapter("errors.jsonl", format="jsonl"),
    schema_model=ValidatedRecord,
)
```

## Format Comparison

| Feature | JSONL | Array | Pretty Array |
|---------|-------|-------|--------------|
| File Size | Smallest | Medium | Largest |
| Streamable | ✅ Yes | ❌ No | ❌ No |
| Human-Readable | ⚠️ Moderate | ✅ Yes | ✅✅ Very |
| Memory Usage | Constant | High* | High* |
| Line-by-Line Tools | ✅ Yes | ❌ No | ❌ No |
| API Response | ❌ No | ✅ Yes | ✅ Yes |

\* Array formats load entire file into memory

## Best Practices

### For Reading
1. **Validate JSONL Format**: Ensure each line contains a complete JSON object
2. **Handle Encoding**: Files should be UTF-8 encoded
3. **Use Error Output**: Route malformed JSON to error output for debugging
4. **Large Files**: JSONL format enables streaming of arbitrarily large datasets

### For Writing
1. **Choose JSONL for Data Pipelines**: Streaming-friendly and most efficient
2. **Choose Array for APIs**: Better for small datasets and web services
3. **Use Indent for Debugging**: Pretty-print during development, compact in production
4. **Field Ordering**: JSON objects have consistent field ordering (sorted alphabetically)

## Common Patterns

### API Data Export

```python
from zoopipe import JSONInputAdapter, JSONOutputAdapter, Pipe, SQLInputAdapter

pipe = Pipe(
    input_adapter=SQLInputAdapter(
        "postgresql://user:pass@localhost/db",
        query="SELECT id, name, email FROM users LIMIT 1000"
    ),
    output_adapter=JSONOutputAdapter("api_export.json", format="array", indent=2),
)
```

### Log File Processing

```python
from zoopipe import CSVOutputAdapter, JSONInputAdapter, Pipe

pipe = Pipe(
    input_adapter=JSONInputAdapter("application.log.jsonl"),
    output_adapter=CSVOutputAdapter("log_summary.csv"),
)
```

### Data Migration

```python
from zoopipe import JSONInputAdapter, MultiThreadExecutor, Pipe, SQLOutputAdapter

pipe = Pipe(
    input_adapter=JSONInputAdapter("legacy_data.jsonl"),
    output_adapter=SQLOutputAdapter(
        "sqlite:///new_database.db",
        table_name="migrated_data",
        mode="replace"
    ),
    executor=MultiThreadExecutor(max_workers=4),
)
```

## Error Handling

```python
try:
    pipe = Pipe(
        input_adapter=JSONInputAdapter("data.jsonl"),
        output_adapter=JSONOutputAdapter("output.jsonl", format="jsonl"),
    )
    pipe.start()
except Exception as e:
    print(f"Error: {e}")
```

Common errors:
- **Invalid JSON**: Malformed JSON object on a specific line
- **File Not Found**: Input file doesn't exist
- **Permission Denied**: Can't read input or write output
- **Encoding Error**: Non-UTF-8 characters in file

## Performance Tips

1. **JSONL vs Array**: JSONL is 2-3x faster for large datasets
2. **Compact Output**: Avoid `indent` in production for better performance
3. **Batch Size**: Default 2000 rows works well for most JSONL files
4. **Multi-Threading**: Use `MultiThreadExecutor` for files > 50MB
5. **Memory**: JSONL maintains constant memory usage; Array format loads entire output into memory

## Integration Examples

### With Pandas

```python
import pandas as pd
from zoopipe import JSONInputAdapter, JSONOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=JSONInputAdapter("input.jsonl"),
    output_adapter=JSONOutputAdapter("filtered.jsonl", format="jsonl"),
)

with pipe:
    pipe.wait()

df = pd.read_json("filtered.jsonl", lines=True)
print(df.head())
```

### With jq (Command Line)

```bash
# Process JSONL output with jq
cat output.jsonl | jq '.email'

# Filter and transform
cat output.jsonl | jq 'select(.age > 18) | {id, name}'
```

### With Streaming APIs

```python
from zoopipe import JSONInputAdapter, JSONOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=JSONInputAdapter("stream_dump.jsonl"),
    output_adapter=JSONOutputAdapter("processed.jsonl", format="jsonl"),
)
```
