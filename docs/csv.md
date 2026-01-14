# CSV Adapters

ZooPipe provides ultra-fast CSV readers and writers built entirely in Rust. These adapters are optimized for maximum throughput and minimal memory overhead.

## CSVInputAdapter

Read CSV files with configurable delimiters, quotes, and field handling.

### Basic Usage

```python
from zoopipe import CSVInputAdapter, JSONOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("data.csv"),
    output_adapter=JSONOutputAdapter("output.jsonl", format="jsonl"),
)
```

### Parameters

- **source** (`str | pathlib.Path`): Path to the CSV file to read

- **delimiter** (`str`, default=`","`): Field delimiter character
  - Common values: `,` (comma), `\t` (tab), `;` (semicolon), `|` (pipe)

- **quotechar** (`str`, default=`"\""`): Quote character for escaping fields
  - Used when fields contain the delimiter or newlines

- **skip_rows** (`int`, default=`0`): Number of rows to skip before reading headers
  - Useful for skipping metadata or comment lines at the top of the file

- **fieldnames** (`list[str] | None`, default=`None`): Custom field names
  - If `None`, uses first row as headers
  - If provided, treats first row as data

- **generate_ids** (`bool`, default=`True`): Whether to generate UUIDs for each record

### Custom Delimiters

```python
tab_adapter = CSVInputAdapter(
    "data.tsv",
    delimiter="\t"
)

semicolon_adapter = CSVInputAdapter(
    "data.csv",
    delimiter=";"
)
```

### Skip Header Rows

```python
adapter = CSVInputAdapter(
    "data.csv",
    skip_rows=3
)
```

### Custom Field Names

```python
adapter = CSVInputAdapter(
    "data.csv",
    fieldnames=["id", "name", "email", "age"]
)
```

## CSVOutputAdapter

Write data to CSV files with high performance batch operations.

### Basic Usage

```python
from zoopipe import CSVInputAdapter, CSVOutputAdapter, Pipe, JSONInputAdapter

pipe = Pipe(
    input_adapter=JSONInputAdapter("data.jsonl"),
    output_adapter=CSVOutputAdapter("output.csv"),
)
```

### Parameters

- **output** (`str | pathlib.Path`): Path to the CSV file to write
  - Parent directories are automatically created if they don't exist

- **delimiter** (`str`, default=`","`): Field delimiter character

- **quotechar** (`str`, default=`"\""`): Quote character for escaping fields

- **fieldnames** (`list[str] | None`, default=`None`): Explicit field ordering
  - If `None`, field names are inferred from the first record and sorted alphabetically
  - If provided, only these fields are written in the specified order

### Custom Field Order

```python
adapter = CSVOutputAdapter(
    "output.csv",
    fieldnames=["user_id", "username", "email", "created_at"]
)
```

This ensures the CSV columns appear in the exact order specified, regardless of the order in the input data.

## Complete Example

### CSV Processing with Validation

```python
import time
from pydantic import BaseModel, ConfigDict, EmailStr
from zoopipe import CSVInputAdapter, CSVOutputAdapter, MultiThreadExecutor, Pipe

class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: EmailStr
    age: int

pipe = Pipe(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=CSVOutputAdapter(
        "validated_users.csv",
        fieldnames=["user_id", "username", "email", "age"]
    ),
    error_output_adapter=CSVOutputAdapter("errors.csv"),
    schema_model=UserSchema,
    executor=MultiThreadExecutor(max_workers=8, batch_size=2000),
)

pipe.start()

while not pipe.report.is_finished:
    print(
        f"Processed: {pipe.report.total_processed} | "
        f"Speed: {pipe.report.items_per_second:.2f} rows/s | "
        f"Errors: {pipe.report.total_errors}"
    )
    time.sleep(0.5)

print(f"Finished! Processed {pipe.report.total_processed} records")
```

### TSV to CSV Conversion

```python
from zoopipe import CSVInputAdapter, CSVOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("data.tsv", delimiter="\t"),
    output_adapter=CSVOutputAdapter("data.csv", delimiter=","),
)

with pipe:
    pipe.wait()
```

## Performance Characteristics

### Reading
- **100% Rust Implementation**: Zero Python overhead during parsing
- **Streaming**: Constant memory usage regardless of file size
- **Type Handling**: All fields are read as strings (type conversion handled by Pydantic)
- **Quote Handling**: Proper RFC 4180 CSV escaping and unescaping

### Writing
- **Batch Operations**: Efficient buffered writes
- **Automatic Quoting**: Fields containing delimiters or newlines are automatically quoted
- **Directory Creation**: Parent directories are created automatically
- **Field Ordering**: Consistent column ordering via sorted or explicit fieldnames

## Best Practices

### For Reading
1. Use `skip_rows` to ignore metadata lines at the top of files
2. Specify `fieldnames` explicitly if your CSV doesn't have headers
3. Use the default delimiter (`,`) when possible for maximum performance
4. Let Pydantic handle type conversion instead of pre-processing

### For Writing
1. Specify `fieldnames` explicitly for consistent column ordering
2. Use `MultiThreadExecutor` for large datasets
3. Choose appropriate `delimiter` based on your data (avoid delimiters that appear in values)
4. Use the error output to route invalid records for later review

## Common Patterns

### Data Cleaning Pipeline

```python
from pydantic import BaseModel, field_validator

class CleanedData(BaseModel):
    name: str
    email: str

    @field_validator('email')
    def lowercase_email(cls, v):
        return v.lower()

pipe = Pipe(
    input_adapter=CSVInputAdapter("raw_data.csv"),
    output_adapter=CSVOutputAdapter("cleaned_data.csv"),
    error_output_adapter=CSVOutputAdapter("rejected_data.csv"),
    schema_model=CleanedData,
)
```

### Merging CSV Files

```python
from pathlib import Path
from zoopipe import CSVInputAdapter, CSVOutputAdapter, Pipe

for i, csv_file in enumerate(Path("input_dir").glob("*.csv")):
    pipe = Pipe(
        input_adapter=CSVInputAdapter(csv_file),
        output_adapter=CSVOutputAdapter(
            "merged_output.csv",
            fieldnames=["id", "name", "value"]
        ),
    )
    
    with pipe:
        pipe.wait()
```

### Format Standardization

```python
pipe = Pipe(
    input_adapter=CSVInputAdapter(
        "messy_data.csv",
        delimiter=";",
        quotechar="'",
        skip_rows=2
    ),
    output_adapter=CSVOutputAdapter(
        "standard_data.csv",
        delimiter=",",
        quotechar='"'
    ),
)
```

## Error Handling

CSV adapters provide clear error messages for common issues:

```python
try:
    pipe = Pipe(
        input_adapter=CSVInputAdapter("nonexistent.csv"),
        output_adapter=CSVOutputAdapter("output.csv"),
    )
    pipe.start()
except Exception as e:
    print(f"Error: {e}")
```

Common errors:
- File not found
- Permission denied
- Invalid UTF-8 encoding
- Malformed CSV (unclosed quotes, inconsistent columns)

## Performance Tips

1. **Use MultiThreadExecutor**: For files > 10MB, multi-threading provides significant speedup
2. **Batch Size**: Default 2000 is optimal for most use cases
3. **Memory Usage**: Constant ~50-100MB regardless of file size due to streaming
4. **SSD vs HDD**: CSV reading is I/O bound, SSD provides 3-5x better performance
5. **Compression**: Use uncompressed CSV for maximum speed (handle compression upstream)
