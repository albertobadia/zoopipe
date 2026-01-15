# Excel Adapters

ZooPipe provides Rust-based Excel readers and writers optimized for `.xlsx` files.

## ExcelInputAdapter

Read Excel files with automatic sheet selection and row skipping.

### Basic Usage

```python
from zoopipe import ExcelInputAdapter, JSONOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=ExcelInputAdapter("data.xlsx", sheet="Sheet1"),
    output_adapter=JSONOutputAdapter("output.jsonl", format="jsonl"),
)
```

### Parameters

- **source** (`str | pathlib.Path`): Path to the Excel file to read.
- **sheet** (`str | int | None`, default=`None`): Name or index (0-based) of the sheet to read. If `None`, the first sheet is used.
- **skip_rows** (`int`, default=`0`): Number of rows to skip from the top.
- **fieldnames** (`list[str] | None`, default=`None`): Custom column names. If `None`, the first non-skipped row is used as header.
- **generate_ids** (`bool`, default=`True`): Whether to generate unique IDs for each record.

## ExcelOutputAdapter

Write data to Excel (`.xlsx`) files.

### Basic Usage

```python
from zoopipe import CSVInputAdapter, ExcelOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=CSVInputAdapter("data.csv"),
    output_adapter=ExcelOutputAdapter("output.xlsx", sheet_name="Results"),
)
```

### Parameters

- **path** (`str | pathlib.Path`): Path to the Excel file to write.
- **sheet_name** (`str | None`, default=`None`): Name of the sheet to create.
- **fieldnames** (`list[str] | None`, default=`None`): Specific fields to include as columns.

## Complete Example

### CSV to Excel with Header Customization

```python
from zoopipe import CSVInputAdapter, ExcelOutputAdapter, Pipe
from pydantic import BaseModel

class User(BaseModel):
    user_id: str
    name: str

pipe = Pipe(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=ExcelOutputAdapter(
        "users.xlsx", 
        sheet_name="Users",
        fieldnames=["user_id", "name"]
    ),
    schema_model=User,
)

pipe.start()
pipe.wait()
```
