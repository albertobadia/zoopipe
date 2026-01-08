# JSON Output Adapter

The JSONOutputAdapter writes validated entries to a JSON file in two formats: JSON arrays or JSONL (JSON Lines).

## Features

- Support for JSON arrays and JSONL format
- Configurable encoding
- Optional pretty-printing with indentation
- Handles UUIDs and Enums automatically
- Optional metadata inclusion
- Efficient buffered writing

## Usage

```python
from flowschema.output_adapter.json import JSONOutputAdapter

output_adapter = JSONOutputAdapter(
    output="output.json",
    format="array",
    encoding="utf-8",
    include_metadata=False,
    indent=None
)
```

## Parameters

- `output` (required): Path to the output JSON file
- `format` (optional): JSON format. Options: `"array"` or `"jsonl"`. Default: `"array"`
- `encoding` (optional): File encoding. Default: `"utf-8"`
- `include_metadata` (optional): Include entry metadata in output. Default: `False`
- `indent` (optional): Number of spaces for indentation. `None` means compact output. Default: `None`

## Examples

### Writing JSON Array with Pretty Print

```python
from flowschema.output_adapter.json import JSONOutputAdapter

output_adapter = JSONOutputAdapter(
    output="users.json",
    format="array",
    indent=2
)
```

### Writing JSONL

```python
output_adapter = JSONOutputAdapter(
    output="logs.jsonl",
    format="jsonl"
)
```

### Including Metadata

```python
error_adapter = JSONOutputAdapter(
    output="errors.json",
    format="array",
    include_metadata=True,
    indent=2
)
```

### Compact Output

```python
output_adapter = JSONOutputAdapter(
    output="output.json",
    format="array",
    indent=None
)
```

## Output Structure

### With Metadata

```json
{
  "id": "uuid-string",
  "status": "validated",
  "position": 0,
  "metadata": {},
  "data": {
    "field1": "value1",
    "field2": "value2"
  }
}
```

### Without Metadata (Default)

```json
{
  "field1": "value1",
  "field2": "value2"
}
```

---

[← Back to Adapters](adapters.md)
