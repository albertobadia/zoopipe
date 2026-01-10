# JSON Input Adapter

The JSONInputAdapter reads data from JSON files in two formats: JSON arrays or JSONL (JSON Lines).

## Features

- Support for JSON arrays and JSONL format
- Streaming parsing for large JSON files using ijson
- Configurable encoding
- Limit number of items to process
- Memory-efficient for large datasets

## Usage

```python
from zoopipe.input_adapter.json import JSONInputAdapter

input_adapter = JSONInputAdapter(
    source="data.json",
    format="array",
    prefix="item",
    encoding="utf-8",
    max_items=None
)
```

## Parameters

- `source` (required): Path to the JSON file
- `format` (optional): JSON format. Options: `"array"` or `"jsonl"`. Default: `"array"`
- `prefix` (optional): JSON path prefix for array parsing (ijson syntax). Default: `"item"`
- `encoding` (optional): File encoding. Default: `"utf-8"`
- `max_items` (optional): Maximum number of items to read. `None` means all items. Default: `None`

## Examples

### Reading JSON Array

```python
from zoopipe.input_adapter.json import JSONInputAdapter

adapter = JSONInputAdapter(
    source="users.json",
    format="array"
)
```

### Reading JSONL

```python
adapter = JSONInputAdapter(
    source="logs.jsonl",
    format="jsonl",
    max_items=10000
)
```

### Reading Nested JSON

```python
adapter = JSONInputAdapter(
    source="data.json",
    format="array",
    prefix="data.items.item"
)
```

## Format Details

### JSON Array Format

Expected structure:
```json
[
  {"field1": "value1", "field2": "value2"},
  {"field1": "value3", "field2": "value4"}
]
```

### JSONL Format

Expected structure (one JSON object per line):
```
{"field1": "value1", "field2": "value2"}
{"field1": "value3", "field2": "value4"}
```

---

[← Back to Adapters](adapters.md)
