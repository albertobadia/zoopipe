# Hooks System

FlowSchema provides a hooks system that allows you to transform, enrich, or modify data at various stages of the processing pipeline.

## Overview

Hooks can intercept data at three key points:
1. **on_raw_data**: Before validation (modify raw input)
2. **on_validated**: After successful validation
3. **on_failed**: After failed validation

## BaseHook

Abstract base class for creating custom hooks.

```python
from flowschema.hooks.base import BaseHook
from flowschema.models.core import EntryTypedDict

class MyCustomHook(BaseHook):
    def on_raw_data(self, raw_data: dict) -> dict:
        return raw_data
    
    def on_validated(self, entry: EntryTypedDict) -> EntryTypedDict:
        return entry
    
    def on_failed(self, entry: EntryTypedDict) -> EntryTypedDict:
        return entry
```

## Built-in Hooks

### TimestampHook

Automatically adds timestamps to your data.

```python
from flowschema.hooks.builtin import TimestampHook

timestamp_hook = TimestampHook(
    field_name="processed_at",
    timezone="UTC"
)
```

**Parameters:**
- `field_name`: Name of the field to add (default: `"timestamp"`)
- `timezone`: Timezone for the timestamp (default: `"UTC"`)

### FieldMapperHook

Renames or maps fields in your data.

```python
from flowschema.hooks.builtin import FieldMapperHook

mapper_hook = FieldMapperHook(mapping={
    "old_field_name": "new_field_name",
    "user_name": "username"
})
```

**Parameters:**
- `mapping`: Dictionary mapping old field names to new field names

## Using Hooks

### Single Hook

```python
from flowschema.core import FlowSchema
from flowschema.hooks import HookStore, TimestampHook

hook_store = HookStore()
hook_store.register(TimestampHook())

schema_flow = FlowSchema(
    input_adapter=input_adapter,
    output_adapter=output_adapter,
    executor=executor,
    hooks=hook_store
)
```

### Multiple Hooks

```python
from flowschema.hooks import HookStore, TimestampHook, FieldMapperHook

hook_store = HookStore()
hook_store.register(FieldMapperHook(mapping={"old_name": "new_name"}))
hook_store.register(TimestampHook(field_name="imported_at"))

schema_flow = FlowSchema(
    input_adapter=input_adapter,
    output_adapter=output_adapter,
    executor=executor,
    hooks=hook_store
)
```

## Custom Hook Examples

### Data Transformation Hook

```python
from flowschema.hooks.base import BaseHook

class UppercaseHook(BaseHook):
    def __init__(self, fields: list[str]):
        self.fields = fields
    
    def on_raw_data(self, raw_data: dict) -> dict:
        for field in self.fields:
            if field in raw_data:
                raw_data[field] = raw_data[field].upper()
        return raw_data
```

### Data Enrichment Hook

```python
class EnrichmentHook(BaseHook):
    def on_validated(self, entry: EntryTypedDict) -> EntryTypedDict:
        entry["validated_data"]["source"] = "import_system"
        entry["validated_data"]["version"] = "1.0"
        return entry
```

### Logging Hook

```python
import logging

class LoggingHook(BaseHook):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def on_validated(self, entry: EntryTypedDict) -> EntryTypedDict:
        self.logger.info(f"Validated entry: {entry['id']}")
        return entry
    
    def on_failed(self, entry: EntryTypedDict) -> EntryTypedDict:
        self.logger.error(f"Failed entry: {entry['id']}, errors: {entry['errors']}")
        return entry
```

## Hook Execution Order

Hooks are executed in the order they are registered:

```python
hook_store = HookStore()
hook_store.register(hook1)  # Executed first
hook_store.register(hook2)  # Executed second
hook_store.register(hook3)  # Executed third
```

---

[← Back to Adapters](adapters.md)
