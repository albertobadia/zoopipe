# Hooks System

Pipe provides a powerful hooks system that allows you to transform, enrich, or modify data at various stages of the unified processing pipeline.

## Overview

Hooks intercept data at two critical points in the processing pipeline:

1. **Pre-validation Hooks**: Execute before Pydantic validation (modify raw input data)
2. **Post-validation Hooks**: Execute after successful validation (enrich validated data)

The hook system is designed to be:
- **Thread-safe**: Uses a `HookStore` with locking mechanisms
- **Lifecycle-aware**: Hooks have `setup()` and `teardown()` methods
- **Metadata-capable**: Hooks can return metadata that gets merged into entries

## BaseHook

Abstract base class for creating custom hooks.

```python
from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.models.core import EntryTypedDict

class MyCustomHook(BaseHook):
    def setup(self, store: HookStore) -> None:
        pass
    
    def execute(self, entry: EntryTypedDict, store: HookStore) -> dict | None:
        return {"custom_field": "value"}
    
    def teardown(self, store: HookStore) -> None:
        pass
```

### Hook Methods

- **`setup(store)`**: Called once before processing starts. Initialize resources or shared state.
- **`execute(entry, store)`**: Called for each entry. Return a dict to add metadata to the entry, or None.
- **`teardown(store)`**: Called once after all processing completes. Clean up resources.

### HookStore

The `HookStore` provides thread-safe access to shared data across hook executions:

```python
class CountingHook(BaseHook):
    def setup(self, store: HookStore) -> None:
        store.count = 0  # Set using attribute style
    
    def execute(self, entry: EntryTypedDict, store: HookStore) -> dict | None:
        current = store.get("count", 0)  # Get with default
        store.count = current + 1
        return {"processing_order": current}
```

**API Methods:**
- **Set values**: `store.attribute_name = value` (attribute style)
- **Get values**: `value = store.attribute_name` (without default) or `value = store.get("name", default)` (with default)
- **Check existence**: `if "name" in store:`

## Built-in Hooks

### TimestampHook

Automatically adds timestamps to your data.

```python
from zoopipe.hooks.builtin import TimestampHook

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
from zoopipe.hooks.builtin import FieldMapperHook

mapper_hook = FieldMapperHook(mapping={
    "old_field_name": "new_field_name",
    "user_name": "username"
})
```

**Parameters:**
- `mapping`: Dictionary mapping old field names to new field names

## Using Hooks

Hooks are registered as either **pre-validation** or **post-validation** hooks when creating your Pipe.

### Pre-validation Hooks

Pre-validation hooks run before Pydantic validation and can modify raw input data:

```python
from zoopipe.core import Pipe
from zoopipe.hooks.base import BaseHook

class NormalizeFieldsHook(BaseHook):
    def execute(self, entry: dict, store) -> dict | None:
        if "name" in entry:
            entry["name"] = entry["name"].strip().lower()
        return None

pipe = Pipe(
    input_adapter=input_adapter,
    output_adapter=output_adapter,
    executor=executor,
    pre_validation_hooks=[NormalizeFieldsHook()],
)
```

### Post-validation Hooks

Post-validation hooks run after successful validation and can enrich the validated data:

```python
from zoopipe.hooks.builtin import TimestampHook

pipe = Pipe(
    input_adapter=input_adapter,
    output_adapter=output_adapter,
    executor=executor,
    post_validation_hooks=[TimestampHook(field_name="processed_at")],
)
```

### Combining Both

You can use both pre-validation and post-validation hooks together:

```python
pipe = Pipe(
    input_adapter=input_adapter,
    output_adapter=output_adapter,
    executor=executor,
    pre_validation_hooks=[NormalizeFieldsHook()],
    post_validation_hooks=[TimestampHook()],
)
```

## Custom Hook Examples

### Data Transformation Hook

```python
from zoopipe.hooks.base import BaseHook, HookStore

class UppercaseHook(BaseHook):
    def __init__(self, fields: list[str]):
        self.fields = fields
    
    def execute(self, entry: dict, store: HookStore) -> dict | None:
        for field in self.fields:
            if field in entry:
                entry[field] = entry[field].upper()
        return None
```

### Data Enrichment Hook

```python
class EnrichmentHook(BaseHook):
    def execute(self, entry: dict, store: HookStore) -> dict | None:
        return {
            "source": "import_system",
            "version": "1.0"
        }
```

### Logging Hook with Lifecycle

```python
import logging

class LoggingHook(BaseHook):
    def setup(self, store: HookStore) -> None:
        self.logger = logging.getLogger(__name__)
        store.processed_count = 0
        self.logger.info("Starting processing...")
    
    def execute(self, entry: dict, store: HookStore) -> dict | None:
        count = store.get("processed_count", 0)
        store.processed_count = count + 1
        self.logger.debug(f"Processing entry #{count + 1}")
        return None
    
    def teardown(self, store: HookStore) -> None:
        total = store.get("processed_count", 0)
        self.logger.info(f"Finished processing {total} entries")
```

## Hook Execution Order

### Within Pre-validation Hooks

Pre-validation hooks are executed in list order:

```python
pre_validation_hooks = [
    hook1,  # Executed first
    hook2,  # Executed second
    hook3,  # Executed third
]
```

### Within Post-validation Hooks

Post-validation hooks are similarly executed in list order:

```python
post_validation_hooks = [
    hook1,  # Executed first
    hook2,  # Executed second
]
```

### Complete Pipeline Order

The full processing sequence is:

1. Data unpacking (if binary packing enabled)
2. Pre-validation hooks (in registration order)
3. Pydantic schema validation
4. Post-validation hooks (in registration order)
5. Results collection

---

[← Back to Adapters](adapters.md)
