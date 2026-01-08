# Creating Custom Adapters

FlowSchema provides base classes for creating your own input and output adapters to support custom data sources and destinations.

## Custom Input Adapters

### BaseInputAdapter

Abstract base class for creating custom synchronous input adapters.

```python
from flowschema.input_adapter.base import BaseInputAdapter
from flowschema.models.core import EntryTypedDict
import typing

class MyCustomInputAdapter(BaseInputAdapter):
    def __init__(self, source_config):
        self.source_config = source_config
    
    @property
    def generator(self) -> typing.Generator[dict[str, typing.Any], None, None]:
        for raw_data in self._fetch_data():
            yield raw_data
    
    def _fetch_data(self):
        pass
```

### BaseAsyncInputAdapter

Abstract base class for creating async input adapters.

```python
from flowschema.input_adapter.base_async import BaseAsyncInputAdapter

class MyAsyncInputAdapter(BaseAsyncInputAdapter):
    @property
    async def generator(self):
        async for raw_data in self._fetch_data_async():
            yield raw_data
    
    async def _fetch_data_async(self):
        pass
```

## Custom Output Adapters

### BaseOutputAdapter

Abstract base class for creating custom synchronous output adapters.

```python
from flowschema.output_adapter.base import BaseOutputAdapter
from flowschema.models.core import EntryTypedDict

class MyCustomOutputAdapter(BaseOutputAdapter):
    def __init__(self, destination_config):
        self.destination_config = destination_config
    
    def write(self, entry: EntryTypedDict) -> None:
        pass
    
    def close(self) -> None:
        pass
```

### BaseAsyncOutputAdapter

Abstract base class for creating async output adapters.

```python
from flowschema.output_adapter.base_async import BaseAsyncOutputAdapter

class MyAsyncOutputAdapter(BaseAsyncOutputAdapter):
    async def write(self, entry: EntryTypedDict) -> None:
        pass
    
    async def close(self) -> None:
        pass
```

## Example: Database Output Adapter

```python
from flowschema.output_adapter.base import BaseOutputAdapter
from flowschema.models.core import EntryTypedDict
import sqlite3

class SQLiteOutputAdapter(BaseOutputAdapter):
    def __init__(self, db_path: str, table_name: str):
        super().__init__()
        self.db_path = db_path
        self.table_name = table_name
        self.connection = None
        self.cursor = None
    
    def open(self) -> None:
        self.connection = sqlite3.connect(self.db_path)
        self.cursor = self.connection.cursor()
        super().open()
    
    def write(self, entry: EntryTypedDict) -> None:
        if entry['status'].value == 'validated':
            data = entry['validated_data']
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['?' for _ in data])
            query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
            self.cursor.execute(query, list(data.values()))
    
    def close(self) -> None:
        if self.connection:
            self.connection.commit()
            self.connection.close()
        super().close()
```

## Example: API Input Adapter

```python
from flowschema.input_adapter.base import BaseInputAdapter
import typing
import requests

class APIInputAdapter(BaseInputAdapter):
    def __init__(self, api_url: str, api_key: str):
        super().__init__()
        self.api_url = api_url
        self.api_key = api_key
    
    @property
    def generator(self) -> typing.Generator[dict[str, typing.Any], None, None]:
        headers = {"Authorization": f"Bearer {self.api_key}"}
        response = requests.get(self.api_url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        for item in data.get('results', []):
            yield item
```

## Best Practices

1. **Resource Management**: Always implement proper `open()` and `close()` methods for resources
2. **Error Handling**: Handle exceptions gracefully and provide meaningful error messages
3. **Memory Efficiency**: Use generators for large datasets instead of loading everything into memory
4. **Type Safety**: Use type hints for better IDE support and code maintainability
5. **Documentation**: Document your custom adapters with docstrings

---

[← Back to Adapters](adapters.md)
