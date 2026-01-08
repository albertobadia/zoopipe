# Examples

This document contains practical examples of using FlowSchema for various data processing scenarios.

---

## Table of Contents

- [Basic CSV Processing](#basic-csv-processing)
- [Parallel Processing with Multiprocessing](#parallel-processing-with-multiprocessing)
- [Distributed Processing with Ray](#distributed-processing-with-ray)
- [Custom Validation Logic](#custom-validation-logic)
- [Handling Errors](#handling-errors)
- [Processing Large Files](#processing-large-files)
- [Custom Adapters](#custom-adapters)

---

## Basic CSV Processing

Simple example of reading a CSV, validating it, and writing the results.

```python
from pydantic import BaseModel, ConfigDict
from flowschema.core import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter

class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    last_name: str
    age: int

schema_flow = FlowSchema(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=CSVOutputAdapter("processed_users.csv"),
    error_output_adapter=CSVOutputAdapter("errors.csv"),
    executor=SyncFifoExecutor(UserSchema),
)

for entry in schema_flow.run():
    status = entry['status'].value
    print(f"[{status.upper()}] Row {entry['position']}: {entry['id']}")
```

---

## Parallel Processing with Multiprocessing

Process a large CSV file using multiple CPU cores.

```python
from pydantic import BaseModel
from flowschema.core import FlowSchema
from flowschema.executor.multiprocessing import MultiProcessingExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter

class SalesRecord(BaseModel):
    order_id: str
    customer_id: str
    amount: float
    date: str

schema_flow = FlowSchema(
    input_adapter=CSVInputAdapter("sales_data.csv"),
    output_adapter=CSVOutputAdapter("processed_sales.csv"),
    error_output_adapter=CSVOutputAdapter("sales_errors.csv"),
    executor=MultiProcessingExecutor(
        SalesRecord,
        max_workers=4,
        chunksize=100,
        compression="lz4"
    ),
)

processed_count = 0
error_count = 0

for entry in schema_flow.run():
    if entry['status'].value == 'validated':
        processed_count += 1
    else:
        error_count += 1

print(f"Processed: {processed_count}, Errors: {error_count}")
```

---

## Distributed Processing with Ray

Process a massive dataset using Ray for distributed computing.

```python
from pydantic import BaseModel
from flowschema.core import FlowSchema
from flowschema.executor.ray import RayExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter

class TransactionSchema(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    currency: str
    timestamp: str

schema_flow = FlowSchema(
    input_adapter=CSVInputAdapter("massive_transactions.csv"),
    output_adapter=CSVOutputAdapter("processed_transactions.csv"),
    error_output_adapter=CSVOutputAdapter("transaction_errors.csv"),
    executor=RayExecutor(
        TransactionSchema,
        address="auto",
        compression="lz4"
    ),
)

for entry in schema_flow.run():
    print(f"Processed transaction {entry['position']}")
```

### Connecting to Remote Ray Cluster

```python
executor = RayExecutor(
    TransactionSchema,
    address="ray://cluster.example.com:10001",
    compression="lz4"
)
```

---

## Custom Validation Logic

Use Pydantic validators for custom validation logic.

```python
from pydantic import BaseModel, field_validator, ValidationError
from datetime import datetime

class OrderSchema(BaseModel):
    order_id: str
    customer_email: str
    order_date: str
    total_amount: float
    
    @field_validator('customer_email')
    @classmethod
    def validate_email(cls, v):
        if '@' not in v:
            raise ValueError('Invalid email format')
        return v.lower()
    
    @field_validator('order_date')
    @classmethod
    def validate_date(cls, v):
        try:
            datetime.strptime(v, '%Y-%m-%d')
        except ValueError:
            raise ValueError('Date must be in YYYY-MM-DD format')
        return v
    
    @field_validator('total_amount')
    @classmethod
    def validate_amount(cls, v):
        if v <= 0:
            raise ValueError('Amount must be positive')
        return v

schema_flow = FlowSchema(
    input_adapter=CSVInputAdapter("orders.csv"),
    output_adapter=CSVOutputAdapter("valid_orders.csv"),
    error_output_adapter=CSVOutputAdapter("invalid_orders.csv"),
    executor=SyncFifoExecutor(OrderSchema),
)

for entry in schema_flow.run():
    if entry['status'].value == 'failed':
        print(f"Row {entry['position']} failed: {entry['errors']}")
```

---

## Handling Errors

Detailed error handling and logging.

```python
from flowschema.core import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

schema_flow = FlowSchema(
    input_adapter=CSVInputAdapter("data.csv"),
    output_adapter=CSVOutputAdapter("output.csv"),
    error_output_adapter=CSVOutputAdapter("errors.csv"),
    executor=SyncFifoExecutor(YourSchema),
)

statistics = {
    'total': 0,
    'validated': 0,
    'failed': 0
}

for entry in schema_flow.run():
    statistics['total'] += 1
    
    if entry['status'].value == 'validated':
        statistics['validated'] += 1
        logger.info(f"Row {entry['position']}: OK")
    else:
        statistics['failed'] += 1
        logger.error(f"Row {entry['position']}: {entry['errors']}")

logger.info(f"Summary: {statistics}")
```

---

## Processing Large Files

Efficiently process large files with progress tracking.

```python
from flowschema.core import FlowSchema
from flowschema.executor.multiprocessing import MultiProcessingExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter
from tqdm import tqdm

schema_flow = FlowSchema(
    input_adapter=CSVInputAdapter("large_file.csv"),
    output_adapter=CSVOutputAdapter("processed.csv"),
    error_output_adapter=CSVOutputAdapter("errors.csv"),
    executor=MultiProcessingExecutor(
        YourSchema,
        max_workers=8,
        chunksize=1000,
        compression="lz4"
    ),
)

with tqdm(desc="Processing") as pbar:
    for entry in schema_flow.run():
        pbar.update(1)
```

---

## Custom Adapters

Create custom input and output adapters.

### Custom JSON Input Adapter

```python
import json
import typing
from flowschema.input_adapter.base import BaseInputAdapter

class JSONInputAdapter(BaseInputAdapter):
    def __init__(self, filepath: str):
        self.filepath = filepath
    
    @property
    def generator(self) -> typing.Generator[dict[str, typing.Any], None, None]:
        with open(self.filepath, 'r') as f:
            data = json.load(f)
            for item in data:
                yield item

schema_flow = FlowSchema(
    input_adapter=JSONInputAdapter("data.json"),
    output_adapter=CSVOutputAdapter("output.csv"),
    executor=SyncFifoExecutor(YourSchema),
)
```

### Custom Database Output Adapter

```python
from flowschema.output_adapter.base import BaseOutputAdapter
from flowschema.models.core import EntryTypedDict

class DatabaseOutputAdapter(BaseOutputAdapter):
    def __init__(self, connection_string: str, table_name: str):
        self.connection_string = connection_string
        self.table_name = table_name
        self.connection = None
    
    def write(self, entry: EntryTypedDict) -> None:
        if entry['status'].value == 'validated':
            data = entry['validated_data']
        
    def close(self) -> None:
        if self.connection:
            self.connection.close()

schema_flow = FlowSchema(
    input_adapter=CSVInputAdapter("data.csv"),
    output_adapter=DatabaseOutputAdapter("postgresql://...", "users"),
    executor=SyncFifoExecutor(YourSchema),
)
```

---

## Advanced: Custom Processing Logic

Process entries with custom business logic.

```python
from flowschema.core import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter

schema_flow = FlowSchema(
    input_adapter=CSVInputAdapter("transactions.csv"),
    output_adapter=CSVOutputAdapter("processed.csv"),
    error_output_adapter=CSVOutputAdapter("errors.csv"),
    executor=SyncFifoExecutor(TransactionSchema),
)

high_value_transactions = []

for entry in schema_flow.run():
    if entry['status'].value == 'validated':
        validated_data = entry['validated_data']
        
        if validated_data['amount'] > 10000:
            high_value_transactions.append(validated_data)
        
        validated_data['processed_at'] = datetime.now().isoformat()

print(f"Found {len(high_value_transactions)} high-value transactions")
```

---

## More Examples

For more advanced examples and use cases, check out:

- [Executors Documentation](executors.md)
- [Adapters Documentation](adapters.md)
- [RFC: Architecture and Design](RFC.md)
