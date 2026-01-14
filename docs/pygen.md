# Python Generator Adapters

ZooPipe provides Python Generator adapters for in-memory streaming and bidirectional data flow. These adapters enable you to read from Python iterables and write to Python generators, perfect for testing, streaming APIs, and in-memory processing.

## Overview

Python Generator adapters provide a bridge between ZooPipe's high-performance pipeline and native Python iterables:

- **PyGeneratorInputAdapter**: Read data from any Python iterable (lists, generators, iterators)
- **PyGeneratorOutputAdapter**: Write data to a Python generator that you can iterate over

This enables powerful patterns like:
- **Testing**: Create test data on-the-fly without files
- **Streaming APIs**: Process data from API responses in memory
- **Real-time Processing**: Stream data between pipeline stages
- **In-Memory ETL**: Transform data without I/O overhead

## PyGeneratorInputAdapter

Read data from any Python iterable as input to your pipeline.

### Basic Usage

```python
from zoopipe import CSVOutputAdapter, Pipe, PyGeneratorInputAdapter

data = [
    {"user_id": "1", "name": "Alice", "email": "alice@example.com"},
    {"user_id": "2", "name": "Bob", "email": "bob@example.com"},
    {"user_id": "3", "name": "Charlie", "email": "charlie@example.com"},
]

pipe = Pipe(
    input_adapter=PyGeneratorInputAdapter(data),
    output_adapter=CSVOutputAdapter("output.csv"),
)

with pipe:
    pipe.wait()
```

### Parameters

- **iterable** (`typing.Iterable[typing.Any]`): Any Python iterable
  - Lists: `[{...}, {...}]`
  - Generators: `(item for item in source)`
  - Iterators: `iter(collection)`
  - Custom iterables: Any object implementing `__iter__`

- **generate_ids** (`bool`, default=`True`): Whether to generate UUIDs for each record

### Reading from Generators

```python
def data_generator():
    for i in range(1000):
        yield {
            "id": str(i),
            "value": i * 2,
            "category": "even" if i % 2 == 0 else "odd"
        }

from zoopipe import JSONOutputAdapter, PyGeneratorInputAdapter, Pipe

pipe = Pipe(
    input_adapter=PyGeneratorInputAdapter(data_generator()),
    output_adapter=JSONOutputAdapter("output.jsonl", format="jsonl"),
)

with pipe:
    pipe.wait()
```

### Reading from API Responses

```python
import requests

def fetch_api_data():
    response = requests.get("https://api.example.com/users")
    for user in response.json():
        yield user

from zoopipe import CSVOutputAdapter, PyGeneratorInputAdapter, Pipe

pipe = Pipe(
    input_adapter=PyGeneratorInputAdapter(fetch_api_data()),
    output_adapter=CSVOutputAdapter("api_users.csv"),
)

with pipe:
    pipe.wait()
```

## PyGeneratorOutputAdapter

Write pipeline results to a Python generator that you can iterate over.

### Basic Usage

```python
from zoopipe import CSVInputAdapter, Pipe, PyGeneratorOutputAdapter

output_adapter = PyGeneratorOutputAdapter(queue_size=100)

pipe = Pipe(
    input_adapter=CSVInputAdapter("input.csv"),
    output_adapter=output_adapter,
)

pipe.start()

for record in output_adapter:
    print(record)
```

### Parameters

- **queue_size** (`int`, default=`1000`): Internal queue size for buffering
  - Larger values: Better throughput, higher memory usage
  - Smaller values: Lower memory usage, potential backpressure
  - Default 1000 is optimal for most use cases

### Consuming Results in Real-Time

```python
from pydantic import BaseModel, ConfigDict
from zoopipe import CSVInputAdapter, Pipe, PyGeneratorOutputAdapter

class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str

output_adapter = PyGeneratorOutputAdapter(queue_size=50)

pipe = Pipe(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=output_adapter,
    schema_model=UserSchema,
)

pipe.start()

for validated_user in output_adapter:
    print(f"Processing: {validated_user['username']}")
```

### Iteration Pattern

The `PyGeneratorOutputAdapter` is both an adapter and an iterator:

```python
output_adapter = PyGeneratorOutputAdapter()

pipe = Pipe(
    input_adapter=CSVInputAdapter("data.csv"),
    output_adapter=output_adapter,
)

pipe.start()

for item in output_adapter:
    if item['value'] > 100:
        print(f"High value item: {item}")
```

## Complete Examples

### In-Memory Testing

```python
from pydantic import BaseModel, EmailStr
from zoopipe import Pipe, PyGeneratorInputAdapter, PyGeneratorOutputAdapter

class UserSchema(BaseModel):
    user_id: str
    email: EmailStr

test_data = [
    {"user_id": "1", "email": "valid@example.com"},
    {"user_id": "2", "email": "invalid-email"},
    {"user_id": "3", "email": "another@example.com"},
]

input_adapter = PyGeneratorInputAdapter(test_data)
output_adapter = PyGeneratorOutputAdapter(queue_size=10)
error_adapter = PyGeneratorOutputAdapter(queue_size=10)

pipe = Pipe(
    input_adapter=input_adapter,
    output_adapter=output_adapter,
    error_output_adapter=error_adapter,
    schema_model=UserSchema,
)

pipe.start()

valid_records = list(output_adapter)
error_records = list(error_adapter)

print(f"Valid: {len(valid_records)}")
print(f"Errors: {len(error_records)}")

assert len(valid_records) == 2
assert len(error_records) == 1
```

### Streaming API Data

```python
import time
from zoopipe import Pipe, PyGeneratorInputAdapter, PyGeneratorOutputAdapter

def simulated_stream():
    for i in range(100):
        yield {"id": i, "timestamp": time.time(), "value": i * 10}
        time.sleep(0.1)

output_adapter = PyGeneratorOutputAdapter(queue_size=20)

pipe = Pipe(
    input_adapter=PyGeneratorInputAdapter(simulated_stream()),
    output_adapter=output_adapter,
)

pipe.start()

for event in output_adapter:
    print(f"Event {event['id']}: {event['value']}")
```

### Multi-Stage In-Memory Pipeline

```python
from zoopipe import Pipe, PyGeneratorInputAdapter, PyGeneratorOutputAdapter

raw_data = [{"id": i, "value": i} for i in range(1000)]

stage1_output = PyGeneratorOutputAdapter(queue_size=100)

stage1 = Pipe(
    input_adapter=PyGeneratorInputAdapter(raw_data),
    output_adapter=stage1_output,
)

stage1.start()

stage1_results = list(stage1_output)

stage2_output = PyGeneratorOutputAdapter(queue_size=100)

stage2 = Pipe(
    input_adapter=PyGeneratorInputAdapter(stage1_results),
    output_adapter=stage2_output,
)

stage2.start()

final_results = list(stage2_output)
print(f"Processed {len(final_results)} records through 2 stages")
```

## Use Cases

### 1. Unit Testing

Create test data without creating files:

```python
from zoopipe import Pipe, PyGeneratorInputAdapter, PyGeneratorOutputAdapter

def test_pipeline_validation():
    test_data = [
        {"id": "1", "value": "10"},
        {"id": "2", "value": "invalid"},
    ]
    
    output = PyGeneratorOutputAdapter()
    errors = PyGeneratorOutputAdapter()
    
    pipe = Pipe(
        input_adapter=PyGeneratorInputAdapter(test_data),
        output_adapter=output,
        error_output_adapter=errors,
    )
    
    pipe.start()
    
    results = list(output)
    error_list = list(errors)
    
    assert len(results) == 1
    assert len(error_list) == 1
```

### 2. Real-Time Streaming

Process streaming data as it arrives:

```python
def websocket_stream():
    while True:
        message = websocket.receive()
        if message is None:
            break
        yield message

output = PyGeneratorOutputAdapter()

pipe = Pipe(
    input_adapter=PyGeneratorInputAdapter(websocket_stream()),
    output_adapter=output,
)

pipe.start()

for processed_message in output:
    send_to_downstream(processed_message)
```

### 3. Data Transformation

Transform data between Python objects:

```python
source_data = database.query("SELECT * FROM users")

output = PyGeneratorOutputAdapter(queue_size=500)

pipe = Pipe(
    input_adapter=PyGeneratorInputAdapter(source_data),
    output_adapter=output,
    schema_model=UserSchema,
)

pipe.start()

for validated_user in output:
    cache.set(validated_user['user_id'], validated_user)
```

### 4. API Response Processing

```python
def paginated_api_call():
    page = 1
    while True:
        response = requests.get(f"https://api.example.com/data?page={page}")
        data = response.json()
        if not data:
            break
        for item in data:
            yield item
        page += 1

output = PyGeneratorOutputAdapter()

pipe = Pipe(
    input_adapter=PyGeneratorInputAdapter(paginated_api_call()),
    output_adapter=output,
)

pipe.start()

all_items = list(output)
```

## Best Practices

### For PyGeneratorInputAdapter
1. **Memory Awareness**: Large lists consume memory; use generators for large datasets
2. **Lazy Evaluation**: Prefer generators over lists for memory efficiency
3. **Error Handling**: Ensure your iterable doesn't raise exceptions unexpectedly
4. **Type Consistency**: All yielded items should have consistent structure

### For PyGeneratorOutputAdapter
1. **Queue Size**: Adjust `queue_size` based on your memory constraints
2. **Consumption Pattern**: Start consuming immediately after `pipe.start()`
3. **Backpressure**: Smaller queue sizes create backpressure, controlling memory
4. **Complete Iteration**: Always consume the entire generator or pipeline may hang

## Performance Characteristics

### PyGeneratorInputAdapter
- **Zero I/O**: No file system overhead
- **Memory Bound**: Limited by Python object creation
- **Throughput**: ~100k-500k items/s depending on object complexity
- **Lazy**: Only materializes data as needed (if using generators)

### PyGeneratorOutputAdapter
- **Buffered**: Internal queue prevents pipeline stalls
- **Blocking**: Iteration blocks until data is available
- **Complete**: Iteration ends when pipeline finishes
- **Thread-Safe**: Can be consumed from same thread as pipeline

## Common Patterns

### Testing with Fixtures

```python
import pytest
from zoopipe import Pipe, PyGeneratorInputAdapter, PyGeneratorOutputAdapter

@pytest.fixture
def sample_data():
    return [
        {"id": "1", "value": 100},
        {"id": "2", "value": 200},
    ]

def test_processing(sample_data):
    output = PyGeneratorOutputAdapter()
    
    pipe = Pipe(
        input_adapter=PyGeneratorInputAdapter(sample_data),
        output_adapter=output,
    )
    
    pipe.start()
    results = list(output)
    
    assert len(results) == 2
```

### Infinite Streams

```python
import time

def infinite_sensor_data():
    while True:
        yield {"timestamp": time.time(), "temperature": random.uniform(20, 30)}
        time.sleep(1)

output = PyGeneratorOutputAdapter(queue_size=10)

pipe = Pipe(
    input_adapter=PyGeneratorInputAdapter(infinite_sensor_data()),
    output_adapter=output,
)

pipe.start()

for reading in output:
    print(f"Temperature: {reading['temperature']:.2f}°C")
    if reading['temperature'] > 28:
        print("Warning: High temperature!")
```

### Batch Processing in Memory

```python
def process_in_batches(data, batch_size=1000):
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        
        output = PyGeneratorOutputAdapter()
        
        pipe = Pipe(
            input_adapter=PyGeneratorInputAdapter(batch),
            output_adapter=output,
        )
        
        pipe.start()
        
        yield from output

large_dataset = [{"id": i} for i in range(10000)]

for processed_item in process_in_batches(large_dataset):
    print(processed_item)
```

## Error Handling

```python
from zoopipe import Pipe, PyGeneratorInputAdapter, PyGeneratorOutputAdapter

output = PyGeneratorOutputAdapter()
errors = PyGeneratorOutputAdapter()

try:
    pipe = Pipe(
        input_adapter=PyGeneratorInputAdapter(data_source),
        output_adapter=output,
        error_output_adapter=errors,
    )
    
    pipe.start()
    
    for item in output:
        process_item(item)
    
    error_list = list(errors)
    if error_list:
        print(f"Encountered {len(error_list)} errors")
        
except Exception as e:
    print(f"Pipeline error: {e}")
```

Common errors:
- **Generator Already Exhausted**: Can only iterate once over the output
- **Pipeline Still Running**: Don't consume before calling `pipe.start()`
- **Queue Full**: Increase `queue_size` if pipeline stalls
- **Infinite Iteration**: Pipeline won't finish if input is infinite

## Integration Examples

### With Pandas

```python
import pandas as pd
from zoopipe import Pipe, PyGeneratorInputAdapter, PyGeneratorOutputAdapter

df = pd.read_csv("data.csv")

output = PyGeneratorOutputAdapter()

pipe = Pipe(
    input_adapter=PyGeneratorInputAdapter(df.to_dict('records')),
    output_adapter=output,
)

pipe.start()

processed_records = list(output)
processed_df = pd.DataFrame(processed_records)
```

### With asyncio

```python
import asyncio
from zoopipe import Pipe, PyGeneratorInputAdapter, PyGeneratorOutputAdapter

async def async_data_source():
    for i in range(100):
        await asyncio.sleep(0.1)
        yield {"id": i, "data": f"item_{i}"}

async def process_async():
    data = [item async for item in async_data_source()]
    
    output = PyGeneratorOutputAdapter()
    
    pipe = Pipe(
        input_adapter=PyGeneratorInputAdapter(data),
        output_adapter=output,
    )
    
    pipe.start()
    
    return list(output)

results = asyncio.run(process_async())
```

## Performance Tips

1. **Use Generators**: Prefer generators over lists for large datasets
2. **Queue Size**: Tune `queue_size` based on your throughput needs
3. **Batch Processing**: Process large datasets in smaller batches
4. **Memory Management**: Monitor memory usage with infinite streams
5. **Early Consumption**: Start consuming output immediately after `pipe.start()`
