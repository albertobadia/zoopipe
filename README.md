# ZooPipe

**ZooPipe** is a lightweight, high-performance data processing framework for Python that combines the simplicity of Pydantic validation with the power of parallel processing.

Whether you're migrating data, cleaning CSVs, or processing streams, ZooPipe provides a structured way to handle validation, transformation, and error management without the complexity of big data frameworks.

---

## ✨ Key Features

- 🔍 **Declarative Validation**: Use [Pydantic](https://docs.pydantic.dev/) models to define and validate your data structures
- 🔌 **Pluggable Architecture**: Easily swap Input Adapters, Output Adapters, and Executors
- ⚡ **Parallel Processing**: Scale from single-threaded to distributed computing with `MultiprocessingExecutor`, `ThreadExecutor`, `DaskExecutor` and `RayExecutor`
- 🗜️ **High-Performance Serialization**: Uses msgpack and optional LZ4 compression for efficient inter-process communication
- 📊 **Built-in Format Support**: Direct support for CSV, JSON (array & JSONL), Parquet, and S3-compatible storage (Boto3, MinIO)
- 🚨 **Automated Error Handling**: Dedicated error output adapter to capture records that fail validation
- 🪝 **Hooks System**: Transform and enrich data at various pipeline stages with built-in and custom hooks
- 🔄 **Async Ready**: Base adapters provided for async implementations
- 🛡️ **Type Safe**: Fully type-hinted for a better developer experience


---

## 🚀 Quick Start

### Installation

```bash
uv add zoopipe
```

Or using pip:

```bash
pip install zoopipe
```

### Simple Example

```python
from pydantic import BaseModel, ConfigDict
from zoopipe import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter

class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    last_name: str
    age: int

pipe = Pipe(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=CSVOutputAdapter("processed_users.csv"),
    error_output_adapter=CSVOutputAdapter("errors.csv"),
    executor=SyncFifoExecutor(UserSchema),
)

# Start the pipewith context manager to ensure cleanup
with pipe:
    report = pipe.start()
    report.wait()

print(f"Finished! Processed {report.total_processed} items.")
```

---

## 💡 Common Data Flows

### CSV → PostgreSQL (via SQLAlchemy)
```python
from zoopipe import Pipe
from zoopipe.executor.multiprocessing import MultiProcessingExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.sqlalchemy import SQLAlchemyOutputAdapter

pipe = Pipe(
    input_adapter=CSVInputAdapter("data.csv"),
    output_adapter=SQLAlchemyOutputAdapter("postgresql://user:pass@localhost/db", "users"),
    executor=MultiProcessingExecutor(UserSchema, max_workers=4),
)
```

### S3 → DuckDB (with JIT fetching)
```python
from zoopipe import Pipe
from zoopipe.executor.ray import RayExecutor
from zoopipe.input_adapter.boto3 import Boto3InputAdapter
from zoopipe.output_adapter.duckdb import DuckDBOutputAdapter

pipe = Pipe(
    input_adapter=Boto3InputAdapter("my-bucket", prefix="data/", jit=True),
    output_adapter=DuckDBOutputAdapter("analytics.duckdb", "events", batch_size=5000),
    executor=RayExecutor(EventSchema),
)
```

### SQLAlchemy → Parquet
```python
from zoopipe import Pipe
from zoopipe.executor.dask import DaskExecutor
from zoopipe.input_adapter.sqlalchemy import SQLAlchemyInputAdapter
from zoopipe.output_adapter.arrow import ArrowOutputAdapter

pipe = Pipe(
    input_adapter=SQLAlchemyInputAdapter("mysql://localhost/olddb", "legacy_table"),
    output_adapter=ArrowOutputAdapter("output.parquet", format="parquet"),
    executor=DaskExecutor(MigrationSchema),
)
```

### CSV with Field Validation (using Pydantic)
```python
from pydantic import BaseModel, field_validator, ConfigDict
from zoopipe import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter

class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    email: str
    age: int
    
    @field_validator('email')
    @classmethod
    def normalize_email(cls, v):
        return v.lower().strip()
    
    @field_validator('name')
    @classmethod
    def normalize_name(cls, v):
        return v.strip().title()

pipe = Pipe(
    input_adapter=CSVInputAdapter("users.csv"),
    output_adapter=CSVOutputAdapter("clean_users.csv"),
    executor=SyncFifoExecutor(UserSchema),
)
```

### API Enrichment with Hooks
```python
from zoopipe import Pipe
from zoopipe.executor.asyncio import AsyncIOExecutor
from zoopipe.input_adapter.json import JSONInputAdapter
from zoopipe.output_adapter.json import JSONOutputAdapter
from zoopipe.hooks.base import BaseHook
import aiohttp

class GeoLocationHook(BaseHook):
    async def execute(self, entries, store):
        async with aiohttp.ClientSession() as session:
            for entry in entries:
                data = entry.get("validated_data", {})
                ip = data.get("ip_address")
                if ip:
                    async with session.get(f"https://ipapi.co/{ip}/json/") as resp:
                        geo_data = await resp.json()
                        entry["metadata"]["location"] = {
                            "country": geo_data.get("country_name"),
                            "city": geo_data.get("city")
                        }
        return entries

pipe = Pipe(
    input_adapter=JSONInputAdapter("events.jsonl", format="jsonl"),
    output_adapter=JSONOutputAdapter("enriched_events.jsonl", format="jsonl"),
    executor=AsyncIOExecutor(EventSchema, concurrency=10),
    post_validation_hooks=[GeoLocationHook()],
)
```

### Database Lookup with Caching (using Store)
```python
from zoopipe import Pipe
from zoopipe.executor.multiprocessing import MultiProcessingExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.json import JSONOutputAdapter
from zoopipe.hooks.base import BaseHook
from sqlalchemy import create_engine

class ProductLookupHook(BaseHook):
    def __init__(self, db_url: str):
        super().__init__()
        self.db_url = db_url
    
    def setup(self, store):
        # Initialize resources once per worker
        store["engine"] = create_engine(self.db_url, pool_size=5)
        store["product_cache"] = {}  # Shared cache
        store["cache_hits"] = 0
    
    async def execute(self, entries, store):
        engine = store["engine"]
        cache = store["product_cache"]
        
        for entry in entries:
            product_id = entry.get("validated_data", {}).get("product_id")
            
            # Check cache first
            if product_id in cache:
                entry["metadata"]["product_name"] = cache[product_id]
                store["cache_hits"] += 1
            else:
                # Fetch from DB and cache
                with engine.connect() as conn:
                    result = conn.execute(
                        f"SELECT name FROM products WHERE id = {product_id}"
                    ).fetchone()
                    if result:
                        cache[product_id] = result[0]
                        entry["metadata"]["product_name"] = result[0]
        
        return entries
    
    def teardown(self, store):
        # Cleanup resources
        engine = store.get("engine")
        if engine:
            engine.dispose()
        print(f"Cache hits: {store.get('cache_hits', 0)}")

pipe = Pipe(
    input_adapter=CSVInputAdapter("orders.csv"),
    output_adapter=JSONOutputAdapter("enriched_orders.jsonl", format="jsonl"),
    executor=MultiProcessingExecutor(OrderSchema, max_workers=4),
    post_validation_hooks=[ProductLookupHook("postgresql://localhost/shop")],
)
```

---

## 📚 Documentation

### Getting Started
- [**Installation & First Steps**](docs/getting-started.md) - Get up and running quickly

### Core Concepts
- [**Executors**](docs/executors.md) - Learn about SyncFifoExecutor, MultiprocessingExecutor, ThreadExecutor, DaskExecutor and RayExecutor
- [**Adapters**](docs/adapters.md) - Input and Output adapters for various data sources
- [**Examples**](docs/examples.md) - Practical examples for common use cases


## 🎯 Use Cases

ZooPipe excels at:

- **Legacy Data Migrations**: Moving data between heterogeneous databases with validation
- **ETL Pipelines**: Extract, Transform, Load workflows with error handling
- **Data Cleaning**: Processing manually generated files (Excel/CSV) with inconsistent formats
- **Quality Filters**: Acting as a validation layer before loading data into Data Lakes or ML models
- **Batch Processing**: Processing large datasets with parallel execution

---

## 🧩 Architecture

ZooPipe uses a decoupled architecture based on four components:

```
┌─────────────────┐      ┌──────────────┐      ┌─────────────────┐
│  Input Adapter  │─────▶│   Executor   │─────▶│ Output Adapter  │
│ (CSV, JSON,     │      │ (Validation  │      │ (CSV, JSON,     │
│  Parquet, DB,   │      │  & Transform)│      │  Parquet, DB,   │
│  S3, API, etc)  │      │              │      │  S3, API, etc)  │
└─────────────────┘      └──────────────┘      └─────────────────┘
                                │
                                │ (errors)
                                ▼
                         ┌──────────────┐
                         │ Error Output │
                         │   Adapter    │
                         └──────────────┘
```

- **Input Adapter**: Reads data from sources (CSV, JSON, Parquet, SQL, S3, API)
- **Executor**: Validates with Pydantic and processes data (sequential or parallel)
- **Output Adapter**: Persists validated data
- **Error Output Adapter**: Captures failed validations (Dead Letter Queue)

---

## 🔧 Executors

ZooPipe provides three execution strategies:

| Executor | Best For | Parallelism |
|----------|----------|-------------|
| `SyncFifoExecutor` | Small datasets, debugging | Single-threaded |
| `MultiprocessingExecutor` | Large datasets on single machine | Multi-process (CPU cores) |
| `ThreadExecutor` | IO-bound tasks (network/DB) | Multi-thread |
| `AsyncIOExecutor` | Async workflows, async hooks | Asyncio concurrency |
| `DaskExecutor` | ETL pipelines, Dask users | Dask cluster |
| `RayExecutor` | Massive datasets, distributed | Ray cluster |

See the [Executors documentation](docs/executors.md) for detailed information.

---

## 🛠 Development

### Setup

```bash
git clone https://github.com/albertobadia/zoopipe.git
cd zoopipe
uv sync
```

### Running Tests

```bash
uv run pytest -v
```

### Linting

```bash
./lint.sh
```

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

---

## 📧 Contact

**Alberto Daniel Badia**  
Email: alberto_badia@enlacepatagonia.com  
GitHub: [@albertobadia](https://github.com/albertobadia)
