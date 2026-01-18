# RayEngine (Distributed Execution)

The `RayEngine` allows ZooPipe to scale horizontally across a Ray cluster. It treats each `Pipe` as a **Ray Actor**, providing isolation and distributed persistence.

## Installation

`RayEngine` is an optional dependency. Install it with:

```bash
pip install "zoopipe[ray]"
```

## Basic Usage

```python
from zoopipe import Pipe, PipeManager, CSVInputAdapter, JSONOutputAdapter
from zoopipe.engines.ray import RayEngine

pipe = Pipe(
    input_adapter=CSVInputAdapter("s3://bucket/data.csv"),
    output_adapter=JSONOutputAdapter("s3://bucket/output.jsonl")
)

# PipeManager will distribute the execution to Ray
manager = PipeManager.parallelize_pipe(
    pipe,
    workers=10,
    engine=RayEngine(address="ray://your-cluster:10001")
)

manager.start()
manager.wait()
```

## Zero-Config Deployment

The `RayEngine` is designed to be truly "Zero-Config" by automatically handling the `runtime_env` on your behalf:

### 1. Auto-Installation
When you start `RayEngine`, it detects your environment:
- **Library Mode**: It automatically tells Ray to install the correct version of `zoopipe` and its dependencies (like `pydantic`) on all workers via `pip`.
- **Development Mode**: If it detects you are working in the ZooPipe repository, it extracts dependencies from `pyproject.toml` and ships your local source code and pre-compiled binaries to the workers.

### 2. Binary Shipping (.so / .abi3.so)
ZooPipe ships its pre-compiled Rust binaries to Ray workers. This avoids the need to have a Rust compiler (`rustc`) installed on the worker nodes, drastically reducing startup time and RAM consumption.

### 3. Smart Filtering (.rayignore)
To keep deployment fast, ZooPipe uses a `.rayignore` file in the project root to exclude heavy directories like `.git`, `.venv`, and `target`. This ensures only your code and the essential binaries are transferred to the cluster.

## Performance Tuning

### Memory Limits
Each `RayPipeWorker` actor is configured with a default memory limit of **512MB** to prevent a single failing pipe from crashing an entire node. You can customize the `RayEngine` behavior by passing additional arguments to `RayEngine(..., num_cpus=2, memory=1024*1024*1024)`.

### Sharding and Merging
When using `parallelize_pipe` with `RayEngine`, ensure your output adapter supports sharding. ZooPipe currently supports sharding for:
- `CSVOutputAdapter`
- `ParquetOutputAdapter`
- `ArrowOutputAdapter`
- `JSONOutputAdapter`

If `should_merge=True` is passed to `parallelize_pipe`, the `PipeManager` will attempt to merge the shards into a single file after all workers finish, assuming they are accessible from the local system.
