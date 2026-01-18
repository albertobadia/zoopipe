# Running Pipelines with Dask

ZooPipe includes a `DaskEngine` that allows you to scale your pipelines across a Dask cluster with zero configuration headaches.

## Installation

To use the Dask engine, install ZooPipe with the dask extras:

```bash
pip install "zoopipe[dask-distributed]"
```

## Quick Start

Using Dask is as simple as swapping the engine in your `PipeManager` or manually initializing `DaskEngine`.

```python
from zoopipe import PipeManager
from zoopipe.engines.dask import DaskEngine

# Initialize the engine (connects to local cluster by default)
manager = PipeManager(engine=DaskEngine())

# Or connect to an existing cluster
# manager = PipeManager(engine=DaskEngine(address="tcp://scheduler-address:8786"))

# Run your pipe as usual
manager.start([my_pipe])
manager.wait()
```

## Zero-Config Dependency Injection

One of the biggest pain points in distributed computing is managing dependencies on workers. ZooPipe's `DaskEngine` solves this automatically:

1. **Agnostic Installation**: The engine intelligently detects your environment manager (`pip`, `uv`, or `poetry`).
2. **Auto-Sync**: When running in development mode (e.g. from a repo), it automatically syncs your dependencies defined in `pyproject.toml` to all workers.
3. **No Boilerplate**: You don't need to build Docker images or manually provision workers for simple tasks.

### Supported Strategies
- **Pip**: Standard behavior.
- **Uv**: If `uv` is found, `uv pip install` is used for faster setup.
- **Poetry**: If running in a poetry env, `poetry run pip install` is used.
- **Manual**: If no package manager is found, we assume the environment is already provisioned (e.g. via Docker) and skip installation.

## Parallelizing Pipes

Just like with Ray, you can parallelize a single large file input across multiple Dask workers:

```python
results = manager.parallelize_pipe(
    pipe=my_big_pipe,
    parallelism=4,  # Split into 4 chunks
    engine=DaskEngine()
)
```

This will:
1. Split the input file into 4 chunks.
2. Spin up 4 Dask Actors.
3. Process them in parallel.
4. Merge the results automatically.
