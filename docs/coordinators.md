# Coordinators

In ZooPipe, a **Coordinator** is a component responsible for managing the lifecycle and state of a distributed or parallel pipeline execution. While **Executors** handle the processing of data within a single worker, and **Engines** handle the distribution of workers, the **Coordinator** ensures that the collective effort of all workers results in a coherent outcome.

It acts as the "brain" that lives in the main process (the orchestrator) and reacts to global events.

## Role of a Coordinator

The primary responsibilities of a coordinator are:

1.  **Sharding (Input Coordination):** Deciding how to split a large dataset among multiple workers so they don't process overlapping data.
2.  **Atomic Commit (Output Coordination):** Ensuring that results from multiple parallel workers are finalized only if *all* workers succeed (e.g., committing a transaction, merging files).
3.  **Cleanup (Error Handling):** Performing cleanup actions if the pipeline crashes (e.g., deleting temporary files, rolling back transactions).

## The `BaseCoordinator` Interface

Any custom coordinator must inherit from `BaseCoordinator` and implement the lifecycle hooks.

```python
class BaseCoordinator(Protocol):
    def prepare_shards(self, input_adapter: Any, workers: int) -> List[Any]:
        """
        Split the input adapter into 'workers' distinct shards.
        """
        ...

    def on_start(self, manager: Any) -> None:
        """
        Called before the pipeline starts. Useful for initializing shared resources.
        """
        ...

    def on_finish(self, manager: Any, results: List[WorkerResult]) -> None:
        """
        Called after all workers complete successfully.
        Use this to merge results or commit transactions.
        """
        ...

    def on_error(self, manager: Any, error: Exception) -> None:
        """
        Called if the pipeline fails. Use this for rollback or cleanup.
        """
        ...
```

## Built-in Coordinators

ZooPipe comes with several built-in coordinators that handle common patterns.

### 1. `CompositeCoordinator`

Used internally by `PipeManager` to chain multiple coordinators together. For example, you might need an `IcebergCoordinator` to handle the commit logic AND a custom coordinator to send a Slack notification on completion.

### 2. `FileMergeCoordinator`

Automatically injected when you run `PipeManager.parallelize_pipe(..., merge=True)`.

-   **Function:** It waits for all workers to finish writing their partial files (e.g., `part-001.csv`, `part-002.csv`) and then concatenates them into the final requested output file.
-   **Supported Formats:** CSV, JSONL.

### 3. `IcebergCoordinator`

Used when writing to Iceberg tables.

-   **Function:** It collects the list of parquet files produced by each worker and submits them to the Iceberg catalog in a single atomic transaction.
-   **Benefit:** Prevents "dirty reads" where users might see partial data while the pipeline is running.

## Creating a Custom Coordinator

You can create custom coordinators to integrate ZooPipe with your infrastructure.

**Example: Notification Coordinator**

```python
from zoopipe.coordinators import BaseCoordinator
from zoopipe import PipeManager

class SlackNotificationCoordinator(BaseCoordinator):
    def __init__(self, channel_id: str):
        self.channel = channel_id

    def on_finish(self, manager: PipeManager, results: list):
        total_rows = manager.report.total_processed
        send_slack_msg(
            self.channel, 
            f"✅ Pipeline finished! Processed {total_rows} rows."
        )

    def on_error(self, manager: PipeManager, error: Exception):
        send_slack_msg(
            self.channel, 
            f"🚨 Pipeline failed: {error}"
        )

# Usage
manager = PipeManager(..., coordinator=SlackNotificationCoordinator("#data-ops"))
manager.run()
```
