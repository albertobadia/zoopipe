import json
from typing import Any, Dict, List

from zoopipe.coordinators.base import BaseCoordinator
from zoopipe.structs import WorkerResult
from zoopipe.zoopipe_rust_core import commit_delta_transaction


class DeltaCoordinator(BaseCoordinator):
    """
    Coordinator for Delta Lake tables.
    Handles atomic commits of data files produced by workers using the Delta Log.
    """

    def __init__(
        self,
        table_uri: str,
        mode: str = "append",
        storage_options: Dict[str, str] | None = None,
        schema: Dict[str, Any] | None = None,
    ):
        self.table_uri = table_uri
        self.mode = mode
        self.storage_options = storage_options
        self.schema = schema

    def prepare_shards(self, adapter: Any, workers: int) -> List[Any]:
        return adapter.split(workers)

    def on_start(self, manager: Any) -> None:
        """
        Validate table existence or prepare transaction state.
        If table does not exist and schema is provided, create it.
        """
        import os

        # Simple check for local filesystem
        if self.table_uri.startswith("file://"):
            path_str = self.table_uri.replace("file://", "")
        elif not self.table_uri.startswith("/"):
            # Assume remote or other scheme, skip auto-creation for now
            return
        else:
            path_str = self.table_uri

        log_dir = os.path.join(path_str, "_delta_log")

        # Check if version 0 exists
        has_log = False
        if os.path.exists(log_dir):
            if any(f.endswith(".json") for f in os.listdir(log_dir)):
                has_log = True

        if not has_log:
            if self.mode == "error":
                raise RuntimeError(f"Delta Table does not exist at {self.table_uri}")

            if self.schema:
                print(f"Creating new Delta Table at {self.table_uri}...")
                import json
                import time
                import uuid

                os.makedirs(log_dir, exist_ok=True)

                protocol = {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}

                metadata = {
                    "metaData": {
                        "id": str(uuid.uuid4()),
                        "format": {"provider": "parquet", "options": {}},
                        "schemaString": json.dumps(self.schema),
                        "partitionColumns": [],
                        "configuration": {},
                        "createdTime": int(time.time() * 1000),
                    }
                }

                log_file = os.path.join(log_dir, "00000000000000000000.json")
                with open(log_file, "w") as f:
                    f.write(json.dumps(protocol) + "\n")
                    f.write(json.dumps(metadata) + "\n")
                print("Table created successfully.")
            else:
                print(
                    f"Warning: Table {self.table_uri} does not exist "
                    "and no schema provided. Write might fail."
                )

    def on_finish(self, manager: Any, results: List[WorkerResult]) -> None:
        """
        Collect AddActions from workers and commit transaction.
        """
        action_jsons = []
        for res in results:
            if res.success and res.output_path:
                # Output path from DeltaWriter is a JSON list of AddActions
                # We collect them all to pass to the commit function
                try:
                    # Validate it's JSON list
                    actions = json.loads(res.output_path)
                    if actions:
                        action_jsons.append(res.output_path)
                except json.JSONDecodeError:
                    print(
                        f"Warning: Worker {res.worker_id} returned"
                        f"invalid JSON output: {res.output_path}"
                    )

        if action_jsons:
            print(f"Committing transaction to Delta table at {self.table_uri}...")
            try:
                commit_delta_transaction(
                    self.table_uri,
                    action_jsons,
                    self.mode,
                    self.storage_options,
                )
                print("Delta Lake transaction committed successfully.")
            except Exception as e:
                print(f"Error committing Delta transaction: {e}")
                raise e
        else:
            print("No data produced to commit.")

    def on_error(self, manager: Any, error: Exception) -> None:
        """
        Clean up orphaned files if necessary.
        """
        print(f"DeltaCoordinator caught error: {error}. Transaction aborted.")
        # Future: Implement logic to delete the parquet files created by failed workers
        # since they are not committed to the log yet.
