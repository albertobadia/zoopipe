import json
import os
import time
import uuid
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
        return []

    def on_start(self, manager: Any) -> None:
        """
        Validate table existence or prepare transaction state.
        If table does not exist and schema is provided, create it.
        """
        log_dir = f"{self.table_uri.rstrip('/')}/_delta_log"

        is_local = self.table_uri.startswith("/") or self.table_uri.startswith(
            "file://"
        )

        if not is_local:
            return

        path_str = self.table_uri.replace("file://", "")
        local_log_dir = os.path.join(path_str, "_delta_log")

        # Check if version 0 exists
        has_log = False
        if os.path.exists(local_log_dir):
            if any(f.endswith(".json") for f in os.listdir(local_log_dir)):
                has_log = True

        if not has_log:
            if self.mode == "error" or self.mode == "append":
                if not self.schema:
                    print(
                        f"Warning: Table {self.table_uri} does not exist "
                        "and no schema provided. Write might fail."
                    )
                    return

            if self.schema:
                print(f"Creating new Delta Table at {self.table_uri}...")

                os.makedirs(local_log_dir, exist_ok=True)

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
        handles = []
        for res in results:
            if res.success and res.output_path:
                handles.append(res.output_path)

        if handles:
            try:
                commit_delta_transaction(
                    self.table_uri,
                    handles,
                    self.mode,
                    self.storage_options,
                )
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
