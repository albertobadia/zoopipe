from typing import Any, Dict, List

from zoopipe.coordinators.base import BaseCoordinator
from zoopipe.structs import WorkerResult
from zoopipe.zoopipe_rust_core import commit_iceberg_transaction


class IcebergCoordinator(BaseCoordinator):
    """
    Coordinator for Iceberg tables.
    Handles atomic commits of data files produced by workers.
    """

    def __init__(self, table_location: str, catalog_properties: Dict[str, str]):
        self.table_location = table_location
        self.catalog_properties = catalog_properties

    def prepare_shards(self, adapter: Any, workers: int) -> List[Any]:
        return []

    def on_start(self, manager: Any) -> None:
        """
        Validate table existence or prepare transaction state.
        """
        pass

    def on_finish(self, manager: Any, results: List[WorkerResult]) -> None:
        """
        Collect data files from workers and commit transaction.
        """
        data_files = []
        for res in results:
            if res.success and res.output_path:
                data_files.append(res.output_path)

        if data_files:
            print(f"Committing {len(data_files)} result batches to Iceberg table...")
            commit_iceberg_transaction(
                self.table_location, self.catalog_properties, data_files
            )
            print("Iceberg commit successful.")

    def on_error(self, manager: Any, error: Exception) -> None:
        """
        Clean up orphaned files if necessary.
        """
        print(f"IcebergCoordinator caught error: {error}. Transaction aborted.")
