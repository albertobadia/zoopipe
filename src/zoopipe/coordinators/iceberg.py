from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    pass

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
        # Deferred import to avoid circular dependency with output_adapter.iceberg
        from zoopipe.output_adapter.iceberg import IcebergOutputAdapter

        if not isinstance(adapter, IcebergOutputAdapter):
            return []

        shards = []
        for _ in range(workers):
            shard = IcebergOutputAdapter(
                table_location=adapter.table_location,
                catalog_properties=adapter.catalog_properties,
            )
            shards.append(shard)

        return shards

    def on_start(self, manager: Any) -> None:
        pass

    def on_finish(self, manager: Any, results: List[WorkerResult]) -> None:
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
        print(f"IcebergCoordinator caught error: {error}. Transaction aborted.")
