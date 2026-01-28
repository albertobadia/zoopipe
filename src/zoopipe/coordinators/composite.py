from typing import TYPE_CHECKING, Any, List

from zoopipe.coordinators.base import BaseCoordinator
from zoopipe.structs import WorkerResult

if TYPE_CHECKING:
    from zoopipe.manager import PipeManager


class CompositeCoordinator(BaseCoordinator):
    """
    Orchestrates multiple coordinators in a specific order.
    """

    def __init__(self, coordinators: List[BaseCoordinator]):
        self.coordinators = sorted(coordinators, key=lambda c: c.priority)

    @property
    def priority(self) -> int:
        return 0

    def prepare_shards(self, adapter: Any, workers: int) -> List[Any]:
        # Typically only one coordinator handles sharding
        # We use the first one that provides a logic, or the default
        for coord in self.coordinators:
            shards = coord.prepare_shards(adapter, workers)
            if shards and len(shards) == workers:
                return shards
        return [adapter] * workers

    def on_start(self, manager: "PipeManager") -> None:
        for coord in self.coordinators:
            coord.on_start(manager)

    def on_finish(self, manager: "PipeManager", results: List[WorkerResult]) -> None:
        # Finish in REVERSE priority order for safety (Output -> Input)
        for coord in reversed(self.coordinators):
            coord.on_finish(manager, results)

    def on_error(self, manager: "PipeManager", error: Exception) -> None:
        for coord in self.coordinators:
            coord.on_error(manager, error)
