from typing import TYPE_CHECKING, Any, List

from zoopipe.coordinators.base import BaseCoordinator
from zoopipe.structs import WorkerResult

if TYPE_CHECKING:
    from zoopipe.manager import PipeManager


class DefaultShardingCoordinator(BaseCoordinator):
    """
    Default coordinator that handles standard sharding by calling
    the adapter's split() method.
    """

    @property
    def priority(self) -> int:
        return 100

    def prepare_shards(self, adapter: Any, workers: int) -> List[Any]:
        if hasattr(adapter, "split"):
            return adapter.split(workers)
        return [adapter] * workers

    def on_start(self, manager: "PipeManager") -> None:
        pass

    def on_finish(self, manager: "PipeManager", results: List[WorkerResult]) -> None:
        pass

    def on_error(self, manager: "PipeManager", error: Exception) -> None:
        pass
