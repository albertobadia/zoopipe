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
        # Detect adapter type without circular imports
        is_input = hasattr(adapter, "get_native_reader")
        is_output = hasattr(adapter, "get_native_writer")

        if hasattr(adapter, "can_split") and not adapter.can_split:
            if is_input:
                return [adapter]
            elif is_output:
                return [adapter] * workers

        if hasattr(adapter, "split"):
            res = adapter.split(workers)
            if not res or len(res) == 1 and is_output and workers > 1:
                return [adapter] * workers
            return res

        if is_output:
            return [adapter] * workers

        return [adapter]

    def on_start(self, manager: "PipeManager") -> None:
        pass

    def on_finish(self, manager: "PipeManager", results: List[WorkerResult]) -> None:
        pass

    def on_error(self, manager: "PipeManager", error: Exception) -> None:
        pass
