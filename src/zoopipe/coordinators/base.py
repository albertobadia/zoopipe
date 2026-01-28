import abc
from typing import TYPE_CHECKING, Any, List

from zoopipe.structs import WorkerResult

if TYPE_CHECKING:
    from zoopipe.manager import PipeManager


class BaseCoordinator(abc.ABC):
    """
    Abstract base class for all Coordinators.

    Coordinators orchestrate the lifecycle of a parallel execution,
    handling sharding strategy, initialization, and finalization/commit logic.
    """

    @property
    def priority(self) -> int:
        """
        Execution priority.
        Lower numbers run first in on_start.
        Higher numbers run first in on_finish (LIFO).
        """
        return 50

    @abc.abstractmethod
    def prepare_shards(self, adapter: Any, workers: int) -> List[Any]:
        """
        Plan how to split the work across workers.
        """
        raise NotImplementedError

    def on_start(self, manager: "PipeManager") -> None:
        """
        Executed before any worker starts.
        """
        pass

    def on_finish(self, manager: "PipeManager", results: List[WorkerResult]) -> None:
        """
        Executed after all workers successfully complete.
        """
        pass

    def on_error(self, manager: "PipeManager", error: Exception) -> None:
        """
        Executed if execution fails.
        """
        pass
