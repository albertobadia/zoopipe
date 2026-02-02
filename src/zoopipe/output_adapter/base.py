import abc
import typing

if typing.TYPE_CHECKING:
    from zoopipe.coordinators.base import BaseCoordinator


class BaseOutputAdapter(abc.ABC):
    """
    Abstract base class for all output adapters.

    Output adapters bridge the pipeline results to external destinations.
    They provide the native Rust writer used by the execution core.
    """

    supports_objects: bool = False

    @property
    def can_split(self) -> bool:
        """Return True if this adapter supports parallel splitting."""
        return type(self).split != BaseOutputAdapter.split

    @abc.abstractmethod
    def get_native_writer(self) -> typing.Any:
        """
        Return the underlying Rust writer instance.

        The writer is responsible for serializing and persisting entries
        passed from the internal pipe buffer.
        """
        raise NotImplementedError

    def get_hooks(self) -> list[typing.Any]:
        """
        Return a list of hooks to be executed by the pipeline.

        Can be used for post-processing or cleaning up resources
        after the data has been written.
        """
        return []

    def split(self, workers: int) -> typing.List["BaseOutputAdapter"]:
        """
        Split the output adapter into `workers` partitions for parallel writing.
        """
        return [self]

    def get_coordinator(self) -> "BaseCoordinator":
        """
        Return the coordinator for this adapter.
        Default is the sharding coordinator that uses split().
        """
        from zoopipe.coordinators.default import DefaultShardingCoordinator

        return DefaultShardingCoordinator()
