import abc
import typing

from zoopipe.coordinators import BaseCoordinator, DefaultShardingCoordinator


class BaseInputAdapter(abc.ABC):
    """
    Abstract base class for all input adapters.

    Input adapters are responsible for providing a native Rust reader
    and optional hooks that are specific to the data source.
    """

    @abc.abstractmethod
    def get_native_reader(self) -> typing.Any:
        """
        Return the underlying Rust reader instance.

        This reader must implement the common reader interface in Rust
        to be compatible with the NativePipe.
        """
        raise NotImplementedError

    def get_hooks(self) -> list[typing.Any]:
        """
        Return a list of hooks to be executed by the pipeline.

        Typically used for pre-fetching data or expanding anchor records
        before they reach the main processing stage.
        """
        return []

    @property
    def can_split(self) -> bool:
        """Return True if this adapter supports parallel splitting."""
        return type(self).split != BaseInputAdapter.split

    def split(self, workers: int) -> typing.List["BaseInputAdapter"]:
        """
        Split the input adapter into `workers` shards for parallel processing.
        """
        return [self]

    def get_coordinator(self) -> "BaseCoordinator":
        """
        Return the coordinator for this adapter.
        Default is the sharding coordinator that uses split().
        """
        return DefaultShardingCoordinator()
