import abc
import typing

if typing.TYPE_CHECKING:
    from zoopipe.coordinators.base import BaseCoordinator


class BaseInputAdapter(abc.ABC):
    """
    Abstract base class for all input adapters.

    Input adapters are responsible for providing a native Rust reader
    and optional hooks that are specific to the data source.
    """

    def __init__(self):
        self.required_columns: typing.List[str] | None = None

    @abc.abstractmethod
    def get_native_reader(self) -> typing.Any:
        """
        Return the underlying Rust reader instance.

        This reader must implement the common reader interface in Rust
        to be compatible with the NativePipe.
        """
        raise NotImplementedError

    def set_required_columns(self, columns: typing.List[str]) -> None:
        """
        Set the list of columns that are strictly required by the pipeline.
        Adapters can use this to optimize I/O by only reading these columns.
        """
        self.required_columns = columns

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
        from zoopipe.coordinators.default import DefaultShardingCoordinator

        return DefaultShardingCoordinator()
