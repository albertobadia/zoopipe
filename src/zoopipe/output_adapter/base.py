import abc
import typing


class BaseOutputAdapter(abc.ABC):
    """
    Abstract base class for all output adapters.

    Output adapters bridge the pipeline results to external destinations.
    They provide the native Rust writer used by the execution core.
    """

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
