from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class InputAdapterProtocol(Protocol):
    """
    Protocol defining the minimal interface for a pipeline source.

    Any object implementing this protocol can be used as the input
    source for a Pipe.
    """

    def get_native_reader(self) -> Any:
        """Returns the Rust-level reader."""
        ...

    def get_hooks(self) -> list[Any]:
        """Returns optional hooks for data expansion or pre-processing."""
        ...


@runtime_checkable
class OutputAdapterProtocol(Protocol):
    """
    Protocol defining the minimal interface for a pipeline destination.

    Any object implementing this protocol can be used as the output
    target for a Pipe.
    """

    def get_native_writer(self) -> Any:
        """Returns the Rust-level writer."""
        ...

    def get_hooks(self) -> list[Any]:
        """Returns optional hooks for cleanup or post-processing."""
        ...
