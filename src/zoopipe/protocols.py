from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class InputAdapterProtocol(Protocol):
    """Protocol for input adapters that provide native readers."""

    def get_native_reader(self) -> Any:
        """Return the native reader instance from Rust core."""
        ...


@runtime_checkable
class OutputAdapterProtocol(Protocol):
    """Protocol for output adapters that provide native writers."""

    def get_native_writer(self) -> Any:
        """Return the native writer instance from Rust core."""
        ...
