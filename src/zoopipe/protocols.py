from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class InputAdapterProtocol(Protocol):
    def get_native_reader(self) -> Any: ...

    def get_hooks(self) -> list[Any]: ...


@runtime_checkable
class OutputAdapterProtocol(Protocol):
    def get_native_writer(self) -> Any: ...

    def get_hooks(self) -> list[Any]: ...
