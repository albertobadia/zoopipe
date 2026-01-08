import abc
import typing

from flowschema.models.core import EntryTypedDict


class BaseExecutor(abc.ABC):
    _upstream_iterator: typing.Iterator[typing.Any] = None

    @property
    def compression_algorithm(self) -> str | None:
        return None

    @property
    def do_binary_pack(self) -> bool:
        return False

    def set_upstream_iterator(self, iterator: typing.Iterator[typing.Any]) -> None:
        self._upstream_iterator = iterator

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        raise NotImplementedError


__all__ = ["BaseExecutor"]
