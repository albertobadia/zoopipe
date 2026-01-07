import abc
import typing

from flowschema.models.core import EntryTypedDict


class BaseAsyncInputAdapter(abc.ABC):
    _is_opened: bool = False

    @property
    @abc.abstractmethod
    def generator(self) -> typing.AsyncGenerator[EntryTypedDict]:
        raise NotImplementedError("Subclasses must implement the generator property")

    async def open(self) -> None:
        self._is_opened = True

    async def close(self) -> None:
        self._is_opened = False

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    async def __aiter__(
        self,
    ) -> typing.AsyncGenerator[EntryTypedDict]:
        return self.generator
