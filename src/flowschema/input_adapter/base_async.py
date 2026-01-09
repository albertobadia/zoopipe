import abc
import logging
import typing

from flowschema.models.core import EntryTypedDict


class BaseAsyncInputAdapter(abc.ABC):
    _is_opened: bool = False
    logger: logging.Logger | None = None

    def set_logger(self, logger: logging.Logger) -> None:
        self.logger = logger

    @property
    @abc.abstractmethod
    def generator(self) -> typing.AsyncGenerator[EntryTypedDict, None]:
        raise NotImplementedError("Subclasses must implement the generator property")

    async def open(self) -> None:
        self._is_opened = True

    async def close(self) -> None:
        self._is_opened = False

    async def __aenter__(self) -> "BaseAsyncInputAdapter":
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    def __aiter__(
        self,
    ) -> typing.AsyncGenerator[EntryTypedDict, None]:
        return self.generator
