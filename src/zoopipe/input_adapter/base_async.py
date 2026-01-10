import abc
import logging
import typing
import uuid

from zoopipe.models.core import EntryTypedDict


class BaseAsyncInputAdapter(abc.ABC):
    def __init__(
        self, id_generator: typing.Callable[[], typing.Any] | None = None
    ) -> None:
        self.id_generator = id_generator or uuid.uuid4
        self._is_opened: bool = False
        self.logger: logging.Logger | None = None

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
