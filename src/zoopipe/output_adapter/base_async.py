import abc
import logging

from zoopipe.models.core import EntryTypedDict


class BaseAsyncOutputAdapter(abc.ABC):
    _is_opened: bool = False
    logger: logging.Logger | None = None

    def set_logger(self, logger: logging.Logger) -> None:
        self.logger = logger

    @abc.abstractmethod
    async def write(self, entry: EntryTypedDict) -> None:
        raise NotImplementedError("Subclasses must implement the write method")

    async def open(self) -> None:
        self._is_opened = True

    async def close(self) -> None:
        self._is_opened = False

    async def __aenter__(self) -> "BaseAsyncOutputAdapter":
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
