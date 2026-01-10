import abc
import logging

from zoopipe.hooks.base import BaseHook
from zoopipe.models.core import EntryTypedDict


class BaseAsyncOutputAdapter(abc.ABC):
    def __init__(
        self,
        pre_hooks: list[BaseHook] | None = None,
        post_hooks: list[BaseHook] | None = None,
    ) -> None:
        self._is_opened: bool = False
        self.logger: logging.Logger | None = None
        self.pre_hooks: list[BaseHook] = pre_hooks or []
        self.post_hooks: list[BaseHook] = post_hooks or []

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
