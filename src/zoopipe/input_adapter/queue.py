import asyncio
import queue
import typing

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.input_adapter.base_async import BaseAsyncInputAdapter
from zoopipe.models.core import EntryStatus, EntryTypedDict


class AsyncQueueInputAdapter(BaseAsyncInputAdapter):
    def __init__(
        self,
        queue: asyncio.Queue,
        sentinel: typing.Any = None,
        id_generator: typing.Callable[[], typing.Any] | None = None,
    ):
        super().__init__(id_generator=id_generator)
        self.queue = queue
        self.sentinel = sentinel
        self._item_count = 0

    @property
    async def generator(self) -> typing.AsyncGenerator[EntryTypedDict, None]:
        if not self._is_opened:
            raise RuntimeError(
                "Adapter must be opened before reading.\n"
                "Use 'async with adapter:' or call await adapter.open()"
            )

        while True:
            item = await self.queue.get()

            if item == self.sentinel:
                break

            yield EntryTypedDict(
                id=self.id_generator(),
                raw_data=item,
                validated_data=None,
                position=self._item_count,
                status=EntryStatus.PENDING,
                errors=[],
                metadata={},
            )
            self._item_count += 1


class QueueInputAdapter(BaseInputAdapter):
    def __init__(
        self,
        queue: queue.Queue,
        sentinel: typing.Any = None,
        id_generator: typing.Callable[[], typing.Any] | None = None,
    ):
        super().__init__(id_generator=id_generator)
        self.queue = queue
        self.sentinel = sentinel
        self._item_count = 0

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        while True:
            item = self.queue.get()

            if item == self.sentinel:
                break

            yield EntryTypedDict(
                id=self.id_generator(),
                raw_data=item,
                validated_data=None,
                position=self._item_count,
                status=EntryStatus.PENDING,
                errors=[],
                metadata={},
            )
            self._item_count += 1
