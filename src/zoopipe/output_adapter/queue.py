import asyncio
import queue

from zoopipe.hooks.base import BaseHook
from zoopipe.models.core import EntryTypedDict
from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.output_adapter.base_async import BaseAsyncOutputAdapter


class AsyncQueueOutputAdapter(BaseAsyncOutputAdapter):
    def __init__(
        self,
        queue: asyncio.Queue,
        pre_hooks: list[BaseHook] | None = None,
        post_hooks: list[BaseHook] | None = None,
    ):
        super().__init__(pre_hooks=pre_hooks, post_hooks=post_hooks)
        self.queue = queue

    async def write(self, entry: EntryTypedDict) -> None:
        if not self._is_opened:
            raise RuntimeError(
                "Adapter must be opened before writing.\n"
                "Use 'async with adapter:' or call await adapter.open()"
            )

        await self.queue.put(entry)


class QueueOutputAdapter(BaseOutputAdapter):
    def __init__(
        self,
        queue: queue.Queue,
        pre_hooks: list[BaseHook] | None = None,
        post_hooks: list[BaseHook] | None = None,
    ):
        super().__init__(pre_hooks=pre_hooks, post_hooks=post_hooks)
        self.queue = queue

    def write(self, entry: EntryTypedDict) -> None:
        self.queue.put(entry)
