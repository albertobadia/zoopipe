import queue
import typing

from zoopipe.hooks.base import BaseHook
from zoopipe.models.core import EntryTypedDict
from zoopipe.output_adapter.base import BaseOutputAdapter


class GeneratorOutputAdapter(BaseOutputAdapter):
    STOP_SENTINEL = object()

    def __init__(
        self,
        max_queue_size: int = 0,
        pre_hooks: list[BaseHook] | None = None,
        post_hooks: list[BaseHook] | None = None,
    ) -> None:
        super().__init__(pre_hooks=pre_hooks, post_hooks=post_hooks)
        self._queue: queue.Queue = queue.Queue(maxsize=max_queue_size)

    def write(self, entry: EntryTypedDict) -> None:
        self._queue.put(entry)

    def close(self) -> None:
        self._queue.put(self.STOP_SENTINEL)
        super().close()

    def __iter__(self) -> typing.Generator[EntryTypedDict, None, None]:
        while True:
            entry = self._queue.get()
            if entry is self.STOP_SENTINEL:
                break
            yield entry


__all__ = ["GeneratorOutputAdapter"]
