import asyncio
import concurrent
import logging
import typing

if typing.TYPE_CHECKING:
    from zoopipe.hooks.base import BaseHook
    from zoopipe.input_adapter.base_async import BaseAsyncInputAdapter
    from zoopipe.output_adapter.base_async import BaseAsyncOutputAdapter

from zoopipe.models.core import EntryTypedDict


class SyncAsyncBridge:
    def __init__(
        self,
        async_obj: typing.Any,
        loop: asyncio.AbstractEventLoop | None = None,
    ):
        self.async_obj = async_obj
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                pass
        self._loop = loop

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            raise RuntimeError(
                "No event loop found. "
                "Async adapters require an active event loop. "
                "Ensure you are running within an async context."
            )
        return self._loop

    def run_sync(self, coro: typing.Coroutine) -> typing.Any:
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        if current_loop is self.loop and self.loop.is_running():
            raise RuntimeError(
                "Deadlock detected: run_sync() called from the same thread "
                "running the event loop. This execution path is synchronous "
                "and blocks the loop, preventing the coroutine from running."
            )

        try:
            return asyncio.run_coroutine_threadsafe(coro, self.loop).result()
        except (concurrent.futures.CancelledError, RuntimeError) as e:
            if "close" in str(coro):
                return None
            raise e


class AsyncInputBridge:
    def __init__(
        self,
        adapter: "BaseAsyncInputAdapter",
        loop: asyncio.AbstractEventLoop | None = None,
        batch_size: int = 100,
    ):
        self.adapter = adapter
        self.bridge = SyncAsyncBridge(adapter, loop)
        self.batch_size = batch_size

    def set_logger(self, logger: logging.Logger) -> None:
        self.adapter.set_logger(logger)

    def open(self) -> None:
        self.bridge.run_sync(self.adapter.open())

    def close(self) -> None:
        self.bridge.run_sync(self.adapter.close())

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def pre_hooks(self) -> list["BaseHook"]:
        return getattr(self.adapter, "pre_hooks", [])

    @property
    def post_hooks(self) -> list["BaseHook"]:
        return getattr(self.adapter, "post_hooks", [])

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        async_gen = self.adapter.generator

        async def _get_batch():
            batch = []
            try:
                for _ in range(self.batch_size):
                    batch.append(await anext(async_gen))
            except StopAsyncIteration:
                pass
            return batch

        while True:
            items = self.bridge.run_sync(_get_batch())
            if not items:
                break
            for item in items:
                yield item


class AsyncOutputBridge:
    def __init__(
        self,
        adapter: "BaseAsyncOutputAdapter",
        loop: asyncio.AbstractEventLoop | None = None,
        batch_size: int = 100,
    ):
        self.adapter = adapter
        self.bridge = SyncAsyncBridge(adapter, loop)
        self.batch_size = batch_size
        self._buffer: list[EntryTypedDict] = []

    def set_logger(self, logger: logging.Logger) -> None:
        self.adapter.set_logger(logger)

    def open(self) -> None:
        self.bridge.run_sync(self.adapter.open())

    def close(self) -> None:
        self._flush()
        self.bridge.run_sync(self.adapter.close())

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def pre_hooks(self) -> list["BaseHook"]:
        return getattr(self.adapter, "pre_hooks", [])

    @property
    def post_hooks(self) -> list["BaseHook"]:
        return getattr(self.adapter, "post_hooks", [])

    def _flush(self) -> None:
        if not self._buffer:
            return

        batch = self._buffer[:]
        self._buffer.clear()

        async def _write_batch():
            for item in batch:
                await self.adapter.write(item)

        self.bridge.run_sync(_write_batch())

    def write(self, entry: EntryTypedDict) -> None:
        self._buffer.append(entry)
        if len(self._buffer) >= self.batch_size:
            self._flush()
