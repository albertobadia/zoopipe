import asyncio
import concurrent
import enum
import json
import logging
import typing
import uuid

from pydantic import BaseModel, ValidationError

from zoopipe.models.core import EntryStatus, EntryTypedDict

if typing.TYPE_CHECKING:
    from zoopipe.input_adapter.base_async import BaseAsyncInputAdapter
    from zoopipe.output_adapter.base_async import BaseAsyncOutputAdapter


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return str(obj)
        if isinstance(obj, enum.Enum):
            return obj.value
        return super().default(obj)


def validate_entry(
    schema_model: type[BaseModel] | None, entry: EntryTypedDict
) -> EntryTypedDict:
    if schema_model is None:
        entry["status"] = EntryStatus.VALIDATED
        return entry

    try:
        validated_data = schema_model.model_validate(entry["raw_data"])
        entry["validated_data"] = validated_data.model_dump()
        entry["status"] = EntryStatus.VALIDATED
        return entry
    except ValidationError as e:
        entry["status"] = EntryStatus.FAILED
        entry["errors"] = [
            {"loc": err["loc"], "msg": err["msg"], "type": err["type"]}
            for err in e.errors()
        ]
        return entry
    except Exception as e:
        entry["status"] = EntryStatus.FAILED
        entry["errors"] = [{"message": str(e), "type": type(e).__name__}]
        return entry


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

    def _flush(self) -> None:
        if not self._buffer:
            return

        # Capture buffer and clear it immediately
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
