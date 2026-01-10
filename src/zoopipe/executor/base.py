import abc
import enum
import logging
import typing
import uuid
from dataclasses import dataclass

import lz4.frame
import msgpack
from pydantic import BaseModel

from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.models.core import EntryStatus, EntryTypedDict
from zoopipe.utils import validate_entry


@dataclass
class WorkerContext:
    schema_model: type[BaseModel] | None
    do_binary_pack: bool
    compression_algorithm: str | None
    pre_hooks: list[BaseHook] | None = None
    post_hooks: list[BaseHook] | None = None
    max_hook_chunk_size: int | None = None


class BaseExecutor(abc.ABC):
    _upstream_iterator: typing.Iterator[typing.Any] | None = None
    logger: logging.Logger | None = None

    def pack_chunk(self, chunk: list[dict[str, typing.Any]]) -> typing.Any:
        if not self.do_binary_pack:
            return chunk

        def _packer_default(obj):
            if isinstance(obj, uuid.UUID):
                return str(obj)
            if isinstance(obj, enum.Enum):
                return obj.value
            return obj

        packed = msgpack.packb(chunk, default=_packer_default)
        if self.compression_algorithm == "lz4":
            packed = lz4.frame.compress(packed)
        return packed

    @staticmethod
    def _unpack_data(
        data: typing.Any, do_binary_pack: bool, compression_algorithm: str | None
    ) -> list[dict[str, typing.Any]]:
        if not do_binary_pack:
            return data
        if compression_algorithm == "lz4":
            data = lz4.frame.decompress(data)
        return msgpack.unpackb(data)

    @staticmethod
    def _handle_hook_error(
        entries: list[EntryTypedDict], hook_name: str, error: Exception
    ) -> None:
        error_msg = str(error)
        for entry in entries:
            entry["status"] = EntryStatus.FAILED
            entry["errors"].append(
                {
                    "type": "HookError",
                    "message": f"{hook_name}: {error_msg}",
                }
            )
            entry["metadata"][f"hook_error_{hook_name}"] = error_msg

    @staticmethod
    def _execute_hook_safe(
        entries: list[EntryTypedDict], hook: BaseHook, store: HookStore
    ) -> None:
        try:
            hook.execute(entries, store)
        except Exception as e:
            BaseExecutor._handle_hook_error(entries, hook.__class__.__name__, e)

    @staticmethod
    def run_hooks(
        entries: list[EntryTypedDict],
        hooks: list[BaseHook],
        store: HookStore,
        max_hook_chunk_size: int | None = None,
    ) -> list[EntryTypedDict]:
        if not entries or not hooks:
            return entries

        chunk_size = max_hook_chunk_size or len(entries)

        for i in range(0, len(entries), chunk_size):
            sub_batch = entries[i : i + chunk_size]
            for hook in hooks:
                BaseExecutor._execute_hook_safe(sub_batch, hook, store)
        return entries

    @staticmethod
    def process_chunk_on_worker(
        data: typing.Any,
        context: WorkerContext,
    ) -> list[EntryTypedDict]:
        entries = BaseExecutor._unpack_data(
            data, context.do_binary_pack, context.compression_algorithm
        )
        store = {}
        all_hooks = (context.pre_hooks or []) + (context.post_hooks or [])

        for hook in all_hooks:
            hook.setup(store)

        try:
            BaseExecutor.run_hooks(
                entries,
                context.pre_hooks or [],
                store,
                context.max_hook_chunk_size,
            )

            results = [
                BaseExecutor._process_single_entry(entry, context.schema_model)
                for entry in entries
            ]

            BaseExecutor.run_hooks(
                results,
                context.post_hooks or [],
                store,
                context.max_hook_chunk_size,
            )
        finally:
            for hook in all_hooks:
                hook.teardown(store)

        return results

    @staticmethod
    def _process_single_entry(
        entry: EntryTypedDict, schema_model: type[BaseModel] | None
    ) -> EntryTypedDict:
        if entry.get("status") == EntryStatus.FAILED:
            return entry
        return validate_entry(schema_model, entry)

    def set_logger(self, logger: logging.Logger) -> None:
        self.logger = logger

    def __init__(self):
        self._pre_validation_hooks: list[BaseHook] = []
        self._post_validation_hooks: list[BaseHook] = []
        self._max_hook_chunk_size: int | None = None

    @property
    def compression_algorithm(self) -> str | None:
        return None

    @property
    def do_binary_pack(self) -> bool:
        return False

    def set_hooks(
        self,
        pre_validation: list[BaseHook],
        post_validation: list[BaseHook],
        max_hook_chunk_size: int | None = None,
    ) -> None:
        self._pre_validation_hooks = pre_validation
        self._post_validation_hooks = post_validation
        self._max_hook_chunk_size = max_hook_chunk_size

    def set_upstream_iterator(self, iterator: typing.Iterator[typing.Any]) -> None:
        self._upstream_iterator = iterator

    def shutdown(self) -> None:
        pass

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>"


__all__ = ["BaseExecutor", "WorkerContext"]
