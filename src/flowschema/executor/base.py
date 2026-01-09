import abc
import enum
import logging
import typing
import uuid
from dataclasses import dataclass

import lz4.frame
import msgpack
from pydantic import BaseModel

from flowschema.hooks.base import BaseHook, HookStore
from flowschema.models.core import EntryStatus, EntryTypedDict
from flowschema.utils import validate_entry


@dataclass
class WorkerContext:
    schema_model: type[BaseModel]
    do_binary_pack: bool
    compression_algorithm: str | None
    pre_hooks: list[BaseHook] | None = None
    post_hooks: list[BaseHook] | None = None


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
    def run_hooks(
        entry: EntryTypedDict, hooks: list[BaseHook], store: HookStore
    ) -> EntryTypedDict:
        with store.lock_context():
            for hook in hooks:
                try:
                    result = hook.execute(entry, store)
                    if isinstance(result, dict) and result is not entry:
                        entry["metadata"].update(result)
                except Exception as e:
                    hook_name = hook.__class__.__name__
                    entry["status"] = EntryStatus.FAILED
                    entry["errors"].append(
                        {"type": "HookError", "message": f"{hook_name}: {str(e)}"}
                    )
                    entry["metadata"][f"hook_error_{hook_name}"] = str(e)
        return entry

    @staticmethod
    def process_chunk_on_worker(
        data: typing.Any,
        context: WorkerContext,
    ) -> list[EntryTypedDict]:
        entries = BaseExecutor._unpack_data(
            data, context.do_binary_pack, context.compression_algorithm
        )
        results = []
        store = HookStore()
        all_hooks = (context.pre_hooks or []) + (context.post_hooks or [])

        for hook in all_hooks:
            hook.setup(store)

        try:
            for entry in entries:
                if context.pre_hooks:
                    BaseExecutor.run_hooks(entry, context.pre_hooks, store)

                if entry.get("status") == EntryStatus.FAILED:
                    results.append(entry)
                    continue

                validated_entry = validate_entry(context.schema_model, entry)

                if context.post_hooks:
                    BaseExecutor.run_hooks(validated_entry, context.post_hooks, store)

                results.append(validated_entry)
        finally:
            for hook in all_hooks:
                hook.teardown(store)

        return results

    def set_logger(self, logger: logging.Logger) -> None:
        self.logger = logger

    def __init__(self):
        self._pre_validation_hooks: list[BaseHook] = []
        self._post_validation_hooks: list[BaseHook] = []

    @property
    def compression_algorithm(self) -> str | None:
        return None

    @property
    def do_binary_pack(self) -> bool:
        return False

    def set_hooks(
        self, pre_validation: list[BaseHook], post_validation: list[BaseHook]
    ) -> None:
        self._pre_validation_hooks = pre_validation
        self._post_validation_hooks = post_validation

    def set_upstream_iterator(self, iterator: typing.Iterator[typing.Any]) -> None:
        self._upstream_iterator = iterator

    def shutdown(self) -> None:
        """Cleanup resources used by the executor."""
        pass

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>"


__all__ = ["BaseExecutor", "WorkerContext"]
