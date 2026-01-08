import abc
import typing

from flowschema.hooks.base import BaseHook, HookStore
from flowschema.models.core import EntryTypedDict


class BaseExecutor(abc.ABC):
    _upstream_iterator: typing.Iterator[typing.Any] = None

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

    def _execute_hooks(
        self, entry: EntryTypedDict, hooks: list[BaseHook], store: HookStore
    ) -> None:
        # Enforce read-only store during execute phase
        store._lock()
        try:
            for hook in hooks:
                try:
                    result = hook.execute(entry, store)
                    if result:
                        entry["metadata"].update(result)
                except Exception as e:
                    hook_name = hook.__class__.__name__
                    entry["metadata"][f"hook_error_{hook_name}"] = str(e)
        finally:
            store._unlock()

    def set_upstream_iterator(self, iterator: typing.Iterator[typing.Any]) -> None:
        self._upstream_iterator = iterator

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        raise NotImplementedError


__all__ = ["BaseExecutor"]
