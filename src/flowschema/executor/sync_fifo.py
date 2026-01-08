import typing

from pydantic import BaseModel

from flowschema.executor.base import BaseExecutor
from flowschema.hooks.base import HookStore
from flowschema.models.core import EntryTypedDict
from flowschema.utils import validate_entry


class SyncFifoExecutor(BaseExecutor):
    def __init__(
        self,
        schema_model: type[BaseModel],
    ) -> None:
        super().__init__()
        self._schema_model = schema_model
        self._position = 0

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        if not self._upstream_iterator:
            return

        store = HookStore()

        for hook in self._pre_validation_hooks + self._post_validation_hooks:
            hook.setup(store)

        try:
            for chunk in self._upstream_iterator:
                for raw_data_entry in chunk:
                    self._execute_hooks(
                        raw_data_entry, self._pre_validation_hooks, store
                    )

                    validated_entry = validate_entry(self._schema_model, raw_data_entry)

                    self._execute_hooks(
                        validated_entry, self._post_validation_hooks, store
                    )

                    self._position += 1
                    yield validated_entry
        finally:
            for hook in self._pre_validation_hooks + self._post_validation_hooks:
                hook.teardown(store)


__all__ = ["SyncFifoExecutor"]
