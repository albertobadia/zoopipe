import typing
from datetime import datetime, timezone

from flowschema.hooks.base import BaseHook, HookStore
from flowschema.models.core import EntryTypedDict


class TimestampHook(BaseHook):
    def __init__(self, field_name: str = "processed_at"):
        self.field_name = field_name

    def execute(self, entry: EntryTypedDict, store: HookStore) -> EntryTypedDict:
        entry["metadata"][self.field_name] = datetime.now(timezone.utc).isoformat()
        return entry


class FieldMapperHook(BaseHook):
    def __init__(
        self,
        field_mapping: dict[
            str, typing.Callable[[EntryTypedDict, HookStore], typing.Any]
        ],
    ):
        self.field_mapping = field_mapping

    def execute(self, entry: EntryTypedDict, store: HookStore) -> EntryTypedDict:
        for field_name, mapper_func in self.field_mapping.items():
            try:
                entry["metadata"][field_name] = mapper_func(entry, store)
            except Exception as e:
                entry["metadata"][f"{field_name}_error"] = str(e)
        return entry
