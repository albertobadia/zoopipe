import typing
from datetime import datetime, timezone

from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.models.core import EntryTypedDict


class TimestampHook(BaseHook):
    def __init__(self, field_name: str = "processed_at"):
        self.field_name = field_name

    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        now = datetime.now(timezone.utc).isoformat()
        for entry in entries:
            entry["metadata"][self.field_name] = now
        return entries


class FieldMapperHook(BaseHook):
    def __init__(
        self,
        field_mapping: dict[
            str, typing.Callable[[EntryTypedDict, HookStore], typing.Any]
        ],
    ):
        self.field_mapping = field_mapping

    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        for entry in entries:
            for field_name, mapper_func in self.field_mapping.items():
                try:
                    entry["metadata"][field_name] = mapper_func(entry, store)
                except Exception as e:
                    entry["metadata"][f"{field_name}_error"] = str(e)
        return entries
