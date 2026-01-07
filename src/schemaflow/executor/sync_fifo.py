import typing

from pydantic import BaseModel

from schemaflow.executor.base import BaseExecutor
from schemaflow.models.core import EntryStatus, EntryTypedDict


class SyncFifoExecutor(BaseExecutor):
    def __init__(
        self,
        schema_model: BaseModel,
    ) -> None:
        super().__init__()
        self._schema_model = schema_model
        self._position = 0

    def _validate_entry(self, entry: EntryTypedDict) -> EntryTypedDict:
        try:
            validated_data = self._schema_model.model_validate(entry["raw_data"])
            entry["validated_data"] = validated_data.model_dump()
            entry["status"] = EntryStatus.VALIDATED
            return entry
        except Exception as e:
            entry["status"] = EntryStatus.FAILED
            entry["errors"] = [e]
            return entry

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        for raw_data_entry in self._raw_data_entries:
            validated_entry = self._validate_entry(raw_data_entry)
            self._position += 1
            yield validated_entry

    def add_raw_data_entries(
        self, raw_data_entries: typing.Generator[EntryTypedDict, None, None]
    ) -> None:
        for raw_data_entry in raw_data_entries:
            self.raw_data_entries.append(raw_data_entry)


__all__ = ["SyncFifoExecutor"]
