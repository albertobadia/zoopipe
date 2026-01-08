import typing

from pydantic import BaseModel

from flowschema.executor.base import BaseExecutor
from flowschema.models.core import EntryTypedDict
from flowschema.utils import validate_entry


class SyncFifoExecutor(BaseExecutor):
    def __init__(
        self,
        schema_model: BaseModel,
    ) -> None:
        super().__init__()
        self._schema_model = schema_model
        self._position = 0

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        for raw_data_entry in self._raw_data_entries:
            validated_entry = validate_entry(self._schema_model, raw_data_entry)
            self._position += 1
            yield validated_entry

    def add_raw_data_entries(
        self, raw_data_entries: typing.Generator[EntryTypedDict, None, None]
    ) -> None:
        for raw_data_entry in raw_data_entries:
            self.raw_data_entries.append(raw_data_entry)


__all__ = ["SyncFifoExecutor"]
