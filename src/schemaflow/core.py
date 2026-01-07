import typing

from schemaflow.executor.base import BaseExecutor
from schemaflow.input_adapter.base import BaseInputAdapter
from schemaflow.models.core import EntryStatus, EntryTypedDict
from schemaflow.output_adapter.base import BaseOutputAdapter


class SchemaFlow:
    def __init__(
        self,
        input_adapter: BaseInputAdapter,
        output_adapter: BaseOutputAdapter,
        executor: BaseExecutor,
        error_output_adapter: BaseOutputAdapter | None = None,
    ) -> None:
        self.input_adapter = input_adapter
        self.output_adapter = output_adapter
        self.executor = executor
        self.error_output_adapter = error_output_adapter
        self.error_entries: list[EntryTypedDict] = []

    def _handle_entry(self, entry: EntryTypedDict) -> None:
        if entry["status"] == EntryStatus.FAILED:
            self.error_entries.append(entry)
            if self.error_output_adapter:
                with self.error_output_adapter as error_output_adapter:
                    error_output_adapter.write(entry)
            return entry

        self.output_adapter.write(entry)
        return entry

    def run(self) -> typing.Generator[EntryTypedDict, None, None]:
        with (
            self.input_adapter as input_adapter,
            self.output_adapter,
        ):
            self.executor.add_raw_data_entries(input_adapter.generator)
            for entry in self.executor.generator:
                yield self._handle_entry(entry)


__all__ = ["SchemaFlow"]
