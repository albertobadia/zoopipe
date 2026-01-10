import csv
import json
import pathlib
import typing

from zoopipe.models.core import EntryTypedDict
from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.utils import JSONEncoder


class CSVOutputAdapter(BaseOutputAdapter):
    def __init__(
        self,
        output: typing.Union[str, pathlib.Path],
        encoding: str = "utf-8",
        delimiter: str = ",",
        quotechar: str = '"',
        fieldnames: list[str] | None = None,
        include_metadata: bool = False,
        **csv_options,
    ):
        super().__init__()
        self.output_path = pathlib.Path(output)
        self.encoding = encoding
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.fieldnames = fieldnames
        self.include_metadata = include_metadata
        self.csv_options = csv_options

        self._file_handle = None
        self._csv_writer = None
        self._header_written = False

    def open(self) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        self._file_handle = open(
            self.output_path, mode="w", encoding=self.encoding, newline=""
        )

        self._csv_writer = None
        if self.fieldnames:
            self._csv_writer = csv.DictWriter(
                self._file_handle,
                fieldnames=self.fieldnames,
                delimiter=self.delimiter,
                quotechar=self.quotechar,
                **self.csv_options,
            )
            self._csv_writer.writeheader()
            self._header_written = True

        super().open()

    def close(self) -> None:
        if self._file_handle is not None:
            self._file_handle.close()
            self._file_handle = None
            self._csv_writer = None
            self._header_written = False

        super().close()

    def write(self, entry: EntryTypedDict) -> None:
        if not self._is_opened or self._file_handle is None:
            raise RuntimeError(
                "Adapter must be opened before writing.\n"
                "Use 'with adapter:' or call adapter.open()"
            )

        record = entry.get("validated_data") or entry.get("raw_data") or {}

        data = {
            "id": str(entry["id"]),
            "status": entry["status"].value,
            "position": entry["position"],
            "metadata": json.dumps(entry["metadata"], cls=JSONEncoder),
        }
        data.update(record)

        if self._csv_writer is None:
            record_keys = list(record.keys())
            self.fieldnames = ["id", "status", "position", "metadata"] + record_keys
            self._csv_writer = csv.DictWriter(
                self._file_handle,
                fieldnames=self.fieldnames,
                delimiter=self.delimiter,
                quotechar=self.quotechar,
                **self.csv_options,
            )
            self._csv_writer.writeheader()
            self._header_written = True

        self._csv_writer.writerow(data)
