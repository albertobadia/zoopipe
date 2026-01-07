import csv
import pathlib
import typing

from schemaflow.models.core import EntryTypedDict
from schemaflow.output_adapter.base import BaseOutputAdapter


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

        # Determine data to write
        # Priority: validated_data > raw_data
        data = entry.get("validated_data") or entry.get("raw_data") or {}

        if self.include_metadata:
            # Flatten or include specific metadata if needed
            # For now, let's just include id and status
            data["__id"] = str(entry["id"])
            data["__status"] = entry["status"].value

        if self._csv_writer is None:
            # Initialize writer with keys from the first entry
            self.fieldnames = list(data.keys())
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
