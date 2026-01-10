import csv
import pathlib
import typing
import uuid

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.models.core import EntryStatus, EntryTypedDict


class CSVInputAdapter(BaseInputAdapter):
    def __init__(
        self,
        source: typing.Union[str, pathlib.Path],
        encoding: str = "utf-8",
        delimiter: str = ",",
        quotechar: str = '"',
        skip_rows: int = 0,
        max_rows: int | None = None,
        fieldnames: list[str] | None = None,
        id_generator: typing.Callable[[], typing.Any] | None = None,
        **csv_options,
    ):
        super().__init__(id_generator=id_generator)

        self.source_path = pathlib.Path(source)
        self.encoding = encoding
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.skip_rows = skip_rows
        self.max_rows = max_rows
        self.fieldnames = fieldnames
        self.id_generator = id_generator or uuid.uuid4
        self.csv_options = csv_options

        self._file_handle = None
        self._csv_reader = None

    def open(self) -> None:
        if not self.source_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.source_path}")

        if not self.source_path.is_file():
            raise ValueError(f"Path is not a file: {self.source_path}")

        self._file_handle = open(
            self.source_path, mode="r", encoding=self.encoding, newline=""
        )

        for _ in range(self.skip_rows):
            next(self._file_handle, None)

        self._csv_reader = csv.DictReader(
            self._file_handle,
            delimiter=self.delimiter,
            quotechar=self.quotechar,
            fieldnames=self.fieldnames,
            **self.csv_options,
        )

        super().open()

    def close(self) -> None:
        if self._file_handle is not None:
            self._file_handle.close()
            self._file_handle = None
            self._csv_reader = None

        super().close()

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        if not self._is_opened or self._csv_reader is None:
            raise RuntimeError(
                "Adapter must be opened before reading.\n"
                "Use 'with adapter:' or call adapter.open()"
            )

        try:
            for row_num, row in enumerate(self._csv_reader, start=1):
                if self.max_rows is not None and row_num > self.max_rows:
                    break

                data = dict(row)
                yield EntryTypedDict(
                    id=self.id_generator(),
                    raw_data=data,
                    validated_data=None,
                    position=row_num - 1,
                    status=EntryStatus.PENDING,
                    errors=[],
                    metadata={},
                )

        except csv.Error as e:
            raise csv.Error(
                f"Error reading CSV at line {self._csv_reader.line_num}: {e}"
            ) from e
