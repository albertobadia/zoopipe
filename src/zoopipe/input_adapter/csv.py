import pathlib
import typing
import uuid

from zoopipe.hooks.base import BaseHook
from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.models.core import EntryStatus, EntryTypedDict
from zoopipe.utils.parsing import parse_csv


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
        pre_hooks: list[BaseHook] | None = None,
        post_hooks: list[BaseHook] | None = None,
        **csv_options,
    ):
        super().__init__(
            id_generator=id_generator, pre_hooks=pre_hooks, post_hooks=post_hooks
        )

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

    def open(self) -> None:
        if not self.source_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.source_path}")
        if not self.source_path.is_file():
            raise ValueError(f"Path is not a file: {self.source_path}")
        self._file_handle = open(self.source_path, mode="rb")
        super().open()

    def close(self) -> None:
        if self._file_handle is not None:
            self._file_handle.close()
            self._file_handle = None
        super().close()

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        try:
            reader = parse_csv(
                self._file_handle,
                delimiter=self.delimiter,
                quotechar=self.quotechar,
                encoding=self.encoding,
                skip_rows=self.skip_rows,
                fieldnames=self.fieldnames,
                **self.csv_options,
            )
            for row_num, row in enumerate(reader, start=1):
                if self.max_rows is not None and row_num > self.max_rows:
                    break
                yield EntryTypedDict(
                    id=self.id_generator(),
                    raw_data=row,
                    validated_data=None,
                    position=row_num - 1 + self.skip_rows,
                    status=EntryStatus.PENDING,
                    errors=[],
                    metadata={},
                )
        except Exception as e:
            raise RuntimeError(f"Error reading CSV: {e}") from e


__all__ = ["CSVInputAdapter"]
