import pathlib
import typing

from zoopipe.hooks.base import BaseHook
from zoopipe.models.core import EntryTypedDict
from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import CSVWriter as NativeCSVWriter


class CSVOutputAdapter(BaseOutputAdapter):
    def __init__(
        self,
        output: typing.Union[str, pathlib.Path],
        encoding: str = "utf-8",
        delimiter: str = ",",
        quotechar: str = '"',
        fieldnames: list[str] | None = None,
        include_metadata: bool = True,
        autoflush: bool = True,
        pre_hooks: list["BaseHook"] | None = None,
        post_hooks: list["BaseHook"] | None = None,
        **csv_options,
    ):
        super().__init__(pre_hooks=pre_hooks, post_hooks=post_hooks)
        self.output_path = pathlib.Path(output)
        self.encoding = encoding
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.fieldnames = fieldnames
        self.include_metadata = include_metadata
        self.autoflush = autoflush
        self.csv_options = csv_options

        self._native_writer = None
        self._header_written = False

    def open(self) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        self._native_writer = NativeCSVWriter(
            str(self.output_path),
            delimiter=ord(self.delimiter),
            quote=ord(self.quotechar),
            fieldnames=self.fieldnames,
            include_metadata=self.include_metadata,
        )
        self._header_written = self.fieldnames is not None
        super().open()

    def close(self) -> None:
        if self._native_writer is not None:
            self._native_writer.close()
            self._native_writer = None
            self._header_written = False

        super().close()

    def write(self, entry: EntryTypedDict) -> None:
        if not self._is_opened or self._native_writer is None:
            raise RuntimeError(
                "Adapter must be opened before writing.\n"
                "Use 'with adapter:' or call adapter.open()"
            )

        self._native_writer.write(entry)

    def write_batch(self, entries: list[EntryTypedDict]) -> None:
        if not self._is_opened or self._native_writer is None:
            raise RuntimeError(
                "Adapter must be opened before writing.\n"
                "Use 'with adapter:' or call adapter.open()"
            )

        if not entries:
            return

        self._native_writer.write_batch(entries)

    def flush(self) -> None:
        if self._native_writer is not None:
            self._native_writer.flush()
