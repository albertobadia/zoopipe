import pathlib
import typing

import pyarrow as pa
import pyarrow.parquet as pq

from zoopipe.models.core import EntryTypedDict
from zoopipe.output_adapter.base import BaseOutputAdapter


class ArrowOutputAdapter(BaseOutputAdapter):
    def __init__(
        self,
        output: typing.Union[str, pathlib.Path],
        format: str = "parquet",
        batch_size: int = 1000,
        schema: pa.Schema | None = None,
        **writer_options,
    ):
        super().__init__()
        self.output_path = pathlib.Path(output)
        self.format = format
        self.batch_size = batch_size
        self.schema = schema
        self.writer_options = writer_options

        self._buffer: list[dict[str, typing.Any]] = []
        self._writer = None
        self._pa_schema = schema

    def open(self) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        super().open()

    def _flush(self) -> None:
        if not self._buffer:
            return

        table = pa.Table.from_pylist(self._buffer, schema=self._pa_schema)
        if self._pa_schema is None:
            self._pa_schema = table.schema

        if self.format == "parquet":
            if self._writer is None:
                self._writer = pq.ParquetWriter(
                    str(self.output_path), self._pa_schema, **self.writer_options
                )
            self._writer.write_table(table)
        else:
            raise ValueError(f"Unsupported format: {self.format}")

        self._buffer = []

    def write(self, entry: EntryTypedDict) -> None:
        if not self._is_opened:
            raise RuntimeError(
                "Adapter must be opened before writing.\n"
                "Use 'with adapter:' or call adapter.open()"
            )

        row = entry.get("validated_data") or entry.get("raw_data") or {}
        # We might want to include some metadata or ID, but usually for Parquet
        # users want the records themselves.
        self._buffer.append(row)

        if len(self._buffer) >= self.batch_size:
            self._flush()

    def close(self) -> None:
        self._flush()
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        super().close()
