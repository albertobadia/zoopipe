import pathlib
import typing

from zoopipe.hooks.base import BaseHook
from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import JSONWriter as NativeJSONWriter


class JSONOutputAdapter(BaseOutputAdapter):
    def __init__(
        self,
        output: typing.Union[str, pathlib.Path],
        format: str = "array",
        encoding: str = "utf-8",
        indent: int | None = None,
        pre_hooks: list["BaseHook"] | None = None,
        post_hooks: list["BaseHook"] | None = None,
    ):
        super().__init__(pre_hooks=pre_hooks, post_hooks=post_hooks)
        self.output_path = pathlib.Path(output)
        self.format = format
        self.encoding = encoding
        self.indent = indent

        self._native_writer = None

        if format not in ["array", "jsonl"]:
            raise ValueError(f"Invalid format: {format}. Must be 'array' or 'jsonl'")

    def open(self) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        self._native_writer = NativeJSONWriter(
            str(self.output_path),
            format=self.format,
            indent=self.indent,
        )
        super().open()

    def close(self) -> None:
        if self._native_writer is not None:
            self._native_writer.close()
            self._native_writer = None

        super().close()

    def write(self, data: dict[str, typing.Any]) -> None:
        if not self._is_opened or self._native_writer is None:
            raise RuntimeError(
                "Adapter must be opened before writing.\n"
                "Use 'with adapter:' or call adapter.open()"
            )

        self._native_writer.write(data)

    def write_batch(self, batch: list[dict[str, typing.Any]]) -> None:
        if not self._is_opened or self._native_writer is None:
            raise RuntimeError(
                "Adapter must be opened before writing.\n"
                "Use 'with adapter:' or call adapter.open()"
            )

        if not batch:
            return

        self._native_writer.write_batch(batch)

    def flush(self) -> None:
        if self._native_writer is not None:
            self._native_writer.flush()
