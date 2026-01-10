import json
import pathlib
import typing

from zoopipe.models.core import EntryTypedDict
from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.utils import JSONEncoder


class JSONOutputAdapter(BaseOutputAdapter):
    def __init__(
        self,
        output: typing.Union[str, pathlib.Path],
        format: str = "array",
        encoding: str = "utf-8",
        include_metadata: bool = False,
        indent: int | None = None,
    ):
        super().__init__()
        self.output_path = pathlib.Path(output)
        self.format = format
        self.encoding = encoding
        self.include_metadata = include_metadata
        self.indent = indent

        self._file_handle = None
        self._is_first_item = True

        if format not in ["array", "jsonl"]:
            raise ValueError(f"Invalid format: {format}. Must be 'array' or 'jsonl'")

    def open(self) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        self._file_handle = open(self.output_path, mode="w", encoding=self.encoding)
        self._is_first_item = True

        if self.format == "array":
            self._file_handle.write("[")
            if self.indent is not None:
                self._file_handle.write("\n")

        super().open()

    def close(self) -> None:
        if self._file_handle is not None:
            if self.format == "array":
                if self.indent is not None:
                    self._file_handle.write("\n")
                self._file_handle.write("]")

            self._file_handle.close()
            self._file_handle = None
            self._is_first_item = True

        super().close()

    def write(self, entry: EntryTypedDict) -> None:
        if not self._is_opened or self._file_handle is None:
            raise RuntimeError(
                "Adapter must be opened before writing.\n"
                "Use 'with adapter:' or call adapter.open()"
            )

        record = entry.get("validated_data") or entry.get("raw_data") or {}
        data = {
            "id": entry["id"],
            "status": entry["status"],
            "position": entry["position"],
            "metadata": entry["metadata"],
            "data": record,
        }

        json_str = json.dumps(data, cls=JSONEncoder, indent=self.indent)

        if self.format == "jsonl":
            self._file_handle.write(json_str)
            self._file_handle.write("\n")
        else:
            if not self._is_first_item:
                self._file_handle.write(",")
                if self.indent is not None:
                    self._file_handle.write("\n")

            if self.indent is not None:
                indented_lines = "\n".join(
                    " " * self.indent + line if line else line
                    for line in json_str.split("\n")
                )
                self._file_handle.write(indented_lines)
            else:
                self._file_handle.write(json_str)

            self._is_first_item = False
