import json
import pathlib
import typing

import ijson

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.models.core import EntryStatus, EntryTypedDict


class JSONInputAdapter(BaseInputAdapter):
    def __init__(
        self,
        source: typing.Union[str, pathlib.Path],
        format: str = "array",
        prefix: str = "item",
        encoding: str = "utf-8",
        max_items: int | None = None,
        id_generator: typing.Callable[[], typing.Any] | None = None,
    ):
        super().__init__(id_generator=id_generator)

        self.source_path = pathlib.Path(source)
        self.format = format
        self.prefix = prefix
        self.encoding = encoding
        self.max_items = max_items

        self._file_handle = None
        self._item_count = 0

        if format not in ["array", "jsonl"]:
            raise ValueError(f"Invalid format: {format}. Must be 'array' or 'jsonl'")

    def open(self) -> None:
        if not self.source_path.exists():
            raise FileNotFoundError(f"JSON file not found: {self.source_path}")

        if not self.source_path.is_file():
            raise ValueError(f"Path is not a file: {self.source_path}")

        self._file_handle = open(self.source_path, mode="rb")
        self._item_count = 0

        super().open()

    def close(self) -> None:
        if self._file_handle is not None:
            self._file_handle.close()
            self._file_handle = None

        super().close()

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        if not self._is_opened or self._file_handle is None:
            raise RuntimeError(
                "Adapter must be opened before reading.\n"
                "Use 'with adapter:' or call adapter.open()"
            )

        try:
            if self.format == "jsonl":
                yield from self._generate_from_jsonl()
            else:
                yield from self._generate_from_array()

        except ijson.JSONError as e:
            raise ValueError(f"Error parsing JSON: {e}") from e
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing JSON at line {e.lineno}: {e}") from e

    def _generate_from_array(self) -> typing.Generator[EntryTypedDict, None, None]:
        parser = ijson.items(self._file_handle, self.prefix)

        for item in parser:
            if self.max_items is not None and self._item_count >= self.max_items:
                break

            yield EntryTypedDict(
                id=self.id_generator(),
                raw_data=item,
                validated_data=None,
                position=self._item_count,
                status=EntryStatus.PENDING,
                errors=[],
                metadata={},
            )
            self._item_count += 1

    def _generate_from_jsonl(self) -> typing.Generator[EntryTypedDict, None, None]:
        for line_num, line_bytes in enumerate(self._file_handle):
            if self.max_items is not None and self._item_count >= self.max_items:
                break

            line = line_bytes.decode(self.encoding).strip()
            if not line:
                continue

            try:
                item = json.loads(line)
                yield EntryTypedDict(
                    id=self.id_generator(),
                    raw_data=item,
                    validated_data=None,
                    position=self._item_count,
                    status=EntryStatus.PENDING,
                    errors=[],
                    metadata={},
                )
                self._item_count += 1
            except json.JSONDecodeError as e:
                raise ValueError(f"Error parsing JSON line {line_num + 1}: {e}") from e
