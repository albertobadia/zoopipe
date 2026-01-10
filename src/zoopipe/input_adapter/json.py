import pathlib
import typing

from zoopipe.hooks.base import BaseHook
from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.models.core import EntryStatus, EntryTypedDict
from zoopipe.utils.parsing import parse_json


class JSONInputAdapter(BaseInputAdapter):
    def __init__(
        self,
        source: typing.Union[str, pathlib.Path],
        format: str = "array",
        prefix: str = "item",
        encoding: str = "utf-8",
        max_items: int | None = None,
        id_generator: typing.Callable[[], typing.Any] | None = None,
        pre_hooks: list["BaseHook"] | None = None,
        post_hooks: list["BaseHook"] | None = None,
    ):
        super().__init__(
            id_generator=id_generator, pre_hooks=pre_hooks, post_hooks=post_hooks
        )
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
            reader = parse_json(
                self._file_handle,
                format=self.format,
                prefix=self.prefix,
                encoding=self.encoding,
            )
            for row in reader:
                if self.max_items is not None and self._item_count >= self.max_items:
                    break
                yield EntryTypedDict(
                    id=self.id_generator(),
                    raw_data=row,
                    validated_data=None,
                    position=self._item_count,
                    status=EntryStatus.PENDING,
                    errors=[],
                    metadata={},
                )
                self._item_count += 1
        except Exception as e:
            raise ValueError(f"Error parsing JSON: {e}") from e


__all__ = ["JSONInputAdapter"]
