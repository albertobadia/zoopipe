import pathlib
import typing

import pyarrow.dataset as ds

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.models.core import EntryStatus, EntryTypedDict


class ArrowInputAdapter(BaseInputAdapter):
    def __init__(
        self,
        source: typing.Union[str, pathlib.Path, list[str], list[pathlib.Path]],
        format: str = "parquet",
        partitioning: str | ds.Partitioning | None = None,
        columns: list[str] | None = None,
        filter: ds.Expression | None = None,
        id_generator: typing.Callable[[], typing.Any] | None = None,
        **dataset_options,
    ):
        super().__init__(id_generator=id_generator)

        self.source = source
        self.format = format
        self.partitioning = partitioning
        self.columns = columns
        self.filter = filter
        self.dataset_options = dataset_options

        self._dataset = None
        self._scanner = None

    def open(self) -> None:
        self._dataset = ds.dataset(
            self.source,
            format=self.format,
            partitioning=self.partitioning,
            **self.dataset_options,
        )
        self._scanner = self._dataset.scanner(
            columns=self.columns,
            filter=self.filter,
        )
        super().open()

    def close(self) -> None:
        self._dataset = None
        self._scanner = None
        super().close()

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        if not self._is_opened or self._scanner is None:
            raise RuntimeError(
                "Adapter must be opened before reading.\n"
                "Use 'with adapter:' or call adapter.open()"
            )

        row_num = 0
        for batch in self._scanner.to_batches():
            rows = batch.to_pylist()
            for row in rows:
                yield EntryTypedDict(
                    id=self.id_generator(),
                    raw_data=row,
                    validated_data=None,
                    position=row_num,
                    status=EntryStatus.PENDING,
                    errors=[],
                    metadata={},
                )
                row_num += 1
