import abc
import typing

from schemaflow.models.core import EntryTypedDict


class BaseExecutor(abc.ABC):
    _raw_data_entries: list[EntryTypedDict] = None

    @property
    def raw_data_entries(self) -> list[EntryTypedDict]:
        if self._raw_data_entries is None:
            self._raw_data_entries = []
        return self._raw_data_entries

    def add_raw_data_entries(
        self, tasks: typing.Generator[EntryTypedDict, None, None]
    ) -> None:
        for task in tasks:
            self.raw_data_entries.append(task)

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        raise NotImplementedError


__all__ = ["BaseExecutor"]
