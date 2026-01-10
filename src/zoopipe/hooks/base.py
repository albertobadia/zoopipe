import typing

from zoopipe.models.core import EntryTypedDict

HookStore = dict[str, typing.Any]


class BaseHook:
    def setup(self, store: HookStore) -> None:
        pass

    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        return entries

    def teardown(self, store: HookStore) -> None:
        pass
