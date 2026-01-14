import typing

from zoopipe.report import EntryTypedDict

HookStore = dict[str, typing.Any]


class HookPriority:
    VERY_HIGH = 0
    HIGH = 25
    NORMAL = 50
    LOW = 75
    VERY_LOW = 100


class BaseHook:
    def __init__(self, priority: int = HookPriority.NORMAL):
        self.priority = priority

    def setup(self, store: HookStore) -> None:
        pass

    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        return entries

    def teardown(self, store: HookStore) -> None:
        pass
