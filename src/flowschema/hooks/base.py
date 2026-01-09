import typing

from flowschema.models.core import EntryTypedDict


class HookStore:
    def __init__(self):
        self._data: dict[str, typing.Any] = {}
        self._read_only = False

    def _lock(self) -> None:
        self._read_only = True

    def _unlock(self) -> None:
        self._read_only = False

    def __setattr__(self, name: str, value: typing.Any) -> None:
        if name.startswith("_"):
            super().__setattr__(name, value)
        else:
            if getattr(self, "_read_only", False):
                raise RuntimeError(
                    f"HookStore is read-only during execute(). "
                    f"Cannot set '{name}'. Use setup() for initialization."
                )
            self._data[name] = value

    def __getattr__(self, name: str) -> typing.Any:
        if name in self._data:
            return self._data[name]
        raise AttributeError(f"HookStore has no attribute '{name}'")

    def __contains__(self, name: str) -> bool:
        return name in self._data

    def get(self, name: str, default: typing.Any = None) -> typing.Any:
        return self._data.get(name, default)


class BaseHook:
    def setup(self, store: HookStore) -> None:
        pass

    def execute(
        self, entry: EntryTypedDict, store: HookStore
    ) -> dict[str, typing.Any] | None:
        raise NotImplementedError("Subclasses must implement execute()")

    def teardown(self, store: HookStore) -> None:
        pass
