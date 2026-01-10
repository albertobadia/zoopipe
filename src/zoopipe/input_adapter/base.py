import abc
import logging
import typing
import uuid

from zoopipe.hooks.base import BaseHook


class BaseInputAdapter(abc.ABC):
    def __init__(
        self,
        id_generator: typing.Callable[[], typing.Any] | None = None,
        pre_hooks: list[BaseHook] | None = None,
        post_hooks: list[BaseHook] | None = None,
    ) -> None:
        self.id_generator = id_generator or uuid.uuid4
        self._is_opened: bool = False
        self.logger: logging.Logger | None = None
        self.pre_hooks: list[BaseHook] = pre_hooks or []
        self.post_hooks: list[BaseHook] = post_hooks or []

    def set_logger(self, logger: logging.Logger) -> None:
        self.logger = logger

    @property
    @abc.abstractmethod
    def generator(self) -> typing.Generator[dict[str, typing.Any], None, None]:
        raise NotImplementedError("Subclasses must implement the generator property")

    def open(self) -> None:
        self._is_opened = True

    def close(self) -> None:
        self._is_opened = False

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __iter__(self) -> typing.Generator[dict[str, typing.Any], None, None]:
        return self.generator

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>"
