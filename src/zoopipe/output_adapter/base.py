import abc
import logging

from zoopipe.models.core import EntryTypedDict


class BaseOutputAdapter(abc.ABC):
    _is_opened: bool = False
    logger: logging.Logger | None = None

    def set_logger(self, logger: logging.Logger) -> None:
        self.logger = logger

    @abc.abstractmethod
    def write(self, entry: EntryTypedDict) -> None:
        raise NotImplementedError("Subclasses must implement the write method")

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

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>"
