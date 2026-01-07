import abc
import typing


class BaseInputAdapter(abc.ABC):
    _is_opened: bool = False

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
