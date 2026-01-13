import abc
import typing


class BaseInputAdapter(abc.ABC):
    @abc.abstractmethod
    def get_native_reader(self) -> typing.Any:
        raise NotImplementedError
