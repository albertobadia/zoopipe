import abc
import typing


class BaseOutputAdapter(abc.ABC):
    @abc.abstractmethod
    def get_native_writer(self) -> typing.Any:
        raise NotImplementedError
