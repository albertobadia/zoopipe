import os
from typing import Any, Callable, Iterator

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.models.core import EntryStatus


class FilePartitioner(BaseInputAdapter):
    def __init__(
        self,
        file_path: str,
        num_partitions: int,
        id_generator: Callable[[], Any] | None = None,
    ):
        super().__init__(id_generator=id_generator)
        self.file_path = file_path
        self.num_partitions = num_partitions

    @property
    def generator(self) -> Iterator[dict]:
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")

        file_size = os.path.getsize(self.file_path)
        if file_size == 0:
            return

        actual_partitions = min(self.num_partitions, file_size)
        partition_size = file_size // actual_partitions

        for i in range(actual_partitions):
            start = i * partition_size
            end = file_size if i == actual_partitions - 1 else (i + 1) * partition_size

            yield {
                "id": self.id_generator(),
                "position": i,
                "status": EntryStatus.PENDING,
                "raw_data": {
                    "path": self.file_path,
                    "start": start,
                    "end": end,
                    "partition_id": i,
                },
                "validated_data": {},
                "errors": [],
                "metadata": {},
            }
