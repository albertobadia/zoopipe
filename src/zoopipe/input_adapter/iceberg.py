import typing

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.zoopipe_rust_core import MultiParquetReader, get_iceberg_data_files


class IcebergInputAdapter(BaseInputAdapter):
    """
    Adapter for reading from Iceberg tables.
    Discovers data files via Iceberg metadata and reads them using MultiParquetReader.
    """

    def __init__(
        self,
        table_location: str,
        files: typing.List[str] | None = None,
        generate_ids: bool = True,
        batch_size: int = 1024,
    ):
        super().__init__()
        self.table_location = table_location
        if files is None:
            self.files = get_iceberg_data_files(table_location)
        else:
            self.files = files

        self.generate_ids = generate_ids
        self.batch_size = batch_size

    def split(self, workers: int) -> typing.List["IcebergInputAdapter"]:
        """
        Split the data files among workers.
        """
        if not self.files:
            return [self]

        num_files = len(self.files)
        if num_files < workers:
            workers = num_files

        if workers <= 1:
            return [self]

        files_per_worker = num_files // workers
        shards = []
        for i in range(workers):
            start = i * files_per_worker
            end = (i + 1) * files_per_worker if i < workers - 1 else num_files
            assigned_files = self.files[start:end]

            shard = self.__class__(
                table_location=self.table_location,
                files=assigned_files,
                generate_ids=self.generate_ids,
                batch_size=self.batch_size,
            )
            shard.required_columns = self.required_columns
            shards.append(shard)
        return shards

    def get_native_reader(self) -> MultiParquetReader:
        return MultiParquetReader(
            paths=self.files,
            generate_ids=self.generate_ids,
            batch_size=self.batch_size,
            projection=self.required_columns,
        )


__all__ = ["IcebergInputAdapter"]
