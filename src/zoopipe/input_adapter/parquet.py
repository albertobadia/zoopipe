import pathlib
import typing

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.zoopipe_rust_core import ParquetReader


class ParquetInputAdapter(BaseInputAdapter):
    """
    Reads records from Apache Parquet files.

    Utilizes the Arrow ecosystem for efficient columnar data reading and
    multi-threaded loading.
    """

    def __init__(
        self,
        source: typing.Union[str, pathlib.Path],
        generate_ids: bool = True,
        batch_size: int = 1024,
        limit: int | None = None,
        offset: int = 0,
        row_groups: typing.List[int] | None = None,
    ):
        """
        Initialize the ParquetInputAdapter.

        Args:
            source: Path to the Parquet file.
            generate_ids: Whether to generate unique IDs for each record.
            batch_size: Number of records to read at once from the file.
            limit: Maximum number of rows to read.
            offset: Number of rows to skip.
        """
        self.source_path = str(source)
        self.generate_ids = generate_ids
        self.batch_size = batch_size
        self.limit = limit
        self.offset = offset
        self.row_groups = row_groups

    def split(self, workers: int) -> typing.List["ParquetInputAdapter"]:
        """
        Split the Parquet input into `workers` shards based on Row Groups.
        """
        row_group_rows = ParquetReader.get_row_groups_info(self.source_path)
        num_groups = len(row_group_rows)
        
        if num_groups < workers:
            workers = num_groups
            
        if workers <= 1:
            return [self]

        # Distribute row groups among workers
        groups_per_worker = num_groups // workers
        shards = []
        for i in range(workers):
            start_idx = i * groups_per_worker
            end_idx = (i + 1) * groups_per_worker if i < workers - 1 else num_groups
            
            assigned_groups = list(range(start_idx, end_idx))
            
            shards.append(
                self.__class__(
                    source=self.source_path,
                    generate_ids=self.generate_ids,
                    batch_size=self.batch_size,
                    row_groups=assigned_groups,
                )
            )
        return shards

    def get_native_reader(self) -> ParquetReader:
        return ParquetReader(
            self.source_path,
            generate_ids=self.generate_ids,
            batch_size=self.batch_size,
            limit=self.limit,
            offset=self.offset,
            row_groups=self.row_groups,
        )


__all__ = ["ParquetInputAdapter"]
