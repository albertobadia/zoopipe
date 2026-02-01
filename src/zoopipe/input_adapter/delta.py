import math
from typing import Any, Dict, Iterator, List, Optional

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.zoopipe_rust_core import DeltaReader, get_delta_files


class DeltaInputAdapter(BaseInputAdapter):
    """
    Reads data from a Delta Lake table.

    Supports "Time Travel" via version or timestamp (future work).
    Efficiently reads Parquet files associated with the Delta Log.
    """

    def __init__(
        self,
        table_uri: str,
        version: Optional[int] = None,
        storage_options: Optional[Dict[str, str]] = None,
        batch_size: int = 1000,
        files: Optional[List[str]] = None,
    ):
        """
        Args:
            table_uri: URI of the Delta table (e.g., s3://bucket/table, /local/path).
            version: Optional specific version of the table to read (Time Travel).
            storage_options: Dict of credentials/config for
            cloud storage (AWS, Azure, GCS).
            batch_size: Number of records to yield per batch.
            files: Optional subset of files to read (used for splitting).
        """
        super().__init__()
        self.table_uri = table_uri
        self.version = version
        self.storage_options = storage_options
        self.batch_size = batch_size
        self.files = files
        self._reader: Optional[DeltaReader] = None

    def _get_rust_reader(self) -> Any:
        if not self._reader:
            self._reader = DeltaReader(
                self.table_uri,
                self.version,
                self.storage_options,
                self.batch_size,
                self.files,
                projection=self.required_columns,
            )
        return self._reader

    def get_native_reader(self) -> Any:
        return self._get_rust_reader()

    def get_batches(self) -> Iterator[List[Dict[str, Any]]]:
        reader = self._get_rust_reader()
        while True:
            batch = reader.read_batch(self.batch_size)
            if batch is None:
                break
            yield batch

    def split(self, n: int) -> List["DeltaInputAdapter"]:
        """
        Splits the Delta table into 'n' roughly equal parts by partitioning
        the underlying Parquet files.
        """
        if n <= 1:
            return [self]

        all_files = self.files
        if all_files is None:
            all_files = get_delta_files(
                self.table_uri,
                self.version,
                self.storage_options,
            )

        if not all_files:
            # Empty table or no files found
            shard = DeltaInputAdapter(
                self.table_uri,
                self.version,
                self.storage_options,
                self.batch_size,
                files=[],
            )
            shard.required_columns = self.required_columns
            return [shard] * n

        total_files = len(all_files)
        chunk_size = math.ceil(total_files / n)
        chunks = [
            all_files[i : i + chunk_size] for i in range(0, total_files, chunk_size)
        ]

        adapters = []
        for file_chunk in chunks:
            shard = DeltaInputAdapter(
                self.table_uri,
                self.version,
                self.storage_options,
                self.batch_size,
                files=file_chunk,
            )
            shard.required_columns = self.required_columns
            adapters.append(shard)

        while len(adapters) < n:
            shard = DeltaInputAdapter(
                self.table_uri,
                self.version,
                self.storage_options,
                self.batch_size,
                files=[],
            )
            shard.required_columns = self.required_columns
            adapters.append(shard)

        return adapters
