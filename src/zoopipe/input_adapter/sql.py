import typing

from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.zoopipe_rust_core import SQLReader


class SQLInputAdapter(BaseInputAdapter):
    """
    Streams records from SQL databases using standard queries.

    Supports any database compatible with SQLAlchemy URIs. It executes a
    provided query or fetches a whole table using a native Rust executor
    for optimal performance.
    """

    def __init__(
        self,
        uri: str,
        query: str | None = None,
        table_name: str | None = None,
        generate_ids: bool = True,
    ):
        """
        Initialize the SQLInputAdapter.

        Args:
            uri: Database URI (e.g., 'sqlite:///data.db').
            query: SQL query to execute.
            table_name: Or name of the table to read (equiv to SELECT * FROM table).
            generate_ids: Whether to generate unique IDs for each record.
        """
        self.uri = uri
        self.generate_ids = generate_ids

        if query is None and table_name is None:
            raise ValueError("Either query or table_name must be provided")

        if query is not None and table_name is not None:
            raise ValueError("Only one of query or table_name should be provided")

        if query is not None:
            self.query = query
        else:
            self.query = f"SELECT * FROM {table_name}"

    def get_native_reader(self) -> SQLReader:
        return SQLReader(
            self.uri,
            self.query,
            generate_ids=self.generate_ids,
        )


class SQLPaginationInputAdapter(SQLInputAdapter):
    """
    Input adapter for SQL databases using anchor-based pagination.

    This adapter generates ID ranges (anchors) and utilizes SQLExpansionHook
    to fetch full records in chunks, which is more efficient for very large tables.
    """

    def __init__(
        self,
        uri: str,
        table_name: str,
        id_column: str,
        chunk_size: int,
        connection_factory: typing.Callable[[], typing.Any],
    ):
        """
        Initialize the SQLPaginationInputAdapter.

        Args:
            uri: Database URI.
            table_name: Name of the table to read.
            id_column: Primary key or indexed column used for pagination.
            chunk_size: Number of records to fetch per chunk.
            connection_factory: Callable that returns a database connection
                for the hook.
        """
        self.table_name = table_name
        self.id_column = id_column
        self.chunk_size = chunk_size
        self.connection_factory = connection_factory

        query = f"""
        WITH RECURSIVE ranges(n) AS (
            SELECT MIN({id_column}) FROM {table_name}
            UNION ALL
            SELECT n + {chunk_size} FROM ranges 
            WHERE n + {chunk_size} <= (SELECT MAX({id_column}) FROM {table_name})
        )
        SELECT n as min_id, n + {chunk_size} - 1 as max_id FROM ranges
        """
        super().__init__(uri, query=query)

    def get_hooks(self):
        from zoopipe.hooks.sql import SQLExpansionHook

        return [SQLExpansionHook(self.connection_factory, self.table_name)]


__all__ = ["SQLInputAdapter", "SQLPaginationInputAdapter"]
