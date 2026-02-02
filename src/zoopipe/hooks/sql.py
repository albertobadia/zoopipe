import typing

from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.report import get_logger
from zoopipe.structs import EntryStatus

if typing.TYPE_CHECKING:
    from zoopipe.structs import EntryTypedDict


class SQLExpansionHook(BaseHook):
    """
    Expands anchor records (e.g., ID ranges) into full records by querying a SQL table.

    This hook is designed to work with chunked data ingestion. It takes minimal
    identifying information (anchors) and performs a bulk fetch from the database
    to retrieve the complete rows.
    """

    def __init__(
        self, connection_factory: typing.Callable[[], typing.Any], table_name: str
    ):
        """
        Initialize the SQLExpansionHook.

        Args:
            connection_factory: Callable that returns a database connection.
            table_name: Name of the SQL table to fetch data from.
        """
        super().__init__()
        self.connection_factory = connection_factory
        self.table_name = table_name
        self.logger = get_logger()

    def setup(self, store: HookStore) -> None:
        pass

    def execute(
        self, entries: list["EntryTypedDict"], store: HookStore
    ) -> list["EntryTypedDict"]:
        expanded = []
        conn = self.connection_factory()

        try:
            cursor = conn.cursor()
            self.logger.debug(
                f"SQLExpansionHook: Expanding batch of {len(entries)} anchor(s)"
            )

            for anchor in entries:
                raw = anchor["raw_data"]
                min_id = raw.get("min_id")
                max_id = raw.get("max_id")

                if min_id is None or max_id is None:
                    continue

                cursor.execute(
                    f"SELECT * FROM {self.table_name} WHERE id BETWEEN ? AND ?",
                    (min_id, max_id),
                )

                columns = (
                    [column[0] for column in cursor.description]
                    if cursor.description
                    else []
                )

                rows = cursor.fetchall()

                for row in rows:
                    if columns:
                        data = dict(zip(columns, row))
                    else:
                        data = dict(row)

                    expanded.append(
                        {
                            "id": None,
                            "position": None,
                            "status": EntryStatus.PENDING,
                            "raw_data": data,
                            "validated_data": None,
                            "metadata": anchor["metadata"],
                            "errors": [],
                        }
                    )
            cursor.close()
        finally:
            conn.close()

        return expanded

    def teardown(self, store: HookStore) -> None:
        pass
