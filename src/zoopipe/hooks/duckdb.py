import duckdb

from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.models.core import EntryStatus, EntryTypedDict


class DuckDBFetchHook(BaseHook):
    def __init__(self, database: str = ":memory:"):
        super().__init__()
        self.database = database

    def setup(self, store: HookStore) -> None:
        conn = duckdb.connect(self.database)
        conn.execute("PRAGMA threads=8")
        conn.execute("PRAGMA memory_limit='2GB'")
        store["duckdb_connection"] = conn

    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        conn = store.get("duckdb_connection")
        if not conn:
            conn = duckdb.connect(self.database)
            store["duckdb_connection"] = conn

        new_entries = []
        for entry in entries:
            meta = entry.get("metadata", {})
            if not meta.get("is_duckdb_range_metadata"):
                new_entries.append(entry)
                continue

            table_name = meta["table"]
            quoted_table = f'"{table_name}"'

            pk_column = meta.get("pk_column")
            pk_start = meta.get("pk_start")
            pk_end = meta.get("pk_end")

            if pk_column and pk_start is not None:
                quoted_pk = f'"{pk_column}"'
                query = f"SELECT * FROM {quoted_table} WHERE {quoted_pk} >= ?"
                params = [pk_start]
                if pk_end is not None:
                    query += f" AND {quoted_pk} < ?"
                    params.append(pk_end)

                result = conn.execute(query, params).fetchall()
                columns = [desc[0] for desc in conn.description]
            else:
                offset = meta.get("batch_offset", 0)
                limit = meta.get("batch_limit", 1000)
                query = f"SELECT * FROM {quoted_table} LIMIT ? OFFSET ?"
                result = conn.execute(query, [limit, offset]).fetchall()
                columns = [desc[0] for desc in conn.description]

            new_entries.extend(
                [
                    EntryTypedDict(
                        id=f"{entry['id']}_{i}",
                        raw_data=dict(zip(columns, row)),
                        validated_data=None,
                        position=(meta.get("batch_offset", 0) + i)
                        if "batch_offset" in meta
                        else i,
                        status=EntryStatus.PENDING,
                        errors=[],
                        metadata=entry["metadata"].copy(),
                    )
                    for i, row in enumerate(result)
                ]
            )

        return new_entries

    def teardown(self, store: HookStore) -> None:
        conn = store.get("duckdb_connection")
        if conn:
            conn.close()
        store.pop("duckdb_connection", None)
