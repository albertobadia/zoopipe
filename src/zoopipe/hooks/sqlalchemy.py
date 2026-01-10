from sqlalchemy import MetaData, Table, create_engine, select

from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.models.core import EntryStatus, EntryTypedDict


class SQLAlchemyFetchHook(BaseHook):
    def __init__(self, connection_string: str):
        super().__init__()
        self.connection_string = connection_string

    def setup(self, store: HookStore) -> None:
        engine = create_engine(self.connection_string)
        conn = engine.connect()

        metadata = MetaData()
        store["sql_engine"] = engine
        store["sql_connection"] = conn
        store["sql_metadata"] = metadata

    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        conn = store.get("sql_connection")
        metadata = store.get("sql_metadata")
        if not conn or metadata is None:
            raise RuntimeError(
                "SQLAlchemyFetchHook resources not found in store. Was setup called?"
            )

        new_entries = []
        for entry in entries:
            meta = entry.get("metadata", {})
            if not meta.get("is_sql_range_metadata"):
                new_entries.append(entry)
                continue

            table_name = meta["table"]
            table = Table(table_name, metadata, autoload_with=conn.engine)

            pk_column = meta.get("pk_column")
            pk_start = meta.get("pk_start")
            pk_end = meta.get("pk_end")

            if pk_column and pk_start is not None:
                pk_col = table.c[pk_column]
                stmt = select(table).where(pk_col >= pk_start)
                if pk_end is not None:
                    stmt = stmt.where(pk_col < pk_end)

                result = conn.execute(stmt)
            else:
                offset = meta.get("batch_offset", 0)
                limit = meta.get("batch_limit", 1000)
                stmt = select(table).limit(limit).offset(offset)
                result = conn.execute(stmt)

            columns = result.keys()

            for i, row in enumerate(result):
                row_data = dict(zip(columns, row))
                pos = (meta.get("batch_offset", 0) + i) if "batch_offset" in meta else i

                new_entries.append(
                    EntryTypedDict(
                        id=f"{entry['id']}_{i}",
                        raw_data=row_data,
                        validated_data=None,
                        position=pos,
                        status=EntryStatus.PENDING,
                        errors=[],
                        metadata=entry["metadata"].copy(),
                    )
                )

        return new_entries

    def teardown(self, store: HookStore) -> None:
        conn = store.get("sql_connection")
        if conn:
            conn.close()

        engine = store.get("sql_engine")
        if engine:
            engine.dispose()

        store.pop("sql_connection", None)
        store.pop("sql_engine", None)
        store.pop("sql_metadata", None)
