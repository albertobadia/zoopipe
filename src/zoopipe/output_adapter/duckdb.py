from typing import Any

import duckdb

from zoopipe.hooks.base import BaseHook
from zoopipe.output_adapter.base import BaseOutputAdapter


class DuckDBOutputAdapter(BaseOutputAdapter):
    def __init__(
        self,
        database: str,
        table_name: str,
        batch_size: int = 1000,
        pre_hooks: list[BaseHook] | None = None,
        post_hooks: list[BaseHook] | None = None,
    ):
        super().__init__(pre_hooks=pre_hooks, post_hooks=post_hooks)
        self.database = database
        self.table_name = table_name
        self.batch_size = batch_size
        self._conn = None
        self._buffer: list[dict[str, Any]] = []

    def open(self) -> None:
        self._conn = duckdb.connect(self.database)
        self._conn.execute("PRAGMA threads=8")
        super().open()

    def write(self, data: dict[str, Any]) -> None:
        if not self._is_opened or self._conn is None:
            raise RuntimeError("Adapter must be opened before writing")

        if isinstance(data, dict):
            self._buffer.append(data)

        if len(self._buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer or self._conn is None:
            return

        if not self._buffer or self._conn is None:
            return

        keys = list(self._buffer[0].keys())
        placeholders = ", ".join(["?"] * len(keys))
        columns = ", ".join(keys)

        values = []
        for entry in self._buffer:
            values.append(tuple(entry.get(k) for k in keys))

        self._conn.execute("BEGIN TRANSACTION")
        try:
            query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
            self._conn.executemany(query, values)
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

        self._buffer.clear()

    def close(self) -> None:
        if self._conn:
            self.flush()
            self._conn.close()
            self._conn = None
        super().close()
