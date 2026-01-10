import math
import uuid
from typing import Any, Callable, Generator

import duckdb

from zoopipe.hooks.base import BaseHook
from zoopipe.hooks.duckdb import DuckDBFetchHook
from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.models.core import EntryStatus, EntryTypedDict


class DuckDBInputAdapter(BaseInputAdapter):
    def __init__(
        self,
        database: str,
        table_name: str,
        batch_size: int = 1000,
        pk_column: str = "id",
        total_rows: int | None = None,
        id_generator: Callable[[], Any] | None = None,
        pre_hooks: list[BaseHook] | None = None,
        post_hooks: list[BaseHook] | None = None,
        auto_inject_fetch_hook: bool = True,
    ):
        _pre_hooks = pre_hooks or []

        if auto_inject_fetch_hook:
            if not any(isinstance(h, DuckDBFetchHook) for h in _pre_hooks):
                _pre_hooks.append(DuckDBFetchHook(database=database))

        super().__init__(
            id_generator=id_generator, pre_hooks=_pre_hooks, post_hooks=post_hooks
        )
        self.database = database
        self.table_name = table_name
        self.batch_size = batch_size
        self.pk_column = pk_column
        self.total_rows = total_rows
        self.id_generator = id_generator or uuid.uuid4

    @property
    def generator(self) -> Generator[EntryTypedDict, None, None]:
        if not self._is_opened:
            raise RuntimeError(
                "Adapter must be opened before reading.\n"
                "Use 'with adapter:' or call adapter.open()"
            )

        conn = duckdb.connect(self.database)
        quoted_table = f'"{self.table_name}"'

        try:
            if self.total_rows is None:
                self.total_rows = conn.execute(
                    f"SELECT COUNT(*) FROM {quoted_table}"
                ).fetchone()[0]

            num_batches = math.ceil(self.total_rows / self.batch_size)

            use_pk_ranges = False
            try:
                pk_info = conn.execute(f"DESCRIBE {quoted_table}").fetchall()
                pk_type = next(
                    (col[1] for col in pk_info if col[0] == self.pk_column), None
                )

                use_pk_ranges = pk_type in [
                    "BIGINT",
                    "INTEGER",
                    "DOUBLE",
                    "FLOAT",
                    "HUGEINT",
                    "SMALLINT",
                    "TINYINT",
                ]
            except Exception as e:
                if self.logger:
                    self.logger.warning(
                        f"Could not inspect table schema for optimization: {e}. "
                        "Falling back to OFFSET/LIMIT."
                    )

            if use_pk_ranges:
                quoted_pk = f'"{self.pk_column}"'
                boundary_query = f"""
                SELECT {quoted_pk}
                FROM (
                    SELECT {quoted_pk}, row_number()
                        OVER (ORDER BY {quoted_pk}) as rn
                    FROM {quoted_table}
                ) t
                WHERE (t.rn - 1) % {self.batch_size} = 0
                """
                boundaries = [r[0] for r in conn.execute(boundary_query).fetchall()]

                for i, start_val in enumerate(boundaries):
                    end_val = boundaries[i + 1] if i + 1 < len(boundaries) else None

                    yield EntryTypedDict(
                        id=self.id_generator(),
                        raw_data={},
                        validated_data=None,
                        position=i,
                        status=EntryStatus.PENDING,
                        errors=[],
                        metadata={
                            "table": self.table_name,
                            "pk_column": self.pk_column,
                            "pk_start": start_val,
                            "pk_end": end_val,
                            "is_duckdb_range_metadata": True,
                        },
                    )
            else:
                for i in range(num_batches):
                    yield self._yield_offset_metadata(i)

        finally:
            conn.close()

    def _yield_offset_metadata(self, i: int) -> EntryTypedDict:
        offset = i * self.batch_size
        return EntryTypedDict(
            id=self.id_generator(),
            raw_data={},
            validated_data=None,
            position=i,
            status=EntryStatus.PENDING,
            errors=[],
            metadata={
                "table": self.table_name,
                "batch_offset": offset,
                "batch_limit": self.batch_size,
                "is_duckdb_range_metadata": True,
            },
        )
