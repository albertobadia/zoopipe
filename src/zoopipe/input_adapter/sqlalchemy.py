import math
import uuid
from typing import Any, Callable, Generator

from sqlalchemy import MetaData, Table, create_engine, func, select
from sqlalchemy.sql import over

from zoopipe.hooks.base import BaseHook
from zoopipe.hooks.sqlalchemy import SQLAlchemyFetchHook
from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.models.core import EntryStatus, EntryTypedDict


class SQLAlchemyInputAdapter(BaseInputAdapter):
    def __init__(
        self,
        connection_string: str,
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
            if not any(isinstance(h, SQLAlchemyFetchHook) for h in _pre_hooks):
                _pre_hooks.append(
                    SQLAlchemyFetchHook(connection_string=connection_string)
                )

        super().__init__(
            id_generator=id_generator, pre_hooks=_pre_hooks, post_hooks=post_hooks
        )
        self.connection_string = connection_string
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

        engine = create_engine(self.connection_string)
        metadata = MetaData()
        table = Table(self.table_name, metadata, autoload_with=engine)
        pk_col = table.c[self.pk_column]

        with engine.connect() as conn:
            if self.total_rows is None:
                count_stmt = select(func.count()).select_from(table)
                self.total_rows = conn.execute(count_stmt).scalar()

            num_batches = math.ceil(self.total_rows / self.batch_size)

            try:
                pk_stmt = select(func.min(pk_col), func.max(pk_col))
                pk_res = conn.execute(pk_stmt).fetchone()
                min_pk, max_pk = pk_res if pk_res else (None, None)
            except Exception:
                min_pk, max_pk = None, None

            if (
                min_pk is not None
                and max_pk is not None
                and isinstance(min_pk, (int, float))
            ):
                rn_col = over(func.row_number(), order_by=pk_col).label("rn")
                subquery = select(pk_col, rn_col).select_from(table).subquery()

                boundary_stmt = select(subquery.c[self.pk_column]).where(
                    (subquery.c.rn - 1) % self.batch_size == 0
                )

                res = conn.execute(boundary_stmt)
                boundaries = [r[0] for r in res]

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
                            "is_sql_range_metadata": True,
                        },
                    )
            else:
                for i in range(num_batches):
                    offset = i * self.batch_size
                    yield EntryTypedDict(
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
                            "is_sql_range_metadata": True,
                        },
                    )

        engine.dispose()
