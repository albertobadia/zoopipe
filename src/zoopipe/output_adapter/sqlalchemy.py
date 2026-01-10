from typing import Any

from sqlalchemy import MetaData, Table, create_engine, insert

from zoopipe.hooks.base import BaseHook
from zoopipe.models.core import EntryTypedDict
from zoopipe.output_adapter.base import BaseOutputAdapter


class SQLAlchemyOutputAdapter(BaseOutputAdapter):
    def __init__(
        self,
        connection_string: str,
        table_name: str,
        batch_size: int = 1000,
        pre_hooks: list[BaseHook] | None = None,
        post_hooks: list[BaseHook] | None = None,
    ):
        super().__init__(pre_hooks=pre_hooks, post_hooks=post_hooks)
        self.connection_string = connection_string
        self.table_name = table_name
        self.batch_size = batch_size
        self._engine = None
        self._table = None
        self._buffer: list[dict[str, Any]] = []

    def open(self) -> None:
        self._engine = create_engine(self.connection_string)
        metadata = MetaData()
        metadata.reflect(bind=self._engine, only=[self.table_name])
        self._table = Table(self.table_name, metadata, autoload_with=self._engine)
        super().open()

    def write(self, entry: EntryTypedDict) -> None:
        if not self._is_opened or self._engine is None:
            raise RuntimeError("Adapter must be opened before writing")

        data = entry.get("validated_data") or entry.get("raw_data")
        if isinstance(data, dict):
            self._buffer.append(data)

        if len(self._buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer or self._engine is None:
            return

        with self._engine.begin() as conn:
            conn.execute(insert(self._table), self._buffer)

        self._buffer.clear()

    def close(self) -> None:
        if self._engine:
            self.flush()
            self._engine.dispose()
            self._engine = None
        super().close()
