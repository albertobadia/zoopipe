from unittest.mock import MagicMock

from zoopipe.hooks.base import BaseHook, HookPriority
from zoopipe.hooks.sql import SQLExpansionHook
from zoopipe.structs import EntryStatus


def test_hook_priority_constants():
    assert HookPriority.VERY_HIGH == 0
    assert HookPriority.HIGH == 25
    assert HookPriority.NORMAL == 50
    assert HookPriority.LOW == 75
    assert HookPriority.VERY_LOW == 100


def test_base_hook_initialization_with_default_priority():
    hook = BaseHook()
    assert hook.priority == HookPriority.NORMAL


def test_base_hook_initialization_with_custom_priority():
    hook = BaseHook(priority=HookPriority.HIGH)
    assert hook.priority == HookPriority.HIGH


def test_base_hook_setup_default():
    hook = BaseHook()
    store = {}

    hook.setup(store)


def test_base_hook_execute_returns_entries_unchanged():
    hook = BaseHook()
    entries = [
        {
            "id": 1,
            "position": 0,
            "status": EntryStatus.PENDING,
            "raw_data": {"name": "test"},
            "validated_data": None,
            "errors": [],
            "metadata": {},
        }
    ]
    store = {}

    result = hook.execute(entries, store)

    assert result == entries


def test_base_hook_teardown_default():
    hook = BaseHook()
    store = {}

    hook.teardown(store)


def test_sql_expansion_hook_initialization():
    mock_conn_factory = MagicMock()

    hook = SQLExpansionHook(connection_factory=mock_conn_factory, table_name="users")

    assert hook.connection_factory == mock_conn_factory
    assert hook.table_name == "users"


def test_sql_expansion_hook_execute_expands_id_ranges():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_cursor.description = [("id",), ("name",), ("age",)]
    mock_cursor.fetchall.return_value = [
        (1, "Alice", 30),
        (2, "Bob", 25),
        (3, "Charlie", 35),
    ]

    mock_conn.cursor.return_value = mock_cursor

    mock_conn_factory = MagicMock(return_value=mock_conn)

    hook = SQLExpansionHook(connection_factory=mock_conn_factory, table_name="users")

    anchor_entries = [
        {
            "id": None,
            "position": None,
            "status": EntryStatus.PENDING,
            "raw_data": {"min_id": 1, "max_id": 3},
            "validated_data": None,
            "errors": [],
            "metadata": {"source": "batch_1"},
        }
    ]

    result = hook.execute(anchor_entries, {})

    assert len(result) == 3
    assert result[0]["raw_data"]["id"] == 1
    assert result[0]["raw_data"]["name"] == "Alice"
    assert result[0]["metadata"]["source"] == "batch_1"
    assert result[1]["raw_data"]["id"] == 2
    assert result[2]["raw_data"]["id"] == 3


def test_sql_expansion_hook_handles_missing_min_id():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_conn_factory = MagicMock(return_value=mock_conn)

    hook = SQLExpansionHook(connection_factory=mock_conn_factory, table_name="users")

    anchor_entries = [
        {
            "id": None,
            "position": None,
            "status": EntryStatus.PENDING,
            "raw_data": {"max_id": 3},
            "validated_data": None,
            "errors": [],
            "metadata": {},
        }
    ]

    result = hook.execute(anchor_entries, {})

    assert len(result) == 0
    mock_cursor.execute.assert_not_called()


def test_sql_expansion_hook_handles_missing_max_id():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_conn_factory = MagicMock(return_value=mock_conn)

    hook = SQLExpansionHook(connection_factory=mock_conn_factory, table_name="users")

    anchor_entries = [
        {
            "id": None,
            "position": None,
            "status": EntryStatus.PENDING,
            "raw_data": {"min_id": 1},
            "validated_data": None,
            "errors": [],
            "metadata": {},
        }
    ]

    result = hook.execute(anchor_entries, {})

    assert len(result) == 0
    mock_cursor.execute.assert_not_called()


def test_sql_expansion_hook_closes_connection():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = None
    mock_cursor.fetchall.return_value = []

    mock_conn.cursor.return_value = mock_cursor

    mock_conn_factory = MagicMock(return_value=mock_conn)

    hook = SQLExpansionHook(connection_factory=mock_conn_factory, table_name="users")

    anchor_entries = [
        {
            "id": None,
            "position": None,
            "status": EntryStatus.PENDING,
            "raw_data": {"min_id": 1, "max_id": 3},
            "validated_data": None,
            "errors": [],
            "metadata": {},
        }
    ]

    hook.execute(anchor_entries, {})

    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()


def test_sql_expansion_hook_with_empty_batch():
    mock_conn = MagicMock()
    mock_conn_factory = MagicMock(return_value=mock_conn)

    hook = SQLExpansionHook(connection_factory=mock_conn_factory, table_name="users")

    result = hook.execute([], {})

    assert len(result) == 0
    mock_conn.close.assert_called_once()


def test_sql_expansion_hook_preserves_metadata():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_cursor.description = [("id",)]
    mock_cursor.fetchall.return_value = [(1,), (2,)]

    mock_conn.cursor.return_value = mock_cursor
    mock_conn_factory = MagicMock(return_value=mock_conn)

    hook = SQLExpansionHook(connection_factory=mock_conn_factory, table_name="users")

    anchor_entries = [
        {
            "id": None,
            "position": None,
            "status": EntryStatus.PENDING,
            "raw_data": {"min_id": 1, "max_id": 2},
            "validated_data": None,
            "errors": [],
            "metadata": {"batch_id": "batch_123", "source": "migration"},
        }
    ]

    result = hook.execute(anchor_entries, {})

    assert len(result) == 2
    assert result[0]["metadata"]["batch_id"] == "batch_123"
    assert result[0]["metadata"]["source"] == "migration"
    assert result[1]["metadata"]["batch_id"] == "batch_123"
