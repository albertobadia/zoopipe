from pathlib import Path
from unittest.mock import MagicMock

from zoopipe.coordinators.base import BaseCoordinator
from zoopipe.coordinators.composite import CompositeCoordinator
from zoopipe.coordinators.merge import FileMergeCoordinator
from zoopipe.structs import WorkerResult


def test_base_coordinator_priority_default():
    class TestCoordinator(BaseCoordinator):
        def prepare_shards(self, adapter, workers):
            return [adapter]

    coordinator = TestCoordinator()
    assert coordinator.priority == 50


def test_base_coordinator_lifecycle_hooks_defaults():
    class TestCoordinator(BaseCoordinator):
        def prepare_shards(self, adapter, workers):
            return [adapter]

    coordinator = TestCoordinator()
    mock_manager = MagicMock()
    mock_results = []
    mock_error = Exception("test")

    coordinator.on_start(mock_manager)
    coordinator.on_finish(mock_manager, mock_results)
    coordinator.on_error(mock_manager, mock_error)


def test_composite_coordinator_sorts_by_priority():
    class HighPriorityCoord(BaseCoordinator):
        @property
        def priority(self):
            return 10

        def prepare_shards(self, adapter, workers):
            return []

    class LowPriorityCoord(BaseCoordinator):
        @property
        def priority(self):
            return 100

        def prepare_shards(self, adapter, workers):
            return []

    high = HighPriorityCoord()
    low = LowPriorityCoord()

    composite = CompositeCoordinator([low, high])

    assert composite.coordinators[0] == high
    assert composite.coordinators[1] == low


def test_composite_coordinator_priority():
    composite = CompositeCoordinator([])
    assert composite.priority == 0


def test_composite_coordinator_prepare_shards_delegates_to_first():
    class FirstCoord(BaseCoordinator):
        def prepare_shards(self, adapter, workers):
            return ["shard1", "shard2"]

    class SecondCoord(BaseCoordinator):
        def prepare_shards(self, adapter, workers):
            return ["should_not_be_used"]

    first = FirstCoord()
    second = SecondCoord()

    composite = CompositeCoordinator([first, second])

    result = composite.prepare_shards("adapter", 2)

    assert result == ["shard1", "shard2"]


def test_composite_coordinator_prepare_shards_returns_adapter_if_no_shards():
    class NoShardCoord(BaseCoordinator):
        def prepare_shards(self, adapter, workers):
            return []

    coord = NoShardCoord()
    composite = CompositeCoordinator([coord])

    result = composite.prepare_shards("test_adapter", 2)

    assert result == ["test_adapter"]


def test_composite_coordinator_on_start_calls_all():
    mock_coord1 = MagicMock(spec=BaseCoordinator)
    mock_coord1.priority = 50
    mock_coord2 = MagicMock(spec=BaseCoordinator)
    mock_coord2.priority = 60

    composite = CompositeCoordinator([mock_coord1, mock_coord2])

    mock_manager = MagicMock()
    composite.on_start(mock_manager)

    mock_coord1.on_start.assert_called_once_with(mock_manager)
    mock_coord2.on_start.assert_called_once_with(mock_manager)


def test_composite_coordinator_on_finish_calls_in_reverse():
    call_order = []

    class Coord1(BaseCoordinator):
        @property
        def priority(self):
            return 10

        def prepare_shards(self, adapter, workers):
            return []

        def on_finish(self, manager, results):
            call_order.append("coord1")

    class Coord2(BaseCoordinator):
        @property
        def priority(self):
            return 20

        def prepare_shards(self, adapter, workers):
            return []

        def on_finish(self, manager, results):
            call_order.append("coord2")

    coord1 = Coord1()
    coord2 = Coord2()

    composite = CompositeCoordinator([coord1, coord2])

    mock_manager = MagicMock()
    composite.on_finish(mock_manager, [])

    assert call_order == ["coord2", "coord1"]


def test_composite_coordinator_on_error_calls_all():
    mock_coord1 = MagicMock(spec=BaseCoordinator)
    mock_coord1.priority = 50
    mock_coord2 = MagicMock(spec=BaseCoordinator)
    mock_coord2.priority = 60

    composite = CompositeCoordinator([mock_coord1, mock_coord2])

    mock_manager = MagicMock()
    mock_error = Exception("test error")
    composite.on_error(mock_manager, mock_error)

    mock_coord1.on_error.assert_called_once_with(mock_manager, mock_error)
    mock_coord2.on_error.assert_called_once_with(mock_manager, mock_error)


def test_file_merge_coordinator_initialization():
    coordinator = FileMergeCoordinator("/path/to/output.csv")

    assert coordinator.target_path == Path("/path/to/output.csv")
    assert coordinator.remove_parts is True


def test_file_merge_coordinator_initialization_with_remove_parts_false():
    coordinator = FileMergeCoordinator("/path/to/output.csv", remove_parts=False)

    assert coordinator.remove_parts is False


def test_file_merge_coordinator_priority():
    coordinator = FileMergeCoordinator("/path/to/output.csv")
    assert coordinator.priority == 50


def test_file_merge_coordinator_prepare_shards_returns_empty():
    coordinator = FileMergeCoordinator("/path/to/output.csv")

    result = coordinator.prepare_shards("adapter", 3)

    assert result == []


def test_file_merge_coordinator_on_finish_merges_files(tmp_path):
    target = tmp_path / "merged.csv"
    part1 = tmp_path / "part1.csv"
    part2 = tmp_path / "part2.csv"

    part1.write_text("data1\n")
    part2.write_text("data2\n")

    coordinator = FileMergeCoordinator(target)

    results = [
        WorkerResult(worker_id=0, output_path=str(part1)),
        WorkerResult(worker_id=1, output_path=str(part2)),
    ]

    mock_manager = MagicMock()
    coordinator.on_finish(mock_manager, results)

    assert target.exists()
    content = target.read_text()
    assert "data1" in content
    assert "data2" in content


def test_file_merge_coordinator_removes_parts_when_configured(tmp_path):
    target = tmp_path / "merged.csv"
    part1 = tmp_path / "part1.csv"

    part1.write_text("data1\n")

    coordinator = FileMergeCoordinator(target, remove_parts=True)

    results = [WorkerResult(worker_id=0, output_path=str(part1))]

    mock_manager = MagicMock()
    coordinator.on_finish(mock_manager, results)

    assert target.exists()
    assert not part1.exists()


def test_file_merge_coordinator_keeps_parts_when_configured(tmp_path):
    target = tmp_path / "merged.csv"
    part1 = tmp_path / "part1.csv"

    part1.write_text("data1\n")

    coordinator = FileMergeCoordinator(target, remove_parts=False)

    results = [WorkerResult(worker_id=0, output_path=str(part1))]

    mock_manager = MagicMock()
    coordinator.on_finish(mock_manager, results)

    assert target.exists()
    assert part1.exists()


def test_file_merge_coordinator_skips_nonexistent_files(tmp_path):
    target = tmp_path / "merged.csv"

    coordinator = FileMergeCoordinator(target)

    results = [
        WorkerResult(worker_id=0, output_path="/nonexistent/file.csv"),
    ]

    mock_manager = MagicMock()
    coordinator.on_finish(mock_manager, results)

    assert not target.exists()


def test_file_merge_coordinator_handles_no_output_paths(tmp_path):
    target = tmp_path / "merged.csv"

    coordinator = FileMergeCoordinator(target)

    results = [
        WorkerResult(worker_id=0, output_path=None),
    ]

    mock_manager = MagicMock()
    coordinator.on_finish(mock_manager, results)

    assert not target.exists()
