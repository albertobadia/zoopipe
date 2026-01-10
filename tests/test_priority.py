import typing

from zoopipe.core import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.hooks.base import BaseHook, HookPriority, HookStore
from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.models.core import EntryStatus, EntryTypedDict
from zoopipe.output_adapter.memory import MemoryOutputAdapter


class PriorityTrackerHook(BaseHook):
    def __init__(self, name: str, priority: int):
        super().__init__(priority=priority)
        self.name = name

    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        for entry in entries:
            if "execution_order" not in entry["metadata"]:
                entry["metadata"]["execution_order"] = []
            entry["metadata"]["execution_order"].append(self.name)
        return entries


class MockInputAdapter(BaseInputAdapter):
    def __init__(self, pre_hooks=None, post_hooks=None):
        super().__init__(pre_hooks=pre_hooks, post_hooks=post_hooks)

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        yield {
            "id": "1",
            "position": 0,
            "status": EntryStatus.PENDING,
            "raw_data": {},
            "validated_data": {},
            "errors": [],
            "metadata": {},
        }


def test_hook_priority_sorting():
    h_normal = PriorityTrackerHook("normal_1", HookPriority.NORMAL)
    h_high = PriorityTrackerHook("high_1", HookPriority.HIGH)
    h_very_high = PriorityTrackerHook("very_high_1", HookPriority.VERY_HIGH)
    h_low = PriorityTrackerHook("low_1", HookPriority.LOW)

    mixed_pre_hooks = [h_low, h_normal, h_high, h_very_high]

    pipe = Pipe(
        input_adapter=MockInputAdapter(),
        output_adapter=MemoryOutputAdapter(),
        executor=SyncFifoExecutor(schema_model=None),
        pre_validation_hooks=mixed_pre_hooks,
    )

    report = pipe.start()
    report.wait()

    result = pipe.output_adapter.results[0]
    execution_order = result["metadata"]["execution_order"]

    assert execution_order == ["very_high_1", "high_1", "normal_1", "low_1"]


def test_hook_priority_across_adapters():
    h_input_low = PriorityTrackerHook("input_low", HookPriority.LOW)
    h_global_high = PriorityTrackerHook("global_high", HookPriority.HIGH)
    h_output_very_high = PriorityTrackerHook("output_very_high", HookPriority.VERY_HIGH)

    pipe = Pipe(
        input_adapter=MockInputAdapter(pre_hooks=[h_input_low]),
        output_adapter=MemoryOutputAdapter(pre_hooks=[h_output_very_high]),
        executor=SyncFifoExecutor(schema_model=None),
        pre_validation_hooks=[h_global_high],
    )

    report = pipe.start()
    report.wait()

    result = pipe.output_adapter.results[0]
    execution_order = result["metadata"]["execution_order"]

    assert execution_order == ["output_very_high", "global_high", "input_low"]
