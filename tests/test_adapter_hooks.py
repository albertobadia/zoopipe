import typing
from dataclasses import dataclass, field

from zoopipe.core import Pipe
from zoopipe.executor.base import BaseExecutor
from zoopipe.hooks.base import BaseHook, HookPriority, HookStore
from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.models.core import EntryStatus, EntryTypedDict
from zoopipe.output_adapter.base import BaseOutputAdapter


@dataclass
class MockHook(BaseHook):
    name: str
    executed_entries: list[EntryTypedDict] = field(default_factory=list)
    priority: int = field(default=HookPriority.NORMAL)

    def setup(self, store: HookStore) -> None:
        store[f"{self.name}_setup"] = True

    def teardown(self, store: HookStore) -> None:
        store[f"{self.name}_teardown"] = True

    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        self.executed_entries.extend(entries)
        for entry in entries:
            entry["metadata"][f"hook_{self.name}"] = True
            if "val" in entry["raw_data"]:
                entry["raw_data"]["val"] += f"_{self.name}"
        return entries


class MockInputAdapter(BaseInputAdapter):
    def __init__(
        self,
        items: list[dict],
        pre_hooks: list[BaseHook] | None = None,
        post_hooks: list[BaseHook] | None = None,
    ):
        super().__init__(pre_hooks=pre_hooks, post_hooks=post_hooks)
        self.items = items

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        for i, item in enumerate(self.items):
            yield EntryTypedDict(
                id=str(i),
                position=i,
                status=EntryStatus.PENDING,
                raw_data=item,
                validated_data=None,
                errors=[],
                metadata={},
            )


class MockOutputAdapter(BaseOutputAdapter):
    def __init__(
        self,
        pre_hooks: list[BaseHook] | None = None,
        post_hooks: list[BaseHook] | None = None,
    ):
        super().__init__(pre_hooks=pre_hooks, post_hooks=post_hooks)
        self.written_entries: list[EntryTypedDict] = []

    def write(self, entry: EntryTypedDict) -> None:
        self.written_entries.append(entry)


class SyncStackExecutor(BaseExecutor):
    def __init__(self):
        super().__init__()
        self._items = []

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        if self._upstream_iterator is not None:
            pass

        return self._run()

    def _run(self):
        from zoopipe.executor.base import WorkerContext

        ctx = WorkerContext(
            schema_model=None,
            do_binary_pack=False,
            compression_algorithm=None,
            pre_hooks=self._pre_validation_hooks,
            post_hooks=self._post_validation_hooks,
        )

        for chunk_data in self._upstream_iterator:
            entries = self.process_chunk_on_worker(chunk_data, ctx)
            for entry in entries:
                yield entry


# --- Tests ---


def test_pipe_hook_integration():
    input_hook = MockHook(name="input")
    output_hook = MockHook(name="output")

    items = [{"val": "test"}]
    items = [{"val": "test"}]
    # input hook -> pre
    input_adapter = MockInputAdapter(items=items, pre_hooks=[input_hook])
    # output hook -> post
    output_adapter = MockOutputAdapter(post_hooks=[output_hook])
    executor = SyncStackExecutor()

    pipe = Pipe(
        input_adapter=input_adapter, output_adapter=output_adapter, executor=executor
    )

    report = pipe.start()
    report.wait()

    assert report.total_processed == 1
    assert len(output_adapter.written_entries) == 1
    result = output_adapter.written_entries[0]
    assert result["raw_data"]["val"] == "test_input_output"
    assert result["metadata"]["hook_input"] is True
    assert result["metadata"]["hook_output"] is True
    assert len(input_hook.executed_entries) == 1
    assert len(output_hook.executed_entries) == 1


def test_hook_order_precedence():
    h1 = MockHook(name="adapter_in")
    h2 = MockHook(name="global_pre")
    h3 = MockHook(name="global_post")
    h4 = MockHook(name="adapter_out")

    # adapter_in (pre) -> global_pre -> ... -> global_post -> adapter_out (post)
    input_adapter = MockInputAdapter(items=[{"val": "start"}], pre_hooks=[h1])
    output_adapter = MockOutputAdapter(post_hooks=[h4])
    executor = SyncStackExecutor()

    pipe = Pipe(
        input_adapter=input_adapter,
        output_adapter=output_adapter,
        executor=executor,
        pre_validation_hooks=[h2],
        post_validation_hooks=[h3],
    )

    report = pipe.start()
    report.wait()

    result = output_adapter.written_entries[0]
    assert (
        result["raw_data"]["val"]
        == "start_adapter_in_global_pre_global_post_adapter_out"
    )
