import asyncio
import inspect
import typing
from concurrent.futures import ThreadPoolExecutor

from pydantic import BaseModel

from zoopipe.executor.base import BaseExecutor, WorkerContext
from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.models.core import EntryTypedDict


class AsyncIOExecutor(BaseExecutor):
    def __init__(
        self,
        schema_model: type[BaseModel] | None = None,
        max_workers: int | None = None,
        concurrency: int = 10,
        use_batch_validation: bool = False,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        super().__init__()
        self._schema_model = schema_model
        self._max_workers = max_workers
        self._concurrency = concurrency
        self._use_batch_validation = use_batch_validation
        self._main_loop = loop

    @property
    def do_binary_pack(self) -> bool:
        return False

    async def _execute_hook_async_safe(
        self,
        entries: list[EntryTypedDict],
        hook: BaseHook,
        store: HookStore,
        executor: ThreadPoolExecutor,
    ) -> list[EntryTypedDict]:
        try:
            if inspect.iscoroutinefunction(hook.execute):
                if self._main_loop:
                    future = asyncio.run_coroutine_threadsafe(
                        hook.execute(entries, store), self._main_loop
                    )
                    result = future.result()
                else:
                    result = await hook.execute(entries, store)
            else:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    executor, hook.execute, entries, store
                )
            return result or entries
        except Exception as e:
            self._handle_hook_error(entries, hook.__class__.__name__, e)
            return entries

    async def _run_hooks_async(
        self,
        entries: list[EntryTypedDict],
        hooks: list[BaseHook],
        store: HookStore,
        executor: ThreadPoolExecutor,
        max_hook_chunk_size: int | None = None,
    ) -> list[EntryTypedDict]:
        if not entries or not hooks:
            return entries

        chunk_size = max_hook_chunk_size or len(entries)
        new_entries = []

        for i in range(0, len(entries), chunk_size):
            sub_batch = entries[i : i + chunk_size]
            for hook in hooks:
                sub_batch = await self._execute_hook_async_safe(
                    sub_batch, hook, store, executor
                )
            new_entries.extend(sub_batch)
        return new_entries

    async def _process_chunk_async(
        self,
        chunk: list[dict[str, typing.Any]],
        context: WorkerContext,
        executor: ThreadPoolExecutor,
    ) -> list[EntryTypedDict]:
        entries = self._unpack_data(
            chunk, context.do_binary_pack, context.compression_algorithm
        )
        store = {}
        all_hooks = (context.pre_hooks or []) + (context.post_hooks or [])

        all_hooks = (context.pre_hooks or []) + (context.post_hooks or [])

        for hook in all_hooks:
            hook.setup(store)

        try:
            entries = await self._run_hooks_async(
                entries,
                context.pre_hooks or [],
                store,
                executor,
                context.max_hook_chunk_size,
            )

            if context.schema_model and context.use_batch_validation:
                loop = asyncio.get_running_loop()
                entries = await loop.run_in_executor(
                    executor,
                    self._process_batch_with_fallback,
                    entries,
                    context.schema_model,
                )
            else:

                def validate_batch(ents):
                    return [
                        self._process_single_entry(entry, context.schema_model)
                        for entry in ents
                    ]

                loop = asyncio.get_running_loop()
                entries = await loop.run_in_executor(executor, validate_batch, entries)

            entries = await self._run_hooks_async(
                entries,
                context.post_hooks or [],
                store,
                executor,
                context.max_hook_chunk_size,
            )
        finally:
            for hook in all_hooks:
                hook.teardown(store)

        return entries

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        if not self._upstream_iterator:
            return

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        thread_pool = ThreadPoolExecutor(max_workers=self._max_workers)

        context = WorkerContext(
            schema_model=self._schema_model,
            do_binary_pack=False,
            compression_algorithm=None,
            pre_hooks=self._pre_validation_hooks,
            post_hooks=self._post_validation_hooks,
            max_hook_chunk_size=self._max_hook_chunk_size,
            use_batch_validation=self._use_batch_validation,
        )

        try:
            pending_tasks = set()

            iterator = self._upstream_iterator

            while True:
                while len(pending_tasks) < self._concurrency:
                    try:
                        chunk = next(iterator)
                        task = loop.create_task(
                            self._process_chunk_async(chunk, context, thread_pool)
                        )
                        pending_tasks.add(task)
                    except StopIteration:
                        break

                if not pending_tasks:
                    break

                done, pending = loop.run_until_complete(
                    asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
                )

                for task in done:
                    results = task.result()
                    yield from results
                    pending_tasks.remove(task)

        finally:
            thread_pool.shutdown()
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()


__all__ = ["AsyncIOExecutor"]
