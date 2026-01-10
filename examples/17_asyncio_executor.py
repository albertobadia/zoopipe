import asyncio
import random
import time
import typing
import uuid

from pydantic import BaseModel, ConfigDict

from zoopipe import BaseHook, EntryTypedDict, HookStore, Pipe
from zoopipe.executor.asyncio import AsyncIOExecutor
from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.models.core import EntryStatus
from zoopipe.output_adapter.generator import GeneratorOutputAdapter


class WebPage(BaseModel):
    model_config = ConfigDict(extra="ignore")
    url: str
    content_length: int = 0


class GeneratorInputAdapter(BaseInputAdapter):
    def __init__(self, generator_func: typing.Iterator[dict]):
        super().__init__()
        self._gen = generator_func

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        for i, item in enumerate(self._gen):
            yield EntryTypedDict(
                id=uuid.uuid4(),
                raw_data=item,
                validated_data=None,
                position=i,
                status=EntryStatus.PENDING,
                errors=[],
                metadata={},
            )


class FetchHook(BaseHook):
    async def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        tasks = []
        for entry in entries:
            tasks.append(self._fetch_mock(entry))
        await asyncio.gather(*tasks)
        return entries

    async def _fetch_mock(self, entry: EntryTypedDict):
        delay = random.uniform(0.1, 0.5)
        await asyncio.sleep(delay)
        entry["validated_data"]["content_length"] = int(delay * 1000)


def data_generator():
    urls = [f"https://example.com/page_{i}" for i in range(20)]
    for url in urls:
        yield {"url": url}


def main():
    print("Starting AsyncIO Pipeline...")

    executor = AsyncIOExecutor(schema_model=WebPage, concurrency=5)

    fetch_hook = FetchHook()

    pipe = Pipe(
        input_adapter=GeneratorInputAdapter(data_generator()),
        output_adapter=GeneratorOutputAdapter(),
        executor=executor,
        post_validation_hooks=[fetch_hook],
    )

    start_time = time.time()
    report = pipe.start()
    report.wait()
    duration = time.time() - start_time

    print(f"\nTotal time: {duration:.2f}s")
    print(report)


if __name__ == "__main__":
    main()
