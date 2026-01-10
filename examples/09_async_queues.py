import asyncio
import random
import time

from pydantic import BaseModel, ConfigDict

from zoopipe import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.queue import AsyncQueueInputAdapter
from zoopipe.output_adapter.queue import AsyncQueueOutputAdapter


class SensorData(BaseModel):
    model_config = ConfigDict(extra="ignore")
    sensor_id: str
    temperature: float
    timestamp: float


async def producer(queue: asyncio.Queue, count: int):
    """Simulates an async data source (e.g. WebSocket or API)"""
    sensors = ["temp_01", "temp_02", "temp_03"]
    for i in range(count):
        data = {
            "sensor_id": random.choice(sensors),
            "temperature": random.uniform(20.0, 35.0),
            "timestamp": time.time(),
        }
        print(f"PRODUCER: Sending data from {data['sensor_id']}...")
        await queue.put(data)
        await asyncio.sleep(0.1)

    # Send sentinel to end the flow
    await queue.put(None)


async def consumer(queue: asyncio.Queue):
    """Simulates an async data sink (e.g. Real-time Dashboard)"""
    while True:
        entry = await queue.get()
        if entry is None:
            break

        status = entry["status"].value
        data = entry["validated_data"]
        print(
            f"CONSUMER: Received [{status}] {data['sensor_id']} "
            f"({data['temperature']:.2f}°C)"
        )
        queue.task_done()


async def main():
    input_queue = asyncio.Queue()
    output_queue = asyncio.Queue()

    # Configure Pipe with Async Queue Adapters
    # Note: Pipe runs the processing in a background thread,
    # but these adapters bridge that gap by being coro-safe.
    pipe = Pipe(
        input_adapter=AsyncQueueInputAdapter(input_queue),
        output_adapter=AsyncQueueOutputAdapter(output_queue),
        executor=SyncFifoExecutor(SensorData),
    )

    print("🚀 Starting Async Queue Ingestion Pipeline...")

    # 1. Start the producer
    producer_task = asyncio.create_task(producer(input_queue, count=10))

    # 2. Start the consumer
    consumer_task = asyncio.create_task(consumer(output_queue))

    # 3. Running Pipe (it starts its own thread)
    report = pipe.start()

    # 4. Wait for producer to finish
    await producer_task

    # 5. Wait for Pipe to complete
    # Since we are in an async main, we can't easily wait() on the report
    # without blocking the loop, but report.wait() is thread-safe.
    await report.wait_async()

    # 6. Signal consumer to stop
    await output_queue.put(None)
    await consumer_task

    print("\n✅ Pipeline Finished!")
    print("\n✅ Pipeline Finished!")
    print(report)


if __name__ == "__main__":
    asyncio.run(main())
