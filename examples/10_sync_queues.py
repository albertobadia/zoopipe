import queue
import threading
import time

from pydantic import BaseModel

from zoopipe import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.queue import QueueInputAdapter
from zoopipe.output_adapter.queue import QueueOutputAdapter


class UserSchema(BaseModel):
    name: str
    age: int


def producer(q):
    users = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35},
    ]
    for user in users:
        print(f"PRODUCER: Adding {user['name']} to queue")
        q.put(user)
        time.sleep(0.5)
    q.put(None)


def consumer(q):
    while True:
        entry = q.get()
        if entry is None:
            break
        print(
            f"CONSUMER: Processed {entry['validated_data']['name']}, "
            f"Status: {entry['status']}"
        )
        q.task_done()


def main():
    input_q = queue.Queue()
    output_q = queue.Queue()

    pipe = Pipe(
        input_adapter=QueueInputAdapter(input_q),
        output_adapter=QueueOutputAdapter(output_q),
        executor=SyncFifoExecutor(UserSchema),
    )

    producer_thread = threading.Thread(target=producer, args=(input_q,))
    consumer_thread = threading.Thread(target=consumer, args=(output_q,))

    producer_thread.start()
    consumer_thread.start()

    print("🚀 Starting Sync Queue Pipeline...")
    report = pipe.start()
    report.wait()

    output_q.put(None)
    producer_thread.join()
    consumer_thread.join()

    print("\n✅ Pipeline Finished!")
    print("\n✅ Pipeline Finished!")
    print(report)


if __name__ == "__main__":
    main()
