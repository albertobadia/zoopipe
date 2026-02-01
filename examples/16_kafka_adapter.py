import sys
import uuid

from zoopipe import Pipe, SingleThreadExecutor
from zoopipe.input_adapter import KafkaInputAdapter, PyGeneratorInputAdapter
from zoopipe.output_adapter import CSVOutputAdapter, KafkaOutputAdapter


def kafka_producer_flow():
    """
    Generates dummy data in Python and writes it to a Kafka topic.
    """
    print("--- Starting Producer Flow ---")

    def generator():
        for i in range(10):
            yield {"id": i, "message": f"Hello Kafka {i}", "extra": "data"}

    input_adapter = PyGeneratorInputAdapter(generator())
    output_adapter = KafkaOutputAdapter(
        uri="kafka://localhost:9092/test-zoopipe", acks=1
    )

    pipe = Pipe(input_adapter, output_adapter)
    pipe.run()

    print(f"Producer finished: {pipe.report.total_processed} messages sent.")
    print(pipe.report)


def kafka_consumer_flow():
    """
    Reads from Kafka and writes to CSV (stdout for demo).
    """
    print("\n--- Starting Consumer Flow ---")

    input_adapter = KafkaInputAdapter(
        uri="kafka://localhost:9092/test-zoopipe",
        group_id=f"zoopipe-consumer-{uuid.uuid4()}",
        generate_ids=True,
    )

    output_adapter = CSVOutputAdapter("kafka_output.csv")

    pipe = Pipe(
        input_adapter, output_adapter, executor=SingleThreadExecutor(batch_size=1)
    )
    pipe.run()

    print(f"Consumer finished: {pipe.report.total_processed} messages received.")
    print(pipe.report)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "producer":
        kafka_producer_flow()
    elif len(sys.argv) > 1 and sys.argv[1] == "consumer":
        kafka_consumer_flow()
    else:
        print("Usage: python examples/16_kafka_adapter.py [producer|consumer]")
        print("Running producer then consumer...")
        try:
            kafka_producer_flow()
            kafka_consumer_flow()
        except Exception as e:
            print(f"Execution failed (is Kafka running?): {e}")
