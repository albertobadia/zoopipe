# Kafka Adapters

ZooPipe includes high-performance Kafka input and output adapters backed by a customized Rust implementation using the `kafka` crate. These adapters are designed for efficiency, leveraging zero-copy message handling where possible.

## KafkaInputAdapter

The `KafkaInputAdapter` allows you to consume messages from a Kafka topic. It fits seamlessly into ZooPipe's pipeline architecture, acting as an infinite source of data until the pipeline is stopped or specific limits are reached.

### Basic Usage

```python
from zoopipe import KafkaInputAdapter, CSVOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=KafkaInputAdapter(
        uri="kafka://localhost:9092/my-topic",
        group_id="my-consumer-group",
    ),
    output_adapter=CSVOutputAdapter("output.csv"),
)
```

### Parameters

- **uri** (`str`): The connection URI for the Kafka cluster and topic.
  - Format: `kafka://<broker_host>:<port>/<topic>`
  - Example: `kafka://localhost:9092/user-events`

- **group_id** (`str | None`, default=`None`): The consumer group ID.
  - If provided, the adapter will join the consumer group and commit offsets.
  - If `None`, it acts as a simple consumer without group coordination.

- **generate_ids** (`bool`, default=`True`): Whether to generate UUIDs for each message.
  - While Kafka messages have offsets, generating a unique ID within ZooPipe can be useful for downstream tracing.

### Usage Notes

- **Blocking Behavior**: The adapter currently polls the broker. If no messages are available immediately, it may wait depending on the underlying driver's timeout configuration.
- **Message Format**: Messages are yielded as dictionaries containing `value` (bytes or string), `key` (bytes or string, if any), and metadata like partition and offset.

## KafkaOutputAdapter

The `KafkaOutputAdapter` enables you to publish processed data back to a Kafka topic.

### Basic Usage

```python
from zoopipe import JSONInputAdapter, KafkaOutputAdapter, Pipe

pipe = Pipe(
    input_adapter=JSONInputAdapter("data.jsonl"),
    output_adapter=KafkaOutputAdapter(
        uri="kafka://localhost:9092/output-topic",
        acks=1
    ),
)
```

### Parameters

- **uri** (`str`): The connection URI for the Kafka cluster and topic.
  - Format: `kafka://<broker_host>:<port>/<topic>`

- **acks** (`int`, default=`1`): The number of acknowledgments the producer requires the leader to have received before considering a request complete.
  - `0`: No acknowledgments (fire and forget).
  - `1`: Leader acknowledgment.
  - `-1` or `all`: All in-sync replicas must acknowledge.

- **timeout** (`int`, default=`30`): Timeout in seconds for message delivery.

### Data Serialization

The adapter expects data to be serialization-ready. If your pipeline passes dictionaries, they should ideally be converted to bytes or strings (e.g., via a transformation step) before reaching the Kafka output adapter, although the adapter attempts basic string conversion for common types.

## Performance Characteristics

- **Zero-Copy**: The Rust implementation avoids unnecessary memory copies when passing message payloads from the network buffer to Python, using buffer protocol where applicable.
- **Batching**: Write operations support batching for higher throughput.

## Error Handling

Common errors include:
- **Connection Refused**: If the broker is unreachable.
- **Invalid URI**: If the URI scheme or format is incorrect.
- **Topic Not Found**: If the topic does not exist and auto-creation is disabled on the broker.

```python
try:
    pipe.start()
except Exception as e:
    print(f"Pipeline failed: {e}")
```
