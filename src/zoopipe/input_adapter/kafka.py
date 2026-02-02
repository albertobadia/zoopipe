from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.zoopipe_rust_core import KafkaReader


class KafkaInputAdapter(BaseInputAdapter):
    """
    Consumes messages from Apache Kafka topics.

    Acts as a Kafka consumer, streaming messages into the pipeline with
    support for consumer groups and offset management.
    """

    def __init__(
        self,
        uri: str,
        group_id: str | None = None,
        generate_ids: bool = True,
    ):
        """
        Kafka Input Adapter.

        Args:
            uri: Kafka URI (e.g., 'kafka://localhost:9092/topic')
            group_id: Optional consumer group ID.
            generate_ids: Whether to generate unique IDs for each message.
        """
        super().__init__()
        self.uri = uri
        self.group_id = group_id
        self.generate_ids = generate_ids

    def get_native_reader(self) -> KafkaReader:
        return KafkaReader(
            self.uri,
            group_id=self.group_id,
            generate_ids=self.generate_ids,
        )


__all__ = ["KafkaInputAdapter"]
