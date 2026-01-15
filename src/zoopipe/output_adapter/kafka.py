from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.zoopipe_rust_core import KafkaWriter


class KafkaOutputAdapter(BaseOutputAdapter):
    def __init__(
        self,
        uri: str,
        acks: int = 1,
        timeout: int = 30,
    ):
        """
        Kafka Output Adapter.

        Args:
            uri: Kafka URI (e.g., 'kafka://localhost:9092/topic')
            acks: Required ACKs (None=0, 1, 'all'=-1). Defaults to 1.
            timeout: Ack timeout in seconds.
        """
        self.uri = uri
        self.acks = acks
        self.timeout = timeout

    def get_native_writer(self) -> KafkaWriter:
        return KafkaWriter(
            self.uri,
            acks=self.acks,
            timeout=self.timeout,
        )


__all__ = ["KafkaOutputAdapter"]
