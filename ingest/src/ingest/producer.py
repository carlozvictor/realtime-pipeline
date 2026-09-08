import json
import logging
from typing import Any

from confluent_kafka import KafkaError, Message, Producer

logger = logging.getLogger(__name__)


class EventProducer:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._topic = topic
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})

    def publish(self, key: str, event: dict[str, Any]) -> None:
        self._producer.produce(
            self._topic,
            key=key.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush()

    @staticmethod
    def _on_delivery(error: KafkaError | None, message: Message) -> None:
        if error is not None:
            logger.error(json.dumps({"event": "kafka_delivery_failed", "error": str(error)}))
