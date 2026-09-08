import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    jetstream_url: str
    kafka_bootstrap: str
    kafka_topic: str

    @staticmethod
    def from_env() -> "Config":
        return Config(
            jetstream_url=os.environ["JETSTREAM_URL"],
            kafka_bootstrap=os.environ["KAFKA_BOOTSTRAP"],
            kafka_topic=os.environ["KAFKA_TOPIC"],
        )
