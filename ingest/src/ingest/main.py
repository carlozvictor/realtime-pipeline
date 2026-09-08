import asyncio
import json
import logging
import sys

from ingest.config import Config
from ingest.jetstream import normalize_event, stream_raw_messages
from ingest.producer import EventProducer


def _configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logging.basicConfig(level=logging.INFO, handlers=[handler])


async def run(config: Config) -> None:
    producer = EventProducer(config.kafka_bootstrap, config.kafka_topic)
    logger = logging.getLogger(__name__)
    processed = 0

    try:
        async for raw in stream_raw_messages(config.jetstream_url):
            event = normalize_event(raw)
            if event is None:
                continue

            producer.publish(key=event["did"], event=event)
            processed += 1

            if processed % 1000 == 0:
                logger.info(json.dumps({"event": "ingest_progress", "processed": processed}))
    finally:
        producer.flush()


def main() -> None:
    _configure_logging()
    config = Config.from_env()
    asyncio.run(run(config))


if __name__ == "__main__":
    main()
