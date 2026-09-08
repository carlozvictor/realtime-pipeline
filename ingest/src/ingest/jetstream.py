import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

import websockets

logger = logging.getLogger(__name__)

_COLLECTION_KIND = {
    "app.bsky.feed.post": "post",
    "app.bsky.feed.like": "like",
    "app.bsky.feed.repost": "repost",
    "app.bsky.graph.follow": "follow",
}

_MAX_BACKOFF_SECONDS = 30


async def stream_raw_messages(url: str) -> AsyncIterator[dict[str, Any]]:
    backoff = 1
    while True:
        try:
            async with websockets.connect(url) as connection:
                backoff = 1
                async for message in connection:
                    yield json.loads(message)
        except (websockets.ConnectionClosed, OSError) as error:
            logger.warning(json.dumps({"event": "jetstream_disconnect", "error": str(error), "backoff": backoff}))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _MAX_BACKOFF_SECONDS)


def normalize_event(raw: dict[str, Any]) -> dict[str, Any] | None:
    if raw.get("kind") != "commit":
        return None

    commit = raw.get("commit", {})
    collection = commit.get("collection")
    kind = _COLLECTION_KIND.get(collection)
    if kind is None or commit.get("operation") != "create":
        return None

    record = commit.get("record", {})
    langs = record.get("langs") or []
    time_us = raw.get("time_us")
    if time_us is None:
        return None

    return {
        "event_time": time_us / 1_000_000,
        "kind": kind,
        "did": raw.get("did", ""),
        "lang": langs[0] if langs else "",
        "text": record.get("text", ""),
    }
