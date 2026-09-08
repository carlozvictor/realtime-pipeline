from ingest.jetstream import normalize_event


def test_normalize_event_extracts_post_fields():
    raw = {
        "did": "did:plc:abc123",
        "time_us": 1_700_000_000_000_000,
        "kind": "commit",
        "commit": {
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "record": {"text": "hello", "langs": ["en"]},
        },
    }

    event = normalize_event(raw)

    assert event == {
        "event_time": 1_700_000_000.0,
        "kind": "post",
        "did": "did:plc:abc123",
        "lang": "en",
        "text": "hello",
    }


def test_normalize_event_ignores_non_commit_kind():
    assert normalize_event({"kind": "identity"}) is None


def test_normalize_event_ignores_unmapped_collection():
    raw = {
        "did": "did:plc:abc123",
        "time_us": 1_700_000_000_000_000,
        "kind": "commit",
        "commit": {"operation": "create", "collection": "app.bsky.actor.profile", "record": {}},
    }

    assert normalize_event(raw) is None


def test_normalize_event_ignores_delete_operation():
    raw = {
        "did": "did:plc:abc123",
        "time_us": 1_700_000_000_000_000,
        "kind": "commit",
        "commit": {"operation": "delete", "collection": "app.bsky.feed.post", "record": {}},
    }

    assert normalize_event(raw) is None
