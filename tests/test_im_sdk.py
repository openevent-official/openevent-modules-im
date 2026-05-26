from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from openevent.im_sdk import (
    MalformedPayloadError,
    SendRequestInput,
    SendResultInput,
    SyncRecordInput,
    create_client,
    is_request_timeout,
)
from openevent.im_sdk.codec import build_message_too_large_content_raw


class FakeOpenEventClient:
    def __init__(self):
        self.calls = []

    def publish_auto_seq(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(seq=123)


def test_publish_send_request_defaults_recipients_empty():
    oe = FakeOpenEventClient()
    client = create_client(oe)

    seq = client.publish_send_request(
        principal=10001,
        token="tok",
        channel_id=7,
        req=SendRequestInput(
            request_id="req-1",
            msg_type="text",
            content={"text": "hello"},
            event_ms=1710000000000,
        ),
    )

    assert seq == 123
    assert oe.calls[0]["recipients"] == []
    payload = json.loads(oe.calls[0]["payload"].decode("utf-8"))
    assert payload == {
        "kind": "send.request",
        "request_id": "req-1",
        "data": {"msg_type": "text", "content": {"text": "hello"}},
        "timestamps": {"event_ms": 1710000000000},
    }


def test_publish_send_request_uses_callers_recipients():
    oe = FakeOpenEventClient()
    client = create_client(oe)

    client.publish_send_request(
        principal=10001,
        token="tok",
        channel_id=7,
        req=SendRequestInput(
            request_id="req-1",
            msg_type="text",
            content={"text": "hello"},
            event_ms=1710000000000,
        ),
        recipients=[10002],
    )

    assert oe.calls[0]["recipients"] == [10002]


def test_publish_send_result_preserves_recipients():
    oe = FakeOpenEventClient()
    client = create_client(oe)

    client.publish_send_result(
        principal=90001,
        token="tok",
        channel_id=7,
        recipients=[10001],
        req=SendResultInput(
            request_id="req-1",
            prev_seq=11,
            status="SUCCESS",
            provider_message_id="msg-1",
            event_ms=1710000000100,
        ),
    )

    assert oe.calls[0]["recipients"] == [10001]
    payload = json.loads(oe.calls[0]["payload"].decode("utf-8"))
    assert payload["prev_seq"] == 11
    assert payload["data"]["provider_message_id"] == "msg-1"


def test_publish_sync_record_sorts_and_deduplicates_recipients():
    oe = FakeOpenEventClient()
    client = create_client(oe)

    client.publish_sync_record(
        principal=10001,
        token="tok",
        channel_id=7,
        recipients=[10003, 10002, 10002],
        req=SyncRecordInput(
            provider_message_id="msg-1",
            msg_type="text",
            content_raw={"text": "hello"},
            text="hello",
            event_ms=1710000000000,
            ingested_ms=1710000000100,
        ),
    )

    assert oe.calls[0]["recipients"] == [10002, 10003]


def test_parse_message_rejects_source_principal():
    client = create_client(FakeOpenEventClient())
    payload = {
        "kind": "send.request",
        "request_id": "req-1",
        "source_principal": 10001,
        "data": {"msg_type": "text", "content": {}},
        "timestamps": {"event_ms": 1},
    }
    message = SimpleNamespace(
        seq=1,
        channel_id=2,
        principal=3,
        recipients=[],
        payload=json.dumps(payload).encode("utf-8"),
    )

    with pytest.raises(MalformedPayloadError):
        client.parse_message(message)


def test_message_too_large_content_raw_helper():
    content_raw = build_message_too_large_content_raw(
        original_size_bytes=20971520,
        metadata={"provider_message_id": "msg-1"},
    )

    assert content_raw == {
        "omitted": True,
        "reason": "message_too_large",
        "original_size_bytes": 20971520,
        "metadata": {"provider_message_id": "msg-1"},
    }


def test_is_request_timeout():
    assert is_request_timeout(1000, 1600, 600)
    assert not is_request_timeout(1000, 1599, 600)
    assert not is_request_timeout(1000, 999, 1)
