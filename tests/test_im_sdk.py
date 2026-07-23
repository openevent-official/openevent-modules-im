from __future__ import annotations

import json
from types import SimpleNamespace

import grpc
import pytest

from openevent.im_sdk import (
    MalformedPayloadError,
    PublishFailedError,
    SendRequestInput,
    SendResultInput,
    SyncRecordInput,
    create_client,
)


class FakeOpenEventClient:
    def __init__(self):
        self.calls = []

    def get_status(self, principal, token):
        return SimpleNamespace(max_seq=0)

    def publish_auto_seq(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(seq=123)


class FakeRpcError(grpc.RpcError):
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code


class UncertainPublishClient:
    def __init__(self, *, committed: bool, fetch_error: Exception | None = None):
        self.committed = committed
        self.fetch_error = fetch_error
        self.status_calls = 0
        self.publish_calls = 0
        self.payload = None

    def get_status(self, principal, token):
        self.status_calls += 1
        return SimpleNamespace(max_seq=10 if self.status_calls == 1 or not self.committed else 11)

    def publish_auto_seq(self, **kwargs):
        self.publish_calls += 1
        self.payload = kwargs
        raise FakeRpcError(grpc.StatusCode.UNAVAILABLE)

    def fetch(self, **kwargs):
        if self.fetch_error is not None:
            raise self.fetch_error
        message = SimpleNamespace(
            seq=11,
            principal=self.payload["principal"],
            channel_id=self.payload["channel_id"],
            recipients=self.payload["recipients"],
            payload=self.payload["payload"],
        )
        return SimpleNamespace(messages=[message], next_seq=12, last_seq=11)


class PagedUncertainPublishClient(UncertainPublishClient):
    def __init__(self):
        super().__init__(committed=True)
        self.fetch_calls = []

    def get_status(self, principal, token):
        self.status_calls += 1
        return SimpleNamespace(max_seq=10 if self.status_calls == 1 else 12)

    def fetch(self, **kwargs):
        self.fetch_calls.append(kwargs["from_seq"])
        if kwargs["from_seq"] == 11:
            return SimpleNamespace(messages=[], next_seq=12, last_seq=12)
        message = SimpleNamespace(
            seq=12,
            principal=self.payload["principal"],
            channel_id=self.payload["channel_id"],
            recipients=self.payload["recipients"],
            payload=self.payload["payload"],
        )
        return SimpleNamespace(messages=[message], next_seq=13, last_seq=12)


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


def test_uncertain_publish_result_reconciles_committed_message():
    oe = UncertainPublishClient(committed=True)
    client = create_client(oe)

    seq = client.publish_send_request(
        principal=10001,
        token="tok",
        channel_id=7,
        req=SendRequestInput(
            request_id="req-uncertain",
            msg_type="text",
            content={"text": "hello"},
            event_ms=1710000000000,
        ),
    )

    assert seq == 11
    assert oe.publish_calls == 1
    assert oe.status_calls == 2


def test_uncertain_publish_reconciliation_follows_empty_short_page():
    oe = PagedUncertainPublishClient()
    client = create_client(oe)

    seq = client.publish_send_request(
        principal=10001,
        token="tok",
        channel_id=7,
        req=SendRequestInput(
            request_id="req-paged-reconcile",
            msg_type="text",
            content={"text": "hello"},
            event_ms=1710000000000,
        ),
    )

    assert seq == 12
    assert oe.fetch_calls == [11, 12]


def test_uncertain_publish_result_reports_retry_safe_after_absence_proved():
    oe = UncertainPublishClient(committed=False)
    client = create_client(oe)

    with pytest.raises(PublishFailedError) as raised:
        client.publish_send_request(
            principal=10001,
            token="tok",
            channel_id=7,
            req=SendRequestInput(
                request_id="req-not-committed",
                msg_type="text",
                content={"text": "hello"},
                event_ms=1710000000000,
            ),
        )

    assert raised.value.retry_safe
    assert not raised.value.outcome_unknown


def test_uncertain_publish_result_stays_unknown_when_reconciliation_fails():
    oe = UncertainPublishClient(
        committed=True,
        fetch_error=FakeRpcError(grpc.StatusCode.DEADLINE_EXCEEDED),
    )
    client = create_client(oe)

    with pytest.raises(PublishFailedError) as raised:
        client.publish_send_request(
            principal=10001,
            token="tok",
            channel_id=7,
            req=SendRequestInput(
                request_id="req-reconcile-failed",
                msg_type="text",
                content={"text": "hello"},
                event_ms=1710000000000,
            ),
        )

    assert not raised.value.retry_safe
    assert raised.value.outcome_unknown


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


@pytest.mark.parametrize(
    ("payload", "error"),
    [
        (
            {
                "kind": "send.request",
                "request_id": "req-1",
                "data": {"content": {}},
                "timestamps": {"event_ms": 1},
            },
            "data.msg_type",
        ),
        (
            {
                "kind": "send.request",
                "request_id": "req-1",
                "data": {"msg_type": "text"},
                "timestamps": {"event_ms": 1},
            },
            "data.content",
        ),
        (
            {
                "kind": "send.result",
                "request_id": "req-1",
                "data": {"status": "SUCCESS"},
                "timestamps": {"event_ms": 1},
            },
            "prev_seq",
        ),
        (
            {
                "kind": "send.result",
                "request_id": "req-1",
                "prev_seq": 1,
                "data": {"status": "PENDING"},
                "timestamps": {"event_ms": 1},
            },
            "data.status",
        ),
        (
            {
                "kind": "sync.record",
                "data": {"msg_type": "text", "content_raw": {}},
                "timestamps": {"event_ms": 1, "ingested_ms": 2},
            },
            "data.provider_message_id",
        ),
    ],
)
def test_parse_payload_validates_kind_required_fields(payload, error):
    client = create_client(FakeOpenEventClient())

    with pytest.raises(MalformedPayloadError, match=error):
        client.parse_payload(json.dumps(payload).encode("utf-8"))
