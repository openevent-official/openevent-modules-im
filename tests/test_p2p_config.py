from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest

from openevent.im_sdk import ParsedMessage
from openevent.im_p2p_syncer.adapters.base import ProviderAdapter
from openevent.im_p2p_syncer.config import ConfigError
from openevent.im_p2p_syncer.config import load_config
from openevent.im_p2p_syncer.config import parse_config
from openevent.im_p2p_syncer.loop import FatalSyncerError, SingleThreadProcessor
from openevent.im_p2p_syncer.ids import stable_lark_openapi_uuid
from openevent.im_p2p_syncer.mapping import P2PMappingIndex
from openevent.im_p2p_syncer.models import AdapterHealth, ProviderEvent, RetryConfig, SendResult
from openevent.im_p2p_syncer.state import RuntimeState


def test_sample_config_loads():
    config = load_config("p2p_config.yaml")

    assert config.version == "v1"
    assert config.worker.principal == 90001
    assert sorted(config.principal_tokens) == [10001, 90002]


def test_mapping_index_resolves_p2p_participants():
    config = load_config("p2p_config.yaml")
    mapping = P2PMappingIndex(config)

    assert mapping.provider_session(10001) == ("lark", "oc_p2p_10001_bot")
    assert mapping.sender_external_user_id(10001, 10001) == "ou_source"
    assert mapping.sender_external_user_id(10001, 90002, "bot") == "cli_xxx"
    assert mapping.principal_for_external_user(10001, "lark", "ou_source") == 10001
    assert mapping.peer_principals(10001, 10001) == [90002]
    assert mapping.principal_for_external_user(10001, "lark", "cli_xxx", "bot") == 90002


def test_stable_lark_openapi_uuid_is_deterministic_and_bounded():
    uuid = stable_lark_openapi_uuid("request-id-" + "x" * 200)

    assert uuid == stable_lark_openapi_uuid("request-id-" + "x" * 200)
    assert uuid.startswith("oe-")
    assert len(uuid) == 50


def test_lark_provider_config_loads():
    raw = {
        "version": "v1",
        "worker": {"name": "im-sync-p2p-lark", "principal": 90001, "token": "tok-worker"},
        "openevent": {"target": "127.0.0.1:9527"},
        "principal_tokens": [
            {"principal": 10001, "token": "tok-user"},
            {"principal": 90002, "token": "tok-bot"},
        ],
        "providers": [
            {
                "name": "lark",
                "adapter": "lark",
                "sync": {"mode": "poll"},
                "credentials": {"app_id": "cli_xxx", "app_secret": "secret"},
                "options": {"api_base_url": "https://open.larksuite.com"},
            }
        ],
        "mappings": [
            {
                "provider": "lark",
                "identity_type": "user",
                "external_user_id": "ou_source",
                "principal": 10001,
                "session_id": "oc_p2p_10001_bot",
                "channel_id": 10001,
                "status": "active",
            },
            {
                "provider": "lark",
                "identity_type": "bot",
                "external_user_id": "cli_xxx",
                "principal": 90002,
                "session_id": "oc_p2p_10001_bot",
                "channel_id": 10001,
                "status": "active",
            },
        ],
    }

    config = parse_config(raw)
    mapping = P2PMappingIndex(config)

    assert config.providers["lark"].adapter == "lark"
    assert mapping.provider_session(10001) == ("lark", "oc_p2p_10001_bot")


def test_lark_openapi_adapter_name_must_match_provider_name():
    raw = {
        "version": "v1",
        "worker": {"name": "im-sync-p2p-lark", "principal": 90001, "token": "tok-worker"},
        "openevent": {"target": "127.0.0.1:9527"},
        "principal_tokens": [{"principal": 10001, "token": "tok-user"}],
        "providers": [
            {
                "name": "custom",
                "adapter": "lark",
                "sync": {"mode": "poll"},
                "credentials": {"app_id": "cli_xxx", "app_secret": "secret"},
            }
        ],
        "mappings": [
            {
                "provider": "custom",
                "external_user_id": "ou_source",
                "principal": 10001,
                "session_id": "oc_p2p_10001_bot",
                "channel_id": 10001,
                "status": "active",
            }
        ],
    }

    with pytest.raises(ConfigError, match="adapter must match name"):
        parse_config(raw)


def test_mapping_missing_writes_failed_result_and_clears_request():
    config = load_config("p2p_config.yaml")
    state = RuntimeState()
    task = ParsedMessage(
        seq=11,
        channel_id=10001,
        principal=10003,
        recipients=[],
        kind="send.request",
        payload={},
        data={"msg_type": "text", "content": {"text": "hello"}},
        event_ms=1,
        request_id="req-missing",
    )
    state.add_send_request(task)
    im_client = SimpleNamespace(calls=[])

    def publish_send_result(**kwargs):
        im_client.calls.append(kwargs)
        return 12

    im_client.publish_send_result = publish_send_result
    processor = SingleThreadProcessor(
        im_client=im_client,
        mapping=P2PMappingIndex(config),
        adapters={},
        retry=RetryConfig(publish_initial_backoff_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=state,
        logger=logging.getLogger("test"),
    )

    processor.tick()

    assert "req-missing" not in state.pending_outbound
    assert "req-missing" in state.result_written
    assert im_client.calls[0]["recipients"] == [10003]
    result = im_client.calls[0]["req"]
    assert result.status == "FAILED"
    assert result.error_code == "MAPPING_MISSING"
    assert result.prev_seq == 11


def test_failed_send_result_prevents_history_restore():
    state = RuntimeState()
    state.add_send_request(
        ParsedMessage(
            seq=11,
            channel_id=10001,
            principal=10003,
            recipients=[],
            kind="send.request",
            payload={},
            data={"msg_type": "text", "content": {"text": "hello"}},
            event_ms=1,
            request_id="req-failed",
        )
    )
    state.add_send_result(
        ParsedMessage(
            seq=12,
            channel_id=10001,
            principal=90001,
            recipients=[10003],
            kind="send.result",
            payload={},
            data={"status": "FAILED", "error_code": "MAPPING_MISSING"},
            event_ms=2,
            request_id="req-failed",
            prev_seq=11,
        ),
        "lark",
        "oc_p2p_10001_bot",
    )

    assert "req-failed" not in state.pending_outbound
    assert "req-failed" in state.result_written
    assert state.send_result_by_provider_message == {}


def test_duplicate_send_request_keeps_first_seq_as_canonical():
    state = RuntimeState()
    state.add_send_request(
        ParsedMessage(
            seq=11,
            channel_id=10001,
            principal=90002,
            recipients=[],
            kind="send.request",
            payload={},
            data={"msg_type": "text", "content": {"text": "first"}},
            event_ms=1,
            request_id="req-dup",
        )
    )
    state.add_send_request(
        ParsedMessage(
            seq=12,
            channel_id=10001,
            principal=90002,
            recipients=[],
            kind="send.request",
            payload={},
            data={"msg_type": "text", "content": {"text": "second"}},
            event_ms=2,
            request_id="req-dup",
        )
    )

    task = state.next_pending_task()

    assert task.seq == 11
    assert task.content == {"text": "first"}


def test_send_result_prev_seq_mismatch_does_not_clear_pending_request():
    state = RuntimeState()
    state.add_send_request(
        ParsedMessage(
            seq=11,
            channel_id=10001,
            principal=90002,
            recipients=[],
            kind="send.request",
            payload={},
            data={"msg_type": "text", "content": {"text": "hello"}},
            event_ms=1,
            request_id="req-prev",
        )
    )

    state.add_send_result(
        ParsedMessage(
            seq=12,
            channel_id=10001,
            principal=90001,
            recipients=[90002],
            kind="send.result",
            payload={},
            data={"status": "FAILED", "error_code": "PROVIDER_SEND_FAILED"},
            event_ms=2,
            request_id="req-prev",
            prev_seq=99,
        ),
        "lark",
        "oc_p2p_10001_bot",
    )

    assert "req-prev" in state.pending_outbound
    assert "req-prev" not in state.result_written


def test_blocked_channel_does_not_block_other_channel_pending_task():
    state = RuntimeState()
    state.add_send_request(
        ParsedMessage(
            seq=11,
            channel_id=10001,
            principal=90002,
            recipients=[],
            kind="send.request",
            payload={},
            data={"msg_type": "text", "content": {"text": "first"}},
            event_ms=1,
            request_id="req-ch1-a",
        )
    )
    state.add_send_request(
        ParsedMessage(
            seq=12,
            channel_id=10001,
            principal=90002,
            recipients=[],
            kind="send.request",
            payload={},
            data={"msg_type": "text", "content": {"text": "blocked"}},
            event_ms=2,
            request_id="req-ch1-b",
        )
    )
    state.add_send_request(
        ParsedMessage(
            seq=13,
            channel_id=10002,
            principal=90003,
            recipients=[],
            kind="send.request",
            payload={},
            data={"msg_type": "text", "content": {"text": "other"}},
            event_ms=3,
            request_id="req-ch2",
        )
    )

    first = state.next_pending_task()
    second = state.next_pending_task()

    assert first.request_id == "req-ch1-a"
    assert second.request_id == "req-ch2"


def test_mapping_requires_one_user_and_one_bot_participant():
    config = load_config("p2p_config.yaml")
    mapping = P2PMappingIndex(config)

    assert sorted(item.identity_type for item in mapping.entries_for_channel(10001)) == ["bot", "user"]
    assert mapping.peer_principals(10001, 10001) == [90002]


class FakeAdapter(ProviderAdapter):
    def __init__(self):
        self.calls = []

    def provider_name(self):
        return "lark"

    def stop_sync(self):
        return None

    def sync_once(self, session_id, cursor):
        raise AssertionError("not used")

    def send_message(self, **kwargs):
        self.calls.append(kwargs)
        return SendResult(success=True, provider_message_id="om_1")

    def health_check(self):
        return AdapterHealth(ok=True)


class FailingSendAdapter(ProviderAdapter):
    def __init__(self):
        self.calls = []

    def provider_name(self):
        return "lark"

    def stop_sync(self):
        return None

    def sync_once(self, session_id, cursor):
        raise AssertionError("not used")

    def send_message(self, **kwargs):
        self.calls.append(kwargs)
        return SendResult(
            success=False,
            retryable=True,
            error_code="RATE_LIMIT",
            error_message="provider rate limited",
        )

    def health_check(self):
        return AdapterHealth(ok=True)


class RaisingSendAdapter(ProviderAdapter):
    def __init__(self):
        self.calls = []

    def provider_name(self):
        return "lark"

    def stop_sync(self):
        return None

    def sync_once(self, session_id, cursor):
        raise AssertionError("not used")

    def send_message(self, **kwargs):
        self.calls.append(kwargs)
        raise RuntimeError("provider sdk failed")

    def health_check(self):
        return AdapterHealth(ok=True)


def test_bot_principal_can_send_request():
    config = load_config("p2p_config.yaml")
    state = RuntimeState()
    state.add_send_request(
        ParsedMessage(
            seq=11,
            channel_id=10001,
            principal=90002,
            recipients=[],
            kind="send.request",
            payload={},
            data={"msg_type": "text", "content": {"text": "hello"}},
            event_ms=1,
            request_id="req-bot",
        )
    )
    im_client = SimpleNamespace(calls=[])
    im_client.publish_send_result = lambda **kwargs: im_client.calls.append(kwargs) or 12
    adapter = FakeAdapter()
    processor = SingleThreadProcessor(
        im_client=im_client,
        mapping=P2PMappingIndex(config),
        adapters={"lark": adapter},
        retry=RetryConfig(publish_initial_backoff_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=state,
        logger=logging.getLogger("test"),
    )

    processor.tick()

    assert adapter.calls[0]["sender_external_user_id"] == "cli_xxx"
    assert im_client.calls[0]["recipients"] == [90002]
    assert im_client.calls[0]["req"].status == "SUCCESS"


def test_provider_send_failure_writes_failed_result_after_max_attempts():
    config = load_config("p2p_config.yaml")
    state = RuntimeState()
    state.add_send_request(
        ParsedMessage(
            seq=11,
            channel_id=10001,
            principal=90002,
            recipients=[],
            kind="send.request",
            payload={},
            data={"msg_type": "text", "content": {"text": "hello"}},
            event_ms=1,
            request_id="req-provider-fail",
        )
    )
    im_client = SimpleNamespace(calls=[])
    im_client.publish_send_result = lambda **kwargs: im_client.calls.append(kwargs) or 12
    adapter = FailingSendAdapter()
    processor = SingleThreadProcessor(
        im_client=im_client,
        mapping=P2PMappingIndex(config),
        adapters={"lark": adapter},
        retry=RetryConfig(
            publish_initial_backoff_ms=0,
            provider_send_max_attempts=2,
        ),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=state,
        logger=logging.getLogger("test"),
    )

    processor.tick()
    assert "req-provider-fail" in state.pending_outbound
    assert im_client.calls == []

    processor.tick()

    assert len(adapter.calls) == 2
    assert "req-provider-fail" not in state.pending_outbound
    assert "req-provider-fail" in state.result_written
    result = im_client.calls[0]["req"]
    assert result.status == "FAILED"
    assert result.error_code == "PROVIDER_SEND_FAILED"
    assert "RATE_LIMIT" in result.error_message


def test_provider_send_exception_counts_as_send_failure():
    config = load_config("p2p_config.yaml")
    state = RuntimeState()
    state.add_send_request(
        ParsedMessage(
            seq=11,
            channel_id=10001,
            principal=90002,
            recipients=[],
            kind="send.request",
            payload={},
            data={"msg_type": "text", "content": {"text": "hello"}},
            event_ms=1,
            request_id="req-provider-exception",
        )
    )
    im_client = SimpleNamespace(calls=[])
    im_client.publish_send_result = lambda **kwargs: im_client.calls.append(kwargs) or 12
    adapter = RaisingSendAdapter()
    processor = SingleThreadProcessor(
        im_client=im_client,
        mapping=P2PMappingIndex(config),
        adapters={"lark": adapter},
        retry=RetryConfig(
            publish_initial_backoff_ms=0,
            provider_send_max_attempts=2,
        ),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=state,
        logger=logging.getLogger("test"),
    )

    processor.tick()
    processor.tick()

    assert len(adapter.calls) == 2
    result = im_client.calls[0]["req"]
    assert result.status == "FAILED"
    assert result.error_code == "PROVIDER_SEND_FAILED"
    assert "RuntimeError" in result.error_message


def test_publish_retry_exhaustion_raises_fatal_syncer_error():
    config = load_config("p2p_config.yaml")
    processor = SingleThreadProcessor(
        im_client=SimpleNamespace(),
        mapping=P2PMappingIndex(config),
        adapters={},
        retry=RetryConfig(
            publish_max_attempts=2,
            publish_initial_backoff_ms=0,
            provider_send_max_attempts=1,
        ),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=RuntimeState(),
        logger=logging.getLogger("test"),
    )
    calls = []

    def fail_publish():
        calls.append(1)
        raise RuntimeError("publish unavailable")

    with pytest.raises(FatalSyncerError):
        processor._publish_with_retry(fail_publish)
    assert len(calls) == 2


def test_bot_provider_event_publishes_sync_record():
    config = load_config("p2p_config.yaml")
    im_client = SimpleNamespace(calls=[])
    im_client.publish_sync_record = lambda **kwargs: im_client.calls.append(kwargs) or 12
    processor = SingleThreadProcessor(
        im_client=im_client,
        mapping=P2PMappingIndex(config),
        adapters={},
        retry=RetryConfig(publish_initial_backoff_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=RuntimeState(),
        logger=logging.getLogger("test"),
    )

    assert processor._publish_provider_event(
        10001,
        ProviderEvent(
            provider="lark",
            session_id="oc_p2p_10001_bot",
            provider_message_id="om_bot",
            sender_external_user_id="cli_xxx",
            sender_identity_type="bot",
            msg_type="text",
            content_raw={"text": "bot reply"},
            event_ms=1,
            text="bot reply",
        ),
    )

    assert im_client.calls[0]["principal"] == 90002
    assert im_client.calls[0]["token"] == "tok-bot-90002"
    assert im_client.calls[0]["recipients"] == [10001]


class FakeSyncAdapter(ProviderAdapter):
    def __init__(self):
        self.calls = []

    def provider_name(self):
        return "lark"

    def stop_sync(self):
        return None

    def sync_once(self, session_id, cursor):
        self.calls.append((session_id, cursor))
        from openevent.im_p2p_syncer.models import SyncBatch

        return SyncBatch(events=[], cursor=cursor)

    def send_message(self, **kwargs):
        raise AssertionError("not used")

    def health_check(self):
        return AdapterHealth(ok=True)


def test_provider_poll_respects_interval_ms(monkeypatch):
    config = load_config("p2p_config.yaml")
    adapter = FakeSyncAdapter()
    now = {"value": 1000.0}
    monkeypatch.setattr("openevent.im_p2p_syncer.loop.time.time", lambda: now["value"])
    processor = SingleThreadProcessor(
        im_client=SimpleNamespace(),
        mapping=P2PMappingIndex(config),
        adapters={"lark": adapter},
        retry=RetryConfig(publish_initial_backoff_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=RuntimeState(),
        logger=logging.getLogger("test"),
    )

    processor.tick()
    processor.tick()
    now["value"] += config.providers["lark"].sync.interval_ms / 1000
    processor.tick()

    assert len(adapter.calls) == 2
