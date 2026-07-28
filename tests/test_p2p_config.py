from __future__ import annotations

import json
import logging
import queue
import threading
from types import SimpleNamespace

import pytest
from grpc import RpcError, StatusCode
from requests.exceptions import Timeout as RequestsTimeout

from openevent.im_sdk import ParsedMessage, PublishFailedError
from openevent.im_p2p_syncer.adapters.base import ProviderAdapter
from openevent.im_p2p_syncer.adapters.lark_openapi import LarkOpenAPIAdapter
from openevent.im_p2p_syncer.config import ConfigError
from openevent.im_p2p_syncer.config import load_config
from openevent.im_p2p_syncer.config import parse_config
from openevent.im_p2p_syncer.errors import (
    PermanentProviderError,
    ProviderDataError,
    StateConflictError,
    TransientProviderError,
)
from openevent.im_p2p_syncer.loop import FatalSyncerError, SingleThreadProcessor
from openevent.im_p2p_syncer.ids import stable_lark_openapi_uuid
from openevent.im_p2p_syncer.mapping import P2PMappingIndex
from openevent.im_p2p_syncer.models import HistoryPage, ProviderEvent, RetryConfig, SendResult
from openevent.im_p2p_syncer.provider_messages import ProviderMessagePuller
from openevent.im_p2p_syncer.state import RuntimeState
from openevent.im_p2p_syncer.syncer import P2PSyncer
from openevent.sdk.proto import openevent_pb2


class FakeRpcError(RpcError):
    def __init__(self, status_code):
        self._status_code = status_code

    def code(self):
        return self._status_code


def test_sample_config_loads():
    config = load_config("p2p_config.yaml")

    assert config.version == "v1"
    assert config.worker.principal == 90001
    assert config.retry.publish_retry_delay_ms == 200
    assert config.providers["lark"].options["timeout_seconds"] == 10
    assert config.providers["lark"].sync.history_retry_delay_ms == 1000
    assert config.providers["lark"].sync.history_overlap_ms == 300000
    assert sorted(config.principal_tokens) == [10001, 90002]


def test_live_fetch_cursor_allows_stable_tail():
    response = SimpleNamespace(next_seq=11, last_seq=10)

    assert P2PSyncer._validated_next_seq(response, 11) == 11


def test_live_fetch_cursor_rejects_nonadvancing_scan_page():
    response = SimpleNamespace(next_seq=11, last_seq=12)

    with pytest.raises(RuntimeError, match="did not advance"):
        P2PSyncer._validated_next_seq(response, 11)


def test_live_fetch_drains_fixed_snapshot_before_processing_provider_work():
    stop_event = threading.Event()
    processed = []
    tick_snapshots = []
    responses = [
        SimpleNamespace(
            messages=[SimpleNamespace(seq=1)],
            next_seq=2,
            last_seq=3,
        ),
        SimpleNamespace(
            messages=[SimpleNamespace(seq=2), SimpleNamespace(seq=3), SimpleNamespace(seq=4)],
            next_seq=5,
            last_seq=5,
        ),
        SimpleNamespace(
            messages=[SimpleNamespace(seq=5), SimpleNamespace(seq=6)],
            next_seq=7,
            last_seq=6,
        ),
    ]
    fetch_from_seq = []

    def fetch(*args, **kwargs):
        fetch_from_seq.append(kwargs["from_seq"])
        return responses.pop(0)

    def tick():
        tick_snapshots.append(list(processed))
        if len(tick_snapshots) == 2:
            stop_event.set()

    syncer = SimpleNamespace(
        openevent_client=SimpleNamespace(fetch=fetch),
        config=SimpleNamespace(
            worker=SimpleNamespace(principal=1, token="token"),
            retry=SimpleNamespace(idle_sleep_ms=0),
        ),
        _fetch_channels=lambda: (10001,),
        _process_live_message=lambda message: processed.append(message.seq),
        _validated_next_seq=P2PSyncer._validated_next_seq,
        processor=SimpleNamespace(tick=tick),
        _stop_event=stop_event,
        logger=logging.getLogger("test"),
    )

    P2PSyncer._poll_events(syncer, 1)

    assert fetch_from_seq == [1, 2, 5]
    assert tick_snapshots == [[1, 2, 3], [1, 2, 3, 4, 5]]


def test_history_parse_failure_aborts_recovery():
    syncer = SimpleNamespace(
        im_client=SimpleNamespace(
            parse_message=lambda message: (_ for _ in ()).throw(ValueError("invalid payload"))
        )
    )

    with pytest.raises(RuntimeError, match="history message seq=7"):
        P2PSyncer._process_history_message(syncer, SimpleNamespace(seq=7))


def test_live_parse_failure_aborts_worker():
    syncer = SimpleNamespace(
        im_client=SimpleNamespace(
            parse_message=lambda message: (_ for _ in ()).throw(ValueError("invalid payload"))
        )
    )

    with pytest.raises(FatalSyncerError, match="live OpenEvent message seq=8"):
        P2PSyncer._process_live_message(syncer, SimpleNamespace(seq=8))


@pytest.mark.parametrize(
    ("principal", "recipients"),
    [
        (90001, [90002]),
        (10001, []),
        (10001, [90001]),
    ],
)
def test_history_sync_record_requires_participant_to_peer_roles(principal, recipients):
    config = load_config("p2p_config.yaml")
    syncer = object.__new__(P2PSyncer)
    syncer.config = config
    syncer.mapping = P2PMappingIndex(config)
    syncer.state = RuntimeState()
    syncer.im_client = SimpleNamespace(
        parse_message=lambda message: ParsedMessage(
            seq=message.seq,
            channel_id=10001,
            principal=principal,
            recipients=recipients,
            kind="sync.record",
            payload={},
            data={
                "provider_message_id": "om_invalid_role",
                "msg_type": "text",
                "content_raw": {"text": "hello"},
            },
            event_ms=1,
        )
    )

    with pytest.raises(StateConflictError):
        syncer._process_history_message(SimpleNamespace(seq=1))


def test_p2p_channel_must_be_private():
    syncer = SimpleNamespace(
        config=SimpleNamespace(worker=SimpleNamespace(principal=90001)),
        mapping=SimpleNamespace(entries_for_channel=lambda channel_id: []),
    )
    channel = SimpleNamespace(
        visibility=openevent_pb2.VISIBILITY_PROTECTED,
        members=[90001],
    )

    with pytest.raises(ConfigError, match="visibility must be private"):
        P2PSyncer._validate_channel_members(syncer, 10001, channel)


def test_private_p2p_channel_must_include_worker_user_and_bot():
    entries = [SimpleNamespace(principal=10001), SimpleNamespace(principal=90002)]
    syncer = SimpleNamespace(
        config=SimpleNamespace(worker=SimpleNamespace(principal=90001)),
        mapping=SimpleNamespace(entries_for_channel=lambda channel_id: entries),
    )
    channel = SimpleNamespace(
        visibility=openevent_pb2.VISIBILITY_PRIVATE,
        members=[90001, 10001],
    )

    with pytest.raises(ConfigError, match=r"missing members: \[90002\]"):
        P2PSyncer._validate_channel_members(syncer, 10001, channel)


def test_private_p2p_channel_accepts_complete_membership():
    entries = [SimpleNamespace(principal=10001), SimpleNamespace(principal=90002)]
    syncer = SimpleNamespace(
        config=SimpleNamespace(worker=SimpleNamespace(principal=90001)),
        mapping=SimpleNamespace(entries_for_channel=lambda channel_id: entries),
    )
    channel = SimpleNamespace(
        visibility=openevent_pb2.VISIBILITY_PRIVATE,
        members=[90001, 10001, 90002],
    )

    P2PSyncer._validate_channel_members(syncer, 10001, channel)


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
        "worker": {"principal": 90001, "token": "tok-worker"},
        "openevent": {"target": "127.0.0.1:9527"},
        "principal_tokens": [
            {"principal": 10001, "token": "tok-user"},
            {"principal": 90002, "token": "tok-bot"},
        ],
        "providers": [
            {
                "name": "lark",
                "sync": {},
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
            },
            {
                "provider": "lark",
                "identity_type": "bot",
                "external_user_id": "cli_xxx",
                "principal": 90002,
                "session_id": "oc_p2p_10001_bot",
                "channel_id": 10001,
            },
        ],
    }

    config = parse_config(raw)
    mapping = P2PMappingIndex(config)

    assert config.providers["lark"].name == "lark"
    assert mapping.provider_session(10001) == ("lark", "oc_p2p_10001_bot")


@pytest.mark.parametrize(
    "field",
    ["mode", "interval_ms", "startup_lookback_ms", "history_interval_ms"],
)
def test_rejects_removed_poll_sync_fields(field):
    raw = _valid_raw_config()
    raw["providers"][0]["sync"][field] = "poll" if field == "mode" else 1

    with pytest.raises(ConfigError, match="contains removed fields"):
        parse_config(raw)


def test_rejects_removed_openevent_rpc_timeout():
    raw = _valid_raw_config()
    raw["openevent"]["rpc_timeout_seconds"] = 10

    with pytest.raises(ConfigError, match="contains removed fields: rpc_timeout_seconds"):
        parse_config(raw)


@pytest.mark.parametrize(
    "field",
    [
        "provider_send_max_attempts",
        "provider_send_retry_delay_ms",
        "publish_initial_backoff_ms",
        "publish_max_backoff_ms",
    ],
)
def test_rejects_removed_retry_fields(field):
    raw = _valid_raw_config()
    raw["retry"] = {field: 1}

    with pytest.raises(ConfigError, match="contains removed fields"):
        parse_config(raw)


@pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan"), True])
def test_rejects_invalid_lark_timeout(value):
    raw = _valid_raw_config()
    raw["providers"][0]["options"] = {"timeout_seconds": value}

    with pytest.raises(ConfigError, match="timeout_seconds must be a positive number"):
        parse_config(raw)


def test_rejects_lark_bot_mapping_that_does_not_match_app_id():
    raw = _valid_raw_config()
    raw["mappings"][1]["external_user_id"] = "cli_other"

    with pytest.raises(ConfigError, match="must use credentials.app_id"):
        parse_config(raw)


def _valid_raw_config():
    return {
        "version": "v1",
        "worker": {"principal": 90001, "token": "tok-worker"},
        "openevent": {"target": "127.0.0.1:9527"},
        "principal_tokens": [
            {"principal": 10001, "token": "tok-user"},
            {"principal": 90002, "token": "tok-bot"},
        ],
        "providers": [
            {
                "name": "lark",
                "sync": {},
                "credentials": {"app_id": "cli_xxx", "app_secret": "secret"},
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
            },
            {
                "provider": "lark",
                "identity_type": "bot",
                "external_user_id": "cli_xxx",
                "principal": 90002,
                "session_id": "oc_p2p_10001_bot",
                "channel_id": 10001,
            },
        ],
    }


@pytest.mark.parametrize(("section", "field"), [("provider", "adapter"), ("provider", "enabled"), ("mapping", "status")])
def test_rejects_removed_activation_and_adapter_fields(section, field):
    raw = _valid_raw_config()
    target = raw["providers"][0] if section == "provider" else raw["mappings"][0]
    target[field] = True if field == "enabled" else "active"

    with pytest.raises(ConfigError, match="contains removed fields"):
        parse_config(raw)


def test_rejects_unsupported_provider_name():
    raw = _valid_raw_config()
    raw["providers"][0]["name"] = "custom"

    with pytest.raises(ConfigError, match="name must be feishu or lark"):
        parse_config(raw)


def test_mapping_missing_exits_without_writing_result():
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
        adapters={"lark": FakeAdapter()},
        retry=RetryConfig(publish_retry_delay_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=state,
        logger=logging.getLogger("test"),
    )

    with pytest.raises(FatalSyncerError, match="has no bot mapping"):
        processor.tick()

    assert "req-missing" in state.requests_by_id
    assert "req-missing" not in state.completed_request_ids
    assert im_client.calls == []


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

    assert "req-failed" not in state.requests_by_id
    assert "req-failed" in state.completed_request_ids
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
            data={"msg_type": "text", "content": {"text": "first"}},
            event_ms=2,
            request_id="req-dup",
        )
    )

    task = state.next_pending_task()

    assert task.seq == 11
    assert task.content == {"text": "first"}


def test_conflicting_duplicate_send_request_fails():
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
            request_id="req-conflict",
        )
    )

    with pytest.raises(StateConflictError, match="conflicting send.request"):
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
                request_id="req-conflict",
            )
        )


def test_conflicting_duplicate_send_request_fails_after_result():
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
            request_id="req-completed-conflict",
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
            data={"status": "FAILED"},
            event_ms=2,
            request_id="req-completed-conflict",
            prev_seq=11,
        ),
        "lark",
        "oc_p2p_10001_bot",
    )

    with pytest.raises(StateConflictError, match="conflicting send.request"):
        state.add_send_request(
            ParsedMessage(
                seq=13,
                channel_id=10001,
                principal=90002,
                recipients=[],
                kind="send.request",
                payload={},
                data={"msg_type": "text", "content": {"text": "second"}},
                event_ms=3,
                request_id="req-completed-conflict",
            )
        )


def test_send_result_prev_seq_mismatch_fails():
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

    with pytest.raises(StateConflictError, match="prev_seq"):
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

    assert "req-prev" in state.requests_by_id
    assert "req-prev" not in state.completed_request_ids


def test_send_result_recipients_mismatch_fails():
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
            request_id="req-recipients",
        )
    )

    with pytest.raises(StateConflictError, match="recipients"):
        state.add_send_result(
            ParsedMessage(
                seq=12,
                channel_id=10001,
                principal=90001,
                recipients=[10001],
                kind="send.result",
                payload={},
                data={"status": "FAILED"},
                event_ms=2,
                request_id="req-recipients",
                prev_seq=11,
            ),
            "lark",
            "oc_p2p_10001_bot",
        )


def test_conflicting_duplicate_send_result_fails():
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
            request_id="req-result-conflict",
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
            data={"status": "SUCCESS", "provider_message_id": "om_1"},
            event_ms=2,
            request_id="req-result-conflict",
            prev_seq=11,
        ),
        "lark",
        "oc_p2p_10001_bot",
    )

    with pytest.raises(StateConflictError, match="conflicting send.result"):
        state.add_send_result(
            ParsedMessage(
                seq=13,
                channel_id=10001,
                principal=90001,
                recipients=[90002],
                kind="send.result",
                payload={},
                data={"status": "SUCCESS", "provider_message_id": "om_2"},
                event_ms=3,
                request_id="req-result-conflict",
                prev_seq=11,
            ),
            "lark",
            "oc_p2p_10001_bot",
        )


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

    def start_event_stream(self, session_ids):
        return None

    def stop_event_stream(self):
        return None

    def event_stream_error(self):
        return None

    def event_stream_generation(self):
        return 0

    def take_event(self):
        return None

    def fetch_history_page(self, **kwargs):
        return HistoryPage(events=[])

    def send_message(self, **kwargs):
        self.calls.append(kwargs)
        return SendResult(success=True, provider_message_id="om_1")

class FailingSendAdapter(FakeAdapter):
    def __init__(self):
        self.calls = []

    def send_message(self, **kwargs):
        self.calls.append(kwargs)
        return SendResult(
            success=False,
            error_code="RATE_LIMIT",
            error_message="provider rate limited",
        )

class RaisingSendAdapter(FakeAdapter):
    def __init__(self):
        self.calls = []

    def send_message(self, **kwargs):
        self.calls.append(kwargs)
        raise RuntimeError("provider sdk failed")

class FakeLarkResponse:
    def __init__(self, *, success=True, code=0, msg="ok", data=None):
        self._success = success
        self.code = code
        self.msg = msg
        self.data = data

    def success(self):
        return self._success

    def get_log_id(self):
        return "log-123"

    def get_troubleshooter(self):
        return "trouble-123"


class FakeLarkMessageClient:
    def __init__(self, *, create_response):
        self.create_response = create_response
        self.create_requests = []

    def create(self, request):
        self.create_requests.append(request)
        return self.create_response


class RaisingLarkMessageClient:
    def create(self, request):
        raise RequestsTimeout("request timed out")


class FakeLarkListMessageClient:
    def __init__(self, response):
        self.response = response
        self.requests = []

    def list(self, request):
        self.requests.append(request)
        return self.response


class RaisingLarkListMessageClient:
    def list(self, request):
        raise RequestsTimeout("request timed out")


def _fake_lark_adapter(message_client):
    adapter = object.__new__(LarkOpenAPIAdapter)
    adapter._config = load_config("p2p_config.yaml").providers["lark"]
    adapter._client = SimpleNamespace(
        im=SimpleNamespace(v1=SimpleNamespace(message=message_client))
    )
    adapter._events = queue.Queue(maxsize=adapter._config.sync.event_queue_size)
    adapter._allowed_sessions = {"oc_p2p_10001_bot"}
    adapter._event_stream = None
    return adapter


def test_lark_history_rejects_has_more_without_page_token():
    message_client = FakeLarkListMessageClient(
        FakeLarkResponse(
            data=SimpleNamespace(items=[], has_more=True, page_token=None)
        )
    )
    adapter = _fake_lark_adapter(message_client)

    with pytest.raises(RuntimeError, match="has_more without page_token"):
        adapter.fetch_history_page(
            "oc_p2p_10001_bot",
            start_ms=1000,
            end_ms=2000,
            page_token=None,
        )

    assert len(message_client.requests) == 1


def test_lark_history_classifies_retryable_response_as_transient():
    response = FakeLarkResponse(success=False, code=99991402, msg="rate limited")
    adapter = _fake_lark_adapter(FakeLarkListMessageClient(response))

    with pytest.raises(TransientProviderError, match="list messages failed"):
        adapter.fetch_history_page(
            "oc_p2p_10001_bot", start_ms=1000, end_ms=2000, page_token=None
        )


def test_lark_history_classifies_request_timeout_as_transient():
    adapter = _fake_lark_adapter(RaisingLarkListMessageClient())

    with pytest.raises(TransientProviderError, match="temporarily failed"):
        adapter.fetch_history_page(
            "oc_p2p_10001_bot", start_ms=1000, end_ms=2000, page_token=None
        )


def test_lark_history_classifies_rejected_response_as_permanent():
    response = FakeLarkResponse(success=False, code=99991663, msg="permission denied")
    adapter = _fake_lark_adapter(FakeLarkListMessageClient(response))

    with pytest.raises(PermanentProviderError, match="list messages failed"):
        adapter.fetch_history_page(
            "oc_p2p_10001_bot", start_ms=1000, end_ms=2000, page_token=None
        )


def test_lark_message_event_is_normalized_and_queued():
    adapter = _fake_lark_adapter(SimpleNamespace())
    adapter._handle_message_event(
        SimpleNamespace(
            event=SimpleNamespace(
                message=SimpleNamespace(
                    message_id="om_event",
                    chat_id="oc_p2p_10001_bot",
                    message_type="text",
                    create_time=123,
                    content=json.dumps({"text": "hello"}),
                ),
                sender=SimpleNamespace(
                    sender_id=SimpleNamespace(open_id="ou_source"),
                    sender_type="user",
                ),
            )
        )
    )

    event = adapter.take_event()

    assert event.provider_message_id == "om_event"
    assert event.sender_external_user_id == "ou_source"
    assert event.text == "hello"


@pytest.mark.parametrize("sender_type", ["app", "bot"])
def test_lark_bot_message_event_is_normalized_and_queued(sender_type):
    adapter = _fake_lark_adapter(SimpleNamespace())
    adapter._handle_message_event(
        SimpleNamespace(
            event=SimpleNamespace(
                message=SimpleNamespace(
                    message_id="om_bot_event",
                    chat_id="oc_p2p_10001_bot",
                    message_type="text",
                    create_time=123,
                    content=json.dumps({"text": "bot reply"}),
                ),
                sender=SimpleNamespace(
                    sender_id=SimpleNamespace(open_id=None),
                    sender_type=sender_type,
                ),
            )
        )
    )

    event = adapter.take_event()

    assert event.provider_message_id == "om_bot_event"
    assert event.sender_identity_type == "bot"
    assert event.sender_external_user_id == "cli_xxx"
    assert event.text == "bot reply"


def test_lark_history_rejects_unknown_sender_type():
    item = _fake_lark_message()
    item.sender.sender_type = "unknown"
    adapter = _fake_lark_adapter(
        FakeLarkListMessageClient(
            FakeLarkResponse(data=SimpleNamespace(items=[item], has_more=False))
        )
    )

    with pytest.raises(RuntimeError, match="malformed sender"):
        adapter.fetch_history_page(
            "oc_p2p_10001_bot",
            start_ms=1000,
            end_ms=2000,
            page_token=None,
        )


def test_lark_history_rejects_sender_id_type_mismatch():
    item = _fake_lark_message()
    item.sender.id_type = "open_id"
    adapter = _fake_lark_adapter(
        FakeLarkListMessageClient(
            FakeLarkResponse(data=SimpleNamespace(items=[item], has_more=False))
        )
    )

    with pytest.raises(ProviderDataError, match="malformed item"):
        adapter.fetch_history_page(
            "oc_p2p_10001_bot",
            start_ms=1000,
            end_ms=2000,
            page_token=None,
        )


def test_lark_history_rejects_message_from_another_chat():
    item = _fake_lark_message(chat_id="oc_other")
    adapter = _fake_lark_adapter(
        FakeLarkListMessageClient(
            FakeLarkResponse(data=SimpleNamespace(items=[item], has_more=False))
        )
    )

    with pytest.raises(ProviderDataError, match="another chat"):
        adapter.fetch_history_page(
            "oc_p2p_10001_bot",
            start_ms=1000,
            end_ms=2000,
            page_token=None,
        )


def test_lark_message_event_raises_when_queue_is_full():
    adapter = _fake_lark_adapter(SimpleNamespace())
    adapter._events = queue.Queue(maxsize=1)
    data = SimpleNamespace(
        event=SimpleNamespace(
            message=SimpleNamespace(
                message_id="om_event",
                chat_id="oc_p2p_10001_bot",
                message_type="text",
                create_time=123,
                content=json.dumps({"text": "hello"}),
            ),
            sender=SimpleNamespace(
                sender_id=SimpleNamespace(open_id="ou_source"),
                sender_type="user",
            ),
        )
    )
    adapter._handle_message_event(data)

    with pytest.raises(RuntimeError, match="event queue is full"):
        adapter._handle_message_event(data)


def _fake_lark_message(
    *,
    message_id="om_1",
    chat_id="oc_p2p_10001_bot",
    msg_type="text",
    content=None,
    deleted=False,
):
    if content is None:
        content = {"text": "hello"}
    return SimpleNamespace(
        message_id=message_id,
        chat_id=chat_id,
        msg_type=msg_type,
        create_time=123,
        update_time=124,
        deleted=deleted,
        updated=False,
        sender=SimpleNamespace(id="cli_xxx", id_type="app_id", sender_type="app"),
        body=SimpleNamespace(content=json.dumps(content, ensure_ascii=False, separators=(",", ":"))),
    )


def test_lark_send_message_logs_create_response(caplog):
    message_client = FakeLarkMessageClient(
        create_response=FakeLarkResponse(data=_fake_lark_message())
    )
    adapter = _fake_lark_adapter(message_client)

    with caplog.at_level(logging.INFO, logger="openevent.im_p2p_syncer.adapters.lark_openapi"):
        result = adapter.send_message(
            session_id="oc_p2p_10001_bot",
            sender_external_user_id="cli_xxx",
            msg_type="text",
            content={"text": "hello"},
            request_id="req-lark-log",
        )

    assert result.success
    assert result.provider_message_id == "om_1"
    assert "lark_message_create_response" in caplog.text
    assert "message_id" in caplog.text
    assert "om_1" in caplog.text
    assert "chat_id" in caplog.text
    assert "oc_p2p_10001_bot" in caplog.text
    assert "log-123" in caplog.text


def test_lark_send_message_accepts_sparse_success_without_extra_get(caplog):
    message_client = FakeLarkMessageClient(
        create_response=FakeLarkResponse(data=SimpleNamespace(message_id="om_1")),
    )
    adapter = _fake_lark_adapter(message_client)

    with caplog.at_level(logging.INFO, logger="openevent.im_p2p_syncer.adapters.lark_openapi"):
        result = adapter.send_message(
            session_id="oc_p2p_10001_bot",
            sender_external_user_id="cli_xxx",
            msg_type="text",
            content={"text": "hello"},
            request_id="req-lark-get-log",
        )

    assert result.success
    assert len(message_client.create_requests) == 1
    assert "lark_message_create_response" in caplog.text
    assert "om_1" in caplog.text


def test_lark_send_message_does_not_retry_successful_create_response():
    message_client = FakeLarkMessageClient(
        create_response=FakeLarkResponse(
            data=_fake_lark_message(chat_id="oc_wrong")
        )
    )
    adapter = _fake_lark_adapter(message_client)

    result = adapter.send_message(
        session_id="oc_p2p_10001_bot",
        sender_external_user_id="cli_xxx",
        msg_type="text",
        content={"text": "hello"},
        request_id="req-lark-unconfirmed",
    )

    assert result.success
    assert result.provider_message_id == "om_1"
    assert len(message_client.create_requests) == 1


@pytest.mark.parametrize("code", [99991402, 11020, 11021])
def test_lark_rate_limit_response_is_failure(code):
    adapter = _fake_lark_adapter(
        FakeLarkMessageClient(
            create_response=FakeLarkResponse(success=False, code=code, msg="rate limited"),
        )
    )

    result = adapter.send_message(
        session_id="oc_p2p_10001_bot",
        sender_external_user_id="cli_xxx",
        msg_type="text",
        content={"text": "hello"},
        request_id="req-rate-limit",
    )

    assert not result.success
    assert result.error_code == str(code)


def test_lark_http_5xx_response_is_failure():
    response = FakeLarkResponse(success=False, code=1, msg="service unavailable")
    response.raw = SimpleNamespace(status_code=503)
    adapter = _fake_lark_adapter(FakeLarkMessageClient(create_response=response))

    result = adapter.send_message(
        session_id="oc_p2p_10001_bot",
        sender_external_user_id="cli_xxx",
        msg_type="text",
        content={"text": "hello"},
        request_id="req-service-unavailable",
    )

    assert not result.success


def test_lark_business_error_is_failure():
    adapter = _fake_lark_adapter(
        FakeLarkMessageClient(
            create_response=FakeLarkResponse(success=False, code=230001, msg="invalid content"),
        )
    )

    result = adapter.send_message(
        session_id="oc_p2p_10001_bot",
        sender_external_user_id="cli_xxx",
        msg_type="text",
        content={"text": "hello"},
        request_id="req-invalid-content",
    )

    assert not result.success


def test_lark_network_timeout_is_failure():
    adapter = _fake_lark_adapter(RaisingLarkMessageClient())

    result = adapter.send_message(
        session_id="oc_p2p_10001_bot",
        sender_external_user_id="cli_xxx",
        msg_type="text",
        content={"text": "hello"},
        request_id="req-timeout",
    )

    assert not result.success
    assert result.error_code == "Timeout"


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
        retry=RetryConfig(publish_retry_delay_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=state,
        logger=logging.getLogger("test"),
    )

    processor.tick()

    assert adapter.calls[0]["sender_external_user_id"] == "cli_xxx"
    assert im_client.calls[0]["recipients"] == [90002]
    assert im_client.calls[0]["req"].status == "SUCCESS"


def test_provider_send_failure_exits_immediately_without_result():
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
        retry=RetryConfig(publish_retry_delay_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=state,
        logger=logging.getLogger("test"),
    )

    with pytest.raises(FatalSyncerError, match="provider send failed"):
        processor.tick()

    assert len(adapter.calls) == 1
    assert "req-provider-fail" in state.requests_by_id
    assert "req-provider-fail" not in state.completed_request_ids
    assert im_client.calls == []


def test_provider_send_exception_exits_immediately():
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
        retry=RetryConfig(publish_retry_delay_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=state,
        logger=logging.getLogger("test"),
    )

    with pytest.raises(FatalSyncerError, match="RuntimeError"):
        processor.tick()

    assert len(adapter.calls) == 1
    assert "req-provider-exception" in state.requests_by_id
    assert "req-provider-exception" not in state.completed_request_ids
    assert im_client.calls == []


@pytest.mark.parametrize("status_code", [StatusCode.UNAVAILABLE, StatusCode.DEADLINE_EXCEEDED])
def test_publish_retry_exhaustion_uses_fixed_delay(status_code):
    config = load_config("p2p_config.yaml")
    processor = SingleThreadProcessor(
        im_client=SimpleNamespace(),
        mapping=P2PMappingIndex(config),
        adapters={},
        retry=RetryConfig(
            publish_max_attempts=2,
            publish_retry_delay_ms=250,
        ),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=RuntimeState(),
        logger=logging.getLogger("test"),
    )
    calls = []
    waits = []
    processor._stop_event = SimpleNamespace(wait=lambda seconds: waits.append(seconds) or False)

    def fail_publish():
        calls.append(1)
        raise PublishFailedError("publish not committed", retry_safe=True) from FakeRpcError(
            status_code
        )

    with pytest.raises(FatalSyncerError):
        processor._publish_with_retry(fail_publish)
    assert len(calls) == 2
    assert waits == [0.25]


@pytest.mark.parametrize(
    "status_code",
    [StatusCode.UNAUTHENTICATED, StatusCode.PERMISSION_DENIED, StatusCode.CANCELLED],
)
def test_publish_permanent_or_unclassified_rpc_error_is_not_retried(status_code):
    config = load_config("p2p_config.yaml")
    processor = SingleThreadProcessor(
        im_client=SimpleNamespace(),
        mapping=P2PMappingIndex(config),
        adapters={},
        retry=RetryConfig(publish_max_attempts=3, publish_retry_delay_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=RuntimeState(),
        logger=logging.getLogger("test"),
    )
    calls = []

    def fail_publish():
        calls.append(1)
        raise PublishFailedError("publish not committed", retry_safe=True) from FakeRpcError(
            status_code
        )

    with pytest.raises(FatalSyncerError):
        processor._publish_with_retry(fail_publish)
    assert len(calls) == 1


def test_publish_unknown_outcome_is_not_retried():
    config = load_config("p2p_config.yaml")
    processor = SingleThreadProcessor(
        im_client=SimpleNamespace(),
        mapping=P2PMappingIndex(config),
        adapters={},
        retry=RetryConfig(publish_max_attempts=3, publish_retry_delay_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=RuntimeState(),
        logger=logging.getLogger("test"),
    )
    calls = []

    def fail_publish():
        calls.append(1)
        raise PublishFailedError("publish outcome unknown", outcome_unknown=True)

    with pytest.raises(FatalSyncerError):
        processor._publish_with_retry(fail_publish)
    assert len(calls) == 1


def test_bot_provider_event_publishes_sync_record():
    config = load_config("p2p_config.yaml")
    im_client = SimpleNamespace(calls=[])
    im_client.publish_sync_record = lambda **kwargs: im_client.calls.append(kwargs) or 12
    processor = SingleThreadProcessor(
        im_client=im_client,
        mapping=P2PMappingIndex(config),
        adapters={},
        retry=RetryConfig(publish_retry_delay_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=RuntimeState(),
        logger=logging.getLogger("test"),
    )

    processor._publish_provider_event(
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


def test_degraded_sync_record_too_large_fails_without_marking_message_seen():
    config = load_config("p2p_config.yaml")
    state = RuntimeState()
    calls = []

    def reject_as_too_large(**kwargs):
        calls.append(kwargs["req"])
        raise PublishFailedError("payload too large") from FakeRpcError(
            StatusCode.RESOURCE_EXHAUSTED
        )

    processor = SingleThreadProcessor(
        im_client=SimpleNamespace(publish_sync_record=reject_as_too_large),
        mapping=P2PMappingIndex(config),
        adapters={},
        retry=RetryConfig(publish_retry_delay_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=state,
        logger=logging.getLogger("test"),
    )
    event = ProviderEvent(
        provider="lark",
        session_id="oc_p2p_10001_bot",
        provider_message_id="om_too_large",
        sender_external_user_id="ou_source",
        msg_type="text",
        content_raw={"text": "oversized"},
        event_ms=1,
        text="oversized",
    )

    with pytest.raises(FatalSyncerError, match="degraded sync.record still exceeds"):
        processor._publish_provider_event(10001, event)

    assert len(calls) == 2
    assert calls[0].content_omitted is None
    assert calls[1].content_raw == {
        "omitted": True,
        "reason": "message_too_large",
        "metadata": {
            "provider": "lark",
            "session_id": "oc_p2p_10001_bot",
            "sender_identity_type": "user",
            "sender_external_user_id": "ou_source",
            "msg_type": "text",
        },
    }
    assert calls[1].content_omitted is True
    assert calls[1].omit_reason == "message_too_large"
    assert state.inbound_seen == set()
    assert state.latest_event_ms(10001) is None


def test_provider_event_with_missing_sender_mapping_is_fatal():
    config = load_config("p2p_config.yaml")
    processor = SingleThreadProcessor(
        im_client=SimpleNamespace(),
        mapping=P2PMappingIndex(config),
        adapters={},
        retry=RetryConfig(publish_retry_delay_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=RuntimeState(),
        logger=logging.getLogger("test"),
    )

    with pytest.raises(FatalSyncerError, match="sender mapping missing"):
        processor._publish_provider_event(
            10001,
            ProviderEvent(
                provider="lark",
                session_id="oc_p2p_10001_bot",
                provider_message_id="om_unknown",
                sender_external_user_id="ou_unknown",
                msg_type="text",
                content_raw={"text": "hello"},
                event_ms=1,
            ),
        )


class FakeSyncAdapter(ProviderAdapter):
    def __init__(self, *, pages=None, events=None):
        self.calls = []
        self.events = list(events or [])
        self.pages = list(pages if pages is not None else [HistoryPage(events=[])])
        self.generation = 0
        self.stream_error = None

    def provider_name(self):
        return "lark"

    def start_event_stream(self, session_ids):
        self.sessions = session_ids

    def stop_event_stream(self):
        return None

    def event_stream_error(self):
        return self.stream_error

    def event_stream_generation(self):
        return self.generation

    def take_event(self):
        return self.events.pop(0) if self.events else None

    def fetch_history_page(self, **kwargs):
        self.calls.append(kwargs)
        return self.pages.pop(0) if self.pages else HistoryPage(events=[])

    def send_message(self, **kwargs):
        raise AssertionError("not used")


class FairWorkAdapter(FakeSyncAdapter):
    def __init__(self, *, pages=None, events=None):
        super().__init__(pages=pages, events=events)
        self.send_calls = []

    def send_message(self, **kwargs):
        self.send_calls.append(kwargs)
        return SendResult(
            success=True,
            provider_message_id=f"om_sent_{len(self.send_calls)}",
        )


class InvalidHistoryAdapter(FakeSyncAdapter):
    def fetch_history_page(self, **kwargs):
        raise ProviderDataError("malformed history item")


class TransientHistoryAdapter(FakeSyncAdapter):
    def fetch_history_page(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) == 1:
            raise TransientProviderError("temporarily unavailable")
        return HistoryPage(events=[])


def _start_message_puller(config, adapter, session_highwater_ms=None):
    puller = ProviderMessagePuller(
        adapter=adapter,
        config=config.providers["lark"].sync,
        logger=logging.getLogger("test"),
    )
    puller.start(
        session_highwater_ms
        or {"oc_p2p_10001_bot": None}
    )
    return puller


def _provider_event(
    provider_message_id,
    *,
    session_id="oc_p2p_10001_bot",
    sender_external_user_id="ou_source",
    event_ms=1,
):
    return ProviderEvent(
        provider="lark",
        session_id=session_id,
        provider_message_id=provider_message_id,
        sender_external_user_id=sender_external_user_id,
        msg_type="text",
        content_raw={"text": provider_message_id},
        event_ms=event_ms,
        text=provider_message_id,
    )


def _add_outbound_request(state, seq, request_id, channel_id=10001, principal=90002):
    state.add_send_request(
        ParsedMessage(
            seq=seq,
            channel_id=channel_id,
            principal=principal,
            recipients=[],
            kind="send.request",
            payload={},
            data={"msg_type": "text", "content": {"text": request_id}},
            event_ms=seq,
            request_id=request_id,
        )
    )


def test_work_types_rotate_fairly():
    config = load_config("p2p_config.yaml")
    processor = SingleThreadProcessor(
        im_client=SimpleNamespace(),
        mapping=P2PMappingIndex(config),
        adapters={},
        retry=RetryConfig(publish_retry_delay_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=RuntimeState(),
        logger=logging.getLogger("test"),
    )
    calls = []
    processor._process_next_outbound_task = lambda: calls.append("outbound") or True
    processor._process_next_provider_event = lambda: calls.append("provider") or True

    processor.tick()
    processor.tick()
    processor.tick()
    processor.tick()

    assert calls == ["outbound", "provider", "outbound", "provider"]


def test_history_message_uses_same_channel_serial_path_as_subscription_message():
    config = load_config("p2p_config.yaml")
    state = RuntimeState()
    _add_outbound_request(state, 1, "req-pending")
    adapter = FairWorkAdapter(
        pages=[HistoryPage(events=[_provider_event("om_history", event_ms=2)])]
    )
    puller = _start_message_puller(config, adapter)
    order = []
    im_client = SimpleNamespace()
    im_client.publish_send_result = lambda **kwargs: order.append("send.result") or 10
    im_client.publish_sync_record = lambda **kwargs: order.append("sync.record") or 11
    processor = SingleThreadProcessor(
        im_client=im_client,
        mapping=P2PMappingIndex(config),
        adapters={"lark": adapter},
        message_pullers={"lark": puller},
        retry=RetryConfig(publish_retry_delay_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=state,
        logger=logging.getLogger("test"),
    )

    processor.tick()
    assert order == ["send.result"]

    processor.tick()
    assert order == ["send.result", "sync.record"]
    assert state.inbound_seen == {
        ("lark", "oc_p2p_10001_bot", 10001, "om_history")
    }


def test_outbound_backlog_does_not_starve_other_channel_provider_work():
    raw = _valid_raw_config()
    raw["principal_tokens"].extend(
        [
            {"principal": 10002, "token": "tok-user-2"},
            {"principal": 90003, "token": "tok-bot-2"},
        ]
    )
    raw["mappings"].extend(
        [
            {
                "provider": "lark",
                "identity_type": "user",
                "external_user_id": "ou_second",
                "principal": 10002,
                "session_id": "oc_second",
                "channel_id": 10002,
            },
            {
                "provider": "lark",
                "identity_type": "bot",
                "external_user_id": "cli_xxx",
                "principal": 90003,
                "session_id": "oc_second",
                "channel_id": 10002,
            },
        ]
    )
    config = parse_config(raw)
    state = RuntimeState()
    for seq in range(1, 5):
        _add_outbound_request(state, seq, f"req-{seq}")
    adapter = FairWorkAdapter(
        events=[
            _provider_event(
                "om_live_second",
                session_id="oc_second",
                sender_external_user_id="ou_second",
                event_ms=10,
            )
        ]
    )
    puller = _start_message_puller(
        config,
        adapter,
        {
            "oc_p2p_10001_bot": None,
            "oc_second": None,
        },
    )
    im_client = SimpleNamespace(send_results=[], sync_records=[])
    im_client.publish_send_result = (
        lambda **kwargs: im_client.send_results.append(kwargs) or 100 + len(im_client.send_results)
    )
    im_client.publish_sync_record = (
        lambda **kwargs: im_client.sync_records.append(kwargs) or 200 + len(im_client.sync_records)
    )
    processor = SingleThreadProcessor(
        im_client=im_client,
        mapping=P2PMappingIndex(config),
        adapters={"lark": adapter},
        message_pullers={"lark": puller},
        retry=RetryConfig(publish_retry_delay_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=state,
        logger=logging.getLogger("test"),
    )

    processor.tick()
    processor.tick()
    processor.tick()

    assert len(adapter.send_calls) == 2
    assert len(im_client.sync_records) == 1
    assert len(state.requests_by_id) == 2


def test_unfinished_request_blocks_provider_work_in_same_channel():
    config = load_config("p2p_config.yaml")
    state = RuntimeState()
    _add_outbound_request(state, 1, "req-pending")
    processor = SingleThreadProcessor(
        im_client=SimpleNamespace(),
        mapping=P2PMappingIndex(config),
        adapters={},
        retry=RetryConfig(publish_retry_delay_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=state,
        logger=logging.getLogger("test"),
    )

    assert processor._channel_has_pending(10001)


def test_message_pull_failure_preempts_outbound_backlog():
    config = load_config("p2p_config.yaml")
    state = RuntimeState()
    _add_outbound_request(state, 1, "req-pending")
    adapter = FakeSyncAdapter()
    adapter.stream_error = RuntimeError("disconnected")
    puller = _start_message_puller(config, adapter)
    processor = SingleThreadProcessor(
        im_client=SimpleNamespace(),
        mapping=P2PMappingIndex(config),
        adapters={"lark": adapter},
        message_pullers={"lark": puller},
        retry=RetryConfig(publish_retry_delay_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=state,
        logger=logging.getLogger("test"),
    )

    with pytest.raises(FatalSyncerError, match="provider message pull failed"):
        processor.tick()

    assert "req-pending" in state.requests_by_id


def test_provider_history_data_error_fails_through_message_puller():
    config = load_config("p2p_config.yaml")
    adapter = InvalidHistoryAdapter()
    puller = _start_message_puller(config, adapter)
    processor = SingleThreadProcessor(
        im_client=SimpleNamespace(),
        mapping=P2PMappingIndex(config),
        adapters={"lark": adapter},
        message_pullers={"lark": puller},
        retry=RetryConfig(publish_retry_delay_ms=0),
        worker_principal=config.worker.principal,
        worker_token=config.worker.token,
        state=RuntimeState(),
        logger=logging.getLogger("test"),
    )

    with pytest.raises(FatalSyncerError, match="provider message pull failed"):
        processor.tick()


def test_transient_history_error_keeps_query_for_fixed_delay(monkeypatch):
    config = load_config("p2p_config.yaml")
    now = {"value": 1000.0}
    monkeypatch.setattr("openevent.im_p2p_syncer.provider_messages.time.time", lambda: now["value"])
    adapter = TransientHistoryAdapter()
    puller = _start_message_puller(config, adapter)

    assert puller.take_message() is None
    assert puller.take_message() is None
    assert len(adapter.calls) == 1

    now["value"] += config.providers["lark"].sync.history_retry_delay_ms / 1000
    assert puller.take_message() is None

    assert len(adapter.calls) == 2
    assert adapter.calls[1] == adapter.calls[0]


def test_provider_message_puller_requires_ack_and_delivers_history_before_live():
    config = load_config("p2p_config.yaml")
    history_event = _provider_event("om_history", event_ms=1)
    live_event = _provider_event("om_live", event_ms=2)
    adapter = FakeSyncAdapter(
        pages=[HistoryPage(events=[history_event])],
        events=[live_event],
    )
    puller = _start_message_puller(config, adapter)

    assert puller.take_message() == history_event
    assert puller.take_message() is None

    puller.acknowledge(history_event)
    assert puller.take_message() == live_event


def test_reconnect_handoff_uses_completed_window_and_is_not_periodic(monkeypatch):
    config = load_config("p2p_config.yaml")
    now = {"value": 1000.0}
    monkeypatch.setattr("openevent.im_p2p_syncer.provider_messages.time.time", lambda: now["value"])
    adapter = FakeSyncAdapter(
        pages=[
            HistoryPage(events=[], next_page_token="next"),
            HistoryPage(events=[]),
            HistoryPage(events=[]),
        ]
    )
    puller = _start_message_puller(config, adapter)

    assert puller.take_message() is None
    assert puller.take_message() is None
    assert adapter.calls[1]["page_token"] == "next"

    now["value"] = 2000.0
    adapter.generation = 1
    assert puller.take_message() is None
    assert adapter.calls[2]["start_ms"] == (
        1000000 - config.providers["lark"].sync.history_overlap_ms
    )
    assert adapter.calls[2]["end_ms"] == 2000000

    now["value"] += 86400
    assert puller.take_message() is None
    assert len(adapter.calls) == 3


def test_reconnect_handoff_precedes_buffered_live_message(monkeypatch):
    config = load_config("p2p_config.yaml")
    now = {"value": 1000.0}
    monkeypatch.setattr("openevent.im_p2p_syncer.provider_messages.time.time", lambda: now["value"])
    adapter = FakeSyncAdapter()
    puller = _start_message_puller(config, adapter)
    assert puller.take_message() is None

    history_event = _provider_event("om_reconnect_history", event_ms=2)
    live_event = _provider_event("om_reconnect_live", event_ms=3)
    adapter.pages.append(HistoryPage(events=[history_event]))
    adapter.events.append(live_event)
    adapter.generation = 1

    assert puller.take_message() == history_event
    puller.acknowledge(history_event)
    assert puller.take_message() == live_event


@pytest.mark.parametrize(
    ("highwater_ms", "expected_start_ms"),
    [
        (None, 700000),
        (800000, 500000),
    ],
)
def test_initial_handoff_window_uses_recovered_watermark_or_lookback(
    monkeypatch,
    highwater_ms,
    expected_start_ms,
):
    config = load_config("p2p_config.yaml")
    monkeypatch.setattr("openevent.im_p2p_syncer.provider_messages.time.time", lambda: 1000.0)
    adapter = FakeSyncAdapter()
    puller = _start_message_puller(
        config,
        adapter,
        {"oc_p2p_10001_bot": highwater_ms},
    )

    assert puller.take_message() is None
    assert adapter.calls[0]["start_ms"] == expected_start_ms
    assert adapter.calls[0]["end_ms"] == 1000000
