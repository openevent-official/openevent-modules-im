from __future__ import annotations

import json
import os
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from openevent.im_p2p_syncer import syncer as syncer_module
from openevent.im_p2p_syncer.config import parse_config
from openevent.im_p2p_syncer.models import AdapterHealth, ProviderEvent, SendResult, SyncBatch
from openevent.im_p2p_syncer.syncer import P2PSyncer
from openevent.im_sdk import SendRequestInput, create_client as create_im_client
from openevent.sdk import AdminClient, OpenEventClient
from openevent.sdk.proto import openevent_pb2


pytestmark = pytest.mark.skipif(
    os.environ.get("OPENEVENT_IM_E2E") != "1",
    reason="set OPENEVENT_IM_E2E=1 and run test-e2e.sh or make e2e",
)


class MockIMState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.pending_events: list[dict[str, Any]] = []
        self.sent_requests: list[dict[str, Any]] = []
        self.next_sent_id = 1

    def enqueue_user_message(self, text: str, provider_message_id: str = "mock-in-1") -> None:
        with self.lock:
            self.pending_events.append(
                {
                    "provider": "mock_im",
                    "session_id": "session-1",
                    "provider_message_id": provider_message_id,
                    "sender_external_user_id": "user-ext",
                    "sender_identity_type": "user",
                    "msg_type": "text",
                    "content_raw": {"text": text},
                    "text": text,
                    "event_ms": int(time.time() * 1000),
                }
            )

    def take_events(self) -> list[dict[str, Any]]:
        with self.lock:
            events = list(self.pending_events)
            self.pending_events.clear()
            return events

    def record_send(self, payload: dict[str, Any]) -> str:
        with self.lock:
            provider_message_id = f"mock-sent-{self.next_sent_id}"
            self.next_sent_id += 1
            self.sent_requests.append(payload)
            text = payload.get("content", {}).get("text", "")
            self.pending_events.append(
                {
                    "provider": "mock_im",
                    "session_id": payload["session_id"],
                    "provider_message_id": provider_message_id,
                    "sender_external_user_id": payload["sender_external_user_id"],
                    "sender_identity_type": "bot",
                    "msg_type": payload["msg_type"],
                    "content_raw": {"text": text},
                    "text": text,
                    "event_ms": int(time.time() * 1000),
                }
            )
            return provider_message_id


class MockIMHandler(BaseHTTPRequestHandler):
    state: MockIMState

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/sync":
            self.send_error(404)
            return
        self._write_json({"events": self.state.take_events()})

    def do_POST(self) -> None:
        if self.path != "/send":
            self.send_error(404)
            return
        length = int(self.headers.get("content-length", "0"))
        payload = json.loads(self.rfile.read(length).decode("utf-8"))
        provider_message_id = self.state.record_send(payload)
        self._write_json({"provider_message_id": provider_message_id})

    def log_message(self, format: str, *args: object) -> None:
        return None

    def _write_json(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class MockHTTPIMAdapter:
    def __init__(self, config, base_url: str):
        self._config = config
        self._base_url = base_url.rstrip("/")

    def provider_name(self) -> str:
        return self._config.name

    def stop_sync(self) -> None:
        return None

    def health_check(self) -> AdapterHealth:
        return AdapterHealth(ok=True)

    def sync_once(self, session_id: str, cursor: object | None) -> SyncBatch:
        with urllib.request.urlopen(f"{self._base_url}/sync?session_id={urllib.parse.quote(session_id)}") as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        events = [ProviderEvent(**item) for item in payload["events"]]
        return SyncBatch(events=events, cursor={"seen": time.time_ns()})

    def send_message(
        self,
        session_id: str,
        sender_external_user_id: str,
        msg_type: str,
        content: dict[str, object],
        request_id: str,
    ) -> SendResult:
        payload = json.dumps(
            {
                "session_id": session_id,
                "sender_external_user_id": sender_external_user_id,
                "msg_type": msg_type,
                "content": content,
                "request_id": request_id,
            }
        ).encode("utf-8")
        request = urllib.request.Request(
            f"{self._base_url}/send",
            data=payload,
            method="POST",
            headers={"content-type": "application/json"},
        )
        with urllib.request.urlopen(request) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return SendResult(success=True, provider_message_id=data["provider_message_id"])


@pytest.fixture
def mock_im_server():
    state = MockIMState()
    MockIMHandler.state = state
    server = ThreadingHTTPServer(("127.0.0.1", 0), MockIMHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield state, f"http://{host}:{port}"
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def _token(admin: AdminClient, principal: int) -> str:
    return admin.add_token(principal).binding.token


def _wait_until(predicate, timeout_s: float = 5.0):
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(0.05)
    raise AssertionError("condition not reached before timeout")


def _fetch_parsed(im_client, client, principal: int, token: str, from_seq: int = 1):
    response = client.fetch(principal=principal, token=token, from_seq=from_seq, limit=1000)
    parsed = []
    for message in response.messages:
        try:
            parsed.append(im_client.parse_message(message))
        except Exception:
            continue
    return parsed


def test_p2p_syncer_round_trips_with_mock_im_service(monkeypatch, mock_im_server) -> None:
    state, base_url = mock_im_server
    admin = AdminClient(os.environ["OPENEVENT_IM_E2E_ADMIN_TARGET"])
    client = OpenEventClient(os.environ["OPENEVENT_IM_E2E_TARGET"])
    im_client = create_im_client(client)

    worker_principal = 91001
    user_principal = 91002
    bot_principal = 91003
    worker_token = _token(admin, worker_principal)
    user_token = _token(admin, user_principal)
    bot_token = _token(admin, bot_principal)

    channel = client.create_channel(
        principal=worker_principal,
        token=worker_token,
        name=f"im-e2e-{time.time_ns()}",
        visibility=openevent_pb2.VISIBILITY_PRIVATE,
        protocol="im.v1",
        description=json.dumps(
            {
                "version": "v1",
                "provider": "mock_im",
                "session_id": "session-1",
                "session_type": "p2p",
                "updated_at_ms": int(time.time() * 1000),
                "metadata": {},
            },
            separators=(",", ":"),
        ),
        members=[user_principal, bot_principal],
    ).channel

    raw_config = {
        "version": "v1",
        "worker": {
            "name": "mock-im-syncer",
            "principal": worker_principal,
            "token": worker_token,
            "request_result_timeout_ms": 60000,
            "shutdown_timeout_ms": 1000,
        },
        "openevent": {"target": os.environ["OPENEVENT_IM_E2E_TARGET"]},
        "retry": {
            "publish_max_attempts": 2,
            "publish_initial_backoff_ms": 1,
            "publish_max_backoff_ms": 5,
            "provider_send_max_attempts": 2,
            "idle_sleep_ms": 20,
        },
        "principal_tokens": [
            {"principal": user_principal, "token": user_token},
            {"principal": bot_principal, "token": bot_token},
        ],
        "providers": [
            {
                "name": "mock_im",
                "adapter": "mock_http",
                "enabled": True,
                "sync": {"mode": "poll", "interval_ms": 20, "page_size": 10, "startup_lookback_ms": 0},
                "credentials": {"mock": "ok"},
                "options": {"base_url": base_url},
            }
        ],
        "mappings": [
            {
                "provider": "mock_im",
                "identity_type": "user",
                "external_user_id": "user-ext",
                "principal": user_principal,
                "session_id": "session-1",
                "channel_id": channel.channel_id,
                "status": "active",
            },
            {
                "provider": "mock_im",
                "identity_type": "bot",
                "external_user_id": "bot-ext",
                "principal": bot_principal,
                "session_id": "session-1",
                "channel_id": channel.channel_id,
                "status": "active",
            },
        ],
    }
    config = parse_config(raw_config)

    monkeypatch.setattr(
        syncer_module,
        "create_adapter",
        lambda provider_config: MockHTTPIMAdapter(provider_config, provider_config.options["base_url"]),
    )
    syncer = P2PSyncer(config, client)
    thread = threading.Thread(target=syncer.start, daemon=True)

    state.enqueue_user_message("hello from mock im")
    thread.start()
    try:
        sync_record = _wait_until(
            lambda: next(
                (
                    item
                    for item in _fetch_parsed(im_client, client, bot_principal, bot_token)
                    if item.kind == "sync.record" and item.data.get("provider_message_id") == "mock-in-1"
                ),
                None,
            )
        )
        assert sync_record.principal == user_principal
        assert sync_record.recipients == [bot_principal]
        assert sync_record.data["text"] == "hello from mock im"

        send_request_seq = im_client.publish_send_request(
            principal=bot_principal,
            token=bot_token,
            channel_id=channel.channel_id,
            req=SendRequestInput(
                request_id="send-1",
                msg_type="text",
                content={"text": "hello back"},
                event_ms=int(time.time() * 1000),
            ),
        )
        send_result = _wait_until(
            lambda: next(
                (
                    item
                    for item in _fetch_parsed(im_client, client, bot_principal, bot_token, from_seq=send_request_seq)
                    if item.kind == "send.result" and item.request_id == "send-1"
                ),
                None,
            )
        )
        assert send_result.principal == worker_principal
        assert send_result.prev_seq == send_request_seq
        assert send_result.data["status"] == "SUCCESS"
        assert send_result.data["provider_message_id"].startswith("mock-sent-")

        bot_echo = _wait_until(
            lambda: next(
                (
                    item
                    for item in _fetch_parsed(im_client, client, user_principal, user_token, from_seq=send_result.seq)
                    if item.kind == "sync.record"
                    and item.data.get("provider_message_id") == send_result.data["provider_message_id"]
                ),
                None,
            )
        )
        assert bot_echo.principal == bot_principal
        assert bot_echo.recipients == [user_principal]
        assert bot_echo.prev_seq == send_result.seq
        assert bot_echo.data["text"] == "hello back"

        assert state.sent_requests[0]["content"] == {"text": "hello back"}
    finally:
        syncer.stop()
        thread.join(timeout=2)
