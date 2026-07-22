from __future__ import annotations

import threading

import lark_oapi as lark
import pytest

from openevent.im_p2p_syncer.errors import ProviderDataError
from openevent.im_p2p_syncer.lark_event_stream import LarkEventStream


class FakeDispatcherBuilder:
    def __init__(self):
        self.on_message = None

    def register_p2_im_message_receive_v1(self, callback):
        self.on_message = callback
        return self

    def build(self):
        return self


class FakeDispatcherHandler:
    last_builder = None

    @classmethod
    def builder(cls, encrypt_key, verification_token):
        cls.last_builder = FakeDispatcherBuilder()
        return cls.last_builder


class FakeWebSocketClient:
    def __init__(self, *args, **kwargs):
        self._conn = None
        self._stopped = threading.Event()
        self.on_reconnecting = None
        self.on_reconnected = None

    def start(self):
        self._conn = object()
        self._stopped.wait(2)

    async def _disconnect(self):
        self._conn = None
        self._stopped.set()


def test_lark_event_stream_starts_dispatches_and_stops(monkeypatch):
    monkeypatch.setattr(lark, "EventDispatcherHandler", FakeDispatcherHandler)
    monkeypatch.setattr(lark.ws, "Client", FakeWebSocketClient)
    received = []
    stream = LarkEventStream(
        app_id="cli_test",
        app_secret="secret",
        domain="https://open.larksuite.com",
        connect_timeout_seconds=1,
        reconnect_timeout_seconds=5,
        on_message=received.append,
    )

    stream.start()
    FakeDispatcherHandler.last_builder.on_message("event")
    stream.stop()

    assert received == ["event"]
    assert stream.error() is None


def test_lark_event_stream_reports_reconnect_timeout(monkeypatch):
    now = {"value": 10.0}
    monkeypatch.setattr(
        "openevent.im_p2p_syncer.lark_event_stream.time.monotonic",
        lambda: now["value"],
    )
    stream = LarkEventStream(
        app_id="cli_test",
        app_secret="secret",
        domain="https://open.larksuite.com",
        connect_timeout_seconds=1,
        reconnect_timeout_seconds=5,
        on_message=lambda event: None,
    )

    stream._on_reconnecting()
    now["value"] = 16.0

    assert "reconnect timeout" in str(stream.error())


def test_lark_event_stream_increments_generation_after_reconnect():
    stream = LarkEventStream(
        app_id="cli_test",
        app_secret="secret",
        domain="https://open.larksuite.com",
        connect_timeout_seconds=1,
        reconnect_timeout_seconds=5,
        on_message=lambda event: None,
    )

    assert stream.reconnect_generation() == 0
    stream._on_reconnecting()
    assert stream.reconnect_generation() == 0
    stream._on_reconnected()
    assert stream.reconnect_generation() == 1


@pytest.mark.parametrize(
    "error",
    [ProviderDataError("malformed mapped event"), RuntimeError("event queue is full")],
)
def test_lark_event_stream_reports_callback_error(error):
    stream = LarkEventStream(
        app_id="cli_test",
        app_secret="secret",
        domain="https://open.larksuite.com",
        connect_timeout_seconds=1,
        reconnect_timeout_seconds=5,
        on_message=lambda event: (_ for _ in ()).throw(error),
    )

    with pytest.raises(type(error)):
        stream._dispatch_message("event")

    assert stream.error() is error
