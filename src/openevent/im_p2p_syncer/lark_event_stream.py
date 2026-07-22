from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from typing import Any

class LarkEventStream:
    """Blocking Lark WebSocket transport with a synchronous event callback."""

    def __init__(
        self,
        *,
        app_id: str,
        app_secret: str,
        domain: str,
        connect_timeout_seconds: float,
        reconnect_timeout_seconds: float,
        on_message: Callable[[Any], None],
    ) -> None:
        self._app_id = app_id
        self._app_secret = app_secret
        self._domain = domain
        self._connect_timeout_seconds = connect_timeout_seconds
        self._reconnect_timeout_seconds = reconnect_timeout_seconds
        self._on_message = on_message
        self._client: Any = None
        self._thread: threading.Thread | None = None
        self._stopping = threading.Event()
        self._error: BaseException | None = None
        self._disconnected_at: float | None = None
        self._reconnect_generation = 0
        self._state_lock = threading.Lock()

    def start(self) -> None:
        if self._thread is not None:
            return
        try:
            import lark_oapi as lark
        except ImportError as exc:
            raise RuntimeError("lark-oapi is required for the Lark event stream") from exc

        handler = (
            lark.EventDispatcherHandler.builder("", "")
            .register_p2_im_message_receive_v1(self._dispatch_message)
            .build()
        )
        self._client = lark.ws.Client(
            self._app_id,
            self._app_secret,
            event_handler=handler,
            domain=self._domain,
            auto_reconnect=True,
        )
        self._client.on_reconnecting = self._on_reconnecting
        self._client.on_reconnected = self._on_reconnected
        self._thread = threading.Thread(
            target=self._run,
            name="lark-message-events",
            daemon=True,
        )
        self._thread.start()

        deadline = time.monotonic() + self._connect_timeout_seconds
        while time.monotonic() < deadline:
            if self._error is not None:
                self.stop()
                raise RuntimeError(f"Lark event stream failed to start: {self._error}") from self._error
            if getattr(self._client, "_conn", None) is not None:
                return
            time.sleep(0.05)
        self.stop()
        raise RuntimeError("Lark event stream did not become ready before the connect timeout")

    def stop(self) -> None:
        self._stopping.set()
        client = self._client
        if client is not None:
            # lark-oapi's WebSocket client has no public stop method. Keep the
            # version-specific compatibility code isolated in this transport.
            setattr(client, "_auto_reconnect", False)
            disconnect = getattr(client, "_disconnect", None)
            try:
                from lark_oapi.ws import client as ws_client_module

                loop = getattr(ws_client_module, "loop", None)
                if callable(disconnect) and loop is not None:
                    self._disconnect(loop, disconnect)
                if loop is not None and loop.is_running():
                    loop.call_soon_threadsafe(loop.stop)
            except Exception:
                pass
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=5)

    def error(self) -> BaseException | None:
        if self._error is not None:
            return self._error
        with self._state_lock:
            disconnected_at = self._disconnected_at
        if (
            disconnected_at is not None
            and time.monotonic() - disconnected_at >= self._reconnect_timeout_seconds
        ):
            return RuntimeError("Lark event stream reconnect timeout")
        return None

    def reconnect_generation(self) -> int:
        with self._state_lock:
            return self._reconnect_generation

    def _on_reconnecting(self) -> None:
        with self._state_lock:
            if self._disconnected_at is None:
                self._disconnected_at = time.monotonic()

    def _on_reconnected(self) -> None:
        with self._state_lock:
            self._disconnected_at = None
            self._reconnect_generation += 1

    def _dispatch_message(self, data: Any) -> None:
        try:
            self._on_message(data)
        except Exception as exc:
            self._error = exc
            raise

    def _run(self) -> None:
        try:
            self._client.start()
            if not self._stopping.is_set():
                self._error = RuntimeError("Lark event stream exited unexpectedly")
        except BaseException as exc:
            if not self._stopping.is_set():
                self._error = exc

    @staticmethod
    def _disconnect(loop: asyncio.AbstractEventLoop, disconnect: Callable[[], Any]) -> None:
        if loop.is_running():
            future = asyncio.run_coroutine_threadsafe(disconnect(), loop)
            try:
                future.result(timeout=2)
            except Exception:
                pass
        elif not loop.is_closed():
            loop.run_until_complete(disconnect())
