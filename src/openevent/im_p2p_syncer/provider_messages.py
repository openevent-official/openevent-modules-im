from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field

from .adapters.base import ProviderAdapter
from .errors import TransientProviderError
from .models import ProviderEvent, ProviderSyncConfig


@dataclass
class HistoryScan:
    start_ms: int
    end_ms: int
    page_token: str | None = None


@dataclass
class SessionPullState:
    confirmed_event_ms: int | None
    history_highwater_ms: int | None
    history_scan: HistoryScan | None
    history_events: deque[ProviderEvent] = field(default_factory=deque)
    in_flight: ProviderEvent | None = None
    next_history_at_ms: int = 0


class ProviderMessagePuller:
    """Presents provider history and subscription events as one acknowledged stream."""

    def __init__(
        self,
        *,
        adapter: ProviderAdapter,
        config: ProviderSyncConfig,
        logger: logging.Logger,
    ) -> None:
        self._adapter = adapter
        self._config = config
        self._logger = logger
        self._provider = adapter.provider_name()
        self._sessions: dict[str, SessionPullState] = {}
        self._session_order: tuple[str, ...] = ()
        self._session_cursor = 0
        self._live_events: deque[ProviderEvent] = deque()
        self._stream_generation = 0
        self._started = False

    def start(self, session_highwater_ms: dict[str, int | None]) -> None:
        if self._started:
            return
        if not session_highwater_ms:
            raise ValueError(f"{self._provider} message puller requires at least one session")
        self._adapter.start_event_stream(set(session_highwater_ms))
        now_ms = int(time.time() * 1000)
        self._sessions = {
            session_id: SessionPullState(
                confirmed_event_ms=confirmed_ms,
                history_highwater_ms=confirmed_ms,
                history_scan=self._new_history_scan(confirmed_ms, now_ms),
            )
            for session_id, confirmed_ms in session_highwater_ms.items()
        }
        self._session_order = tuple(sorted(self._sessions))
        self._stream_generation = self._adapter.event_stream_generation()
        self._started = True

    def stop(self) -> None:
        self._adapter.stop_event_stream()
        self._started = False

    def take_message(self) -> ProviderEvent | None:
        if not self._started:
            raise RuntimeError(f"{self._provider} message puller is not started")
        self._raise_stream_error()
        self._schedule_reconnect_handoff()
        self._collect_live_event()
        self._raise_stream_error()
        self._schedule_reconnect_handoff()

        now_ms = int(time.time() * 1000)
        for offset in range(len(self._session_order)):
            index = (self._session_cursor + offset) % len(self._session_order)
            session_id = self._session_order[index]
            state = self._sessions[session_id]
            if state.in_flight is not None:
                continue

            if state.history_events:
                return self._deliver(index, state, state.history_events.popleft())

            if state.history_scan is not None and now_ms >= state.next_history_at_ms:
                self._fetch_history_page(session_id, state, now_ms)
                self._session_cursor = (index + 1) % len(self._session_order)
                if state.history_events:
                    return self._deliver(index, state, state.history_events.popleft())

            if state.history_scan is None:
                live_event = self._take_live_event(session_id)
                if live_event is not None:
                    return self._deliver(index, state, live_event)
        return None

    def acknowledge(self, event: ProviderEvent) -> None:
        if event.provider != self._provider:
            raise RuntimeError("provider message acknowledgement has the wrong provider")
        state = self._sessions.get(event.session_id)
        if state is None:
            raise RuntimeError("provider message acknowledgement has an unknown session")
        in_flight = state.in_flight
        if in_flight is None or _event_key(in_flight) != _event_key(event):
            raise RuntimeError(
                "provider message acknowledgement does not match the in-flight message"
            )
        if state.confirmed_event_ms is None or event.event_ms > state.confirmed_event_ms:
            state.confirmed_event_ms = event.event_ms
        state.in_flight = None

    def _deliver(
        self,
        index: int,
        state: SessionPullState,
        event: ProviderEvent,
    ) -> ProviderEvent:
        state.in_flight = event
        self._session_cursor = (index + 1) % len(self._session_order)
        return event

    def _collect_live_event(self) -> None:
        if len(self._live_events) >= self._config.event_queue_size:
            return
        event = self._adapter.take_event()
        if event is None:
            return
        if event.provider != self._provider:
            raise RuntimeError(
                f"{self._provider} subscription returned another provider's event"
            )
        if event.session_id not in self._sessions:
            raise RuntimeError(f"{self._provider} subscription returned an unknown session")
        self._live_events.append(event)

    def _take_live_event(self, session_id: str) -> ProviderEvent | None:
        for index, event in enumerate(self._live_events):
            if event.session_id == session_id:
                del self._live_events[index]
                return event
        return None

    def _fetch_history_page(
        self,
        session_id: str,
        state: SessionPullState,
        now_ms: int,
    ) -> None:
        scan = state.history_scan
        if scan is None:
            return
        try:
            page = self._adapter.fetch_history_page(
                session_id=session_id,
                start_ms=scan.start_ms,
                end_ms=scan.end_ms,
                page_token=scan.page_token,
            )
        except TransientProviderError as exc:
            self._logger.warning(
                "provider_message_history_temporarily_failed "
                "provider=%s session_id=%s error_code=%s error=%s",
                self._provider,
                session_id,
                type(exc).__name__,
                exc,
            )
            state.next_history_at_ms = now_ms + self._config.history_retry_delay_ms
            return

        for event in page.events:
            if event.provider != self._provider or event.session_id != session_id:
                raise RuntimeError("provider history returned an event outside its query session")
            state.history_events.append(event)
        state.next_history_at_ms = 0
        if page.next_page_token is None:
            state.history_highwater_ms = max(
                state.history_highwater_ms or 0,
                scan.end_ms,
            )
            state.history_scan = None
        else:
            scan.page_token = page.next_page_token

    def _schedule_reconnect_handoff(self) -> None:
        generation = self._adapter.event_stream_generation()
        if generation < self._stream_generation:
            raise RuntimeError(f"{self._provider} event stream generation moved backwards")
        if generation == self._stream_generation:
            return
        self._stream_generation = generation
        now_ms = int(time.time() * 1000)
        self._logger.info(
            "provider_message_handoff_started provider=%s reconnect_generation=%s",
            self._provider,
            generation,
        )
        for state in self._sessions.values():
            confirmed_values = (state.confirmed_event_ms, state.history_highwater_ms)
            confirmed_ms = max(
                (value for value in confirmed_values if value is not None),
                default=None,
            )
            replacement = self._new_history_scan(confirmed_ms, now_ms)
            if state.history_scan is not None:
                replacement.start_ms = min(replacement.start_ms, state.history_scan.start_ms)
            state.history_scan = replacement
            state.next_history_at_ms = 0

    def _new_history_scan(self, confirmed_ms: int | None, end_ms: int) -> HistoryScan:
        start_ms = (
            max(0, end_ms - self._config.history_lookback_ms)
            if confirmed_ms is None
            else max(0, min(confirmed_ms, end_ms) - self._config.history_overlap_ms)
        )
        return HistoryScan(start_ms=start_ms, end_ms=end_ms)

    def _raise_stream_error(self) -> None:
        error = self._adapter.event_stream_error()
        if error is not None:
            raise RuntimeError(f"{self._provider} event stream failed") from error


def _event_key(event: ProviderEvent) -> tuple[str, str, str]:
    return event.provider, event.session_id, event.provider_message_id
