from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass

from grpc import RpcError, StatusCode

from openevent.im_sdk import SendResultInput, SyncRecordInput
from openevent.im_sdk.codec import build_message_too_large_content_raw
from openevent.im_sdk.errors import PublishFailedError

from .adapters.base import ProviderAdapter
from .errors import PermanentProviderError, ProviderDataError, TransientProviderError
from .mapping import P2PMappingIndex
from .models import ProviderEvent, RetryConfig, SendResult
from .state import InboundKey, OutboundTask, RuntimeState


class FatalSyncerError(RuntimeError):
    pass


class SyncerStopped(RuntimeError):
    pass


@dataclass
class HistoryScan:
    start_ms: int
    end_ms: int
    page_token: str | None = None


class SingleThreadProcessor:
    def __init__(
        self,
        *,
        im_client,
        mapping: P2PMappingIndex,
        adapters: dict[str, ProviderAdapter],
        retry: RetryConfig,
        worker_principal: int,
        worker_token: str,
        state: RuntimeState,
        logger: logging.Logger,
        stop_event: threading.Event | None = None,
    ):
        self._im_client = im_client
        self._mapping = mapping
        self._adapters = adapters
        self._retry = retry
        self._worker_principal = worker_principal
        self._worker_token = worker_token
        self._state = state
        self._logger = logger
        self._stop_event = stop_event or threading.Event()
        self._pending_events: deque[ProviderEvent] = deque()
        provider_queue_sizes = {
            provider: mapping.provider_config_for_channel(channel_id).sync.event_queue_size
            for channel_id in mapping.channel_ids
            for provider, _ in [mapping.provider_session(channel_id)]
        }
        self._pending_event_limit = sum(provider_queue_sizes.values())
        self._history_highwater_ms: dict[int, int | None] = {
            channel_id: None for channel_id in mapping.channel_ids
        }
        self._history_scans: dict[int, HistoryScan | None] = {
            channel_id: None for channel_id in mapping.channel_ids
        }
        self._next_history_at_ms: dict[int, int] = {
            channel_id: 0 for channel_id in mapping.channel_ids
        }
        self._stream_generations: dict[str, int] = {}
        self._history_initialized = False
        self._history_channel_cursor = 0
        self._work_turn = 0

    def initialize_history_repair(self, anchor_ms: int) -> None:
        if self._history_initialized:
            return
        end_ms = int(time.time() * 1000)
        for channel_id in self._mapping.channel_ids:
            config = self._mapping.provider_config_for_channel(channel_id).sync
            recovered_ms = self._state.latest_event_ms(channel_id)
            self._history_highwater_ms[channel_id] = recovered_ms
            start_ms = (
                max(0, anchor_ms - config.history_lookback_ms)
                if recovered_ms is None
                else max(0, min(recovered_ms, anchor_ms) - config.history_overlap_ms)
            )
            self._history_scans[channel_id] = HistoryScan(
                start_ms=start_ms,
                end_ms=end_ms,
            )
            self._next_history_at_ms[channel_id] = 0
        self._stream_generations = {
            provider: adapter.event_stream_generation()
            for provider, adapter in self._adapters.items()
        }
        self._history_initialized = True

    def history_repair_pending(self) -> bool:
        return any(scan is not None for scan in self._history_scans.values())

    def tick(self) -> bool:
        if self._stop_event.is_set():
            return False
        if not self._history_initialized:
            self.initialize_history_repair(int(time.time() * 1000))
        self._raise_event_stream_error()
        self._schedule_reconnect_history_repair()
        self._collect_provider_events()
        self._schedule_reconnect_history_repair()

        if self.history_repair_pending():
            return self._process_due_history_page()

        work = (
            self._process_next_outbound_task,
            self._process_next_provider_event,
        )
        for offset in range(len(work)):
            index = (self._work_turn + offset) % len(work)
            if work[index]():
                self._work_turn = (index + 1) % len(work)
                return True
        return False

    def _process_next_outbound_task(self) -> bool:
        task = self._next_pending_task()
        if task is None:
            return False
        self._process_outbound_task(task)
        return True

    def _process_due_history_page(self) -> bool:
        channel_ids = self._mapping.channel_ids
        for offset in range(len(channel_ids)):
            index = (self._history_channel_cursor + offset) % len(channel_ids)
            channel_id = channel_ids[index]
            if self._stop_event.is_set():
                return False
            if not self._history_due(channel_id):
                continue
            self._repair_history_page(channel_id)
            self._history_channel_cursor = (index + 1) % len(channel_ids)
            return True
        return False

    def _schedule_reconnect_history_repair(self) -> None:
        now_ms = int(time.time() * 1000)
        for provider, adapter in self._adapters.items():
            generation = adapter.event_stream_generation()
            previous = self._stream_generations.get(provider)
            if previous is None:
                self._stream_generations[provider] = generation
                continue
            if generation < previous:
                raise FatalSyncerError(f"{provider} event stream generation moved backwards")
            if generation == previous:
                continue
            self._stream_generations[provider] = generation
            self._logger.info(
                "provider_history_repair_started provider=%s reconnect_generation=%s",
                provider,
                generation,
            )
            for channel_id in self._mapping.channel_ids:
                mapped_provider, _ = self._mapping.provider_session(channel_id)
                if mapped_provider != provider:
                    continue
                config = self._mapping.provider_config_for_channel(channel_id).sync
                confirmed_values = (
                    self._state.latest_event_ms(channel_id),
                    self._history_highwater_ms[channel_id],
                )
                confirmed_ms = max(
                    (value for value in confirmed_values if value is not None),
                    default=None,
                )
                start_ms = (
                    max(0, now_ms - config.history_lookback_ms)
                    if confirmed_ms is None
                    else max(0, min(confirmed_ms, now_ms) - config.history_overlap_ms)
                )
                current_scan = self._history_scans[channel_id]
                if current_scan is not None:
                    start_ms = min(start_ms, current_scan.start_ms)
                self._history_scans[channel_id] = HistoryScan(
                    start_ms=start_ms,
                    end_ms=now_ms,
                )
                self._next_history_at_ms[channel_id] = 0

    def _raise_event_stream_error(self) -> None:
        for provider, adapter in self._adapters.items():
            error = adapter.event_stream_error()
            if error is not None:
                raise FatalSyncerError(f"{provider} event stream failed") from error

    def _collect_provider_events(self) -> None:
        if len(self._pending_events) >= self._pending_event_limit:
            return
        for adapter in self._adapters.values():
            if len(self._pending_events) >= self._pending_event_limit:
                return
            event = adapter.take_event()
            if event is not None:
                self._pending_events.append(event)

    def _process_next_provider_event(self) -> bool:
        if not self._pending_events:
            return False
        for index, event in enumerate(self._pending_events):
            try:
                channel_id = self._mapping.channel_for_provider_session(
                    event.provider,
                    event.session_id,
                )
            except KeyError:
                self._logger.error(
                    "provider_session_missing provider=%s session_id=%s provider_message_id=%s",
                    event.provider,
                    event.session_id,
                    event.provider_message_id,
                )
                raise FatalSyncerError(
                    f"provider event session is not mapped: {event.provider}/{event.session_id}"
                )
            if self._channel_has_pending(channel_id):
                continue
            self._publish_provider_event(channel_id, event)
            del self._pending_events[index]
            return True
        return False

    def _history_due(self, channel_id: int) -> bool:
        if self._history_scans[channel_id] is None:
            return False
        now_ms = int(time.time() * 1000)
        if now_ms < self._next_history_at_ms[channel_id]:
            return False
        return True

    def _next_pending_task(self) -> OutboundTask | None:
        return self._state.next_pending_task()

    def _channel_has_pending(self, channel_id: int) -> bool:
        return self._state.channel_has_pending(channel_id)

    def _process_outbound_task(self, task: OutboundTask) -> None:
        provider, session_id = self._mapping.provider_session(task.channel_id)
        try:
            sender_external_user_id = self._mapping.sender_external_user_id(
                task.channel_id, task.principal, "bot"
            )
        except KeyError:
            self._logger.error(
                "mapping_missing request_id=%s channel_id=%s principal=%s",
                task.request_id,
                task.channel_id,
                task.principal,
            )
            self._publish_mapping_missing_result(task)
            return

        adapter = self._adapters[provider]
        try:
            result = adapter.send_message(
                session_id=session_id,
                sender_external_user_id=sender_external_user_id,
                msg_type=task.msg_type,
                content=task.content,
                request_id=task.request_id,
            )
        except Exception as exc:
            result = SendResult(
                success=False,
                error_code=type(exc).__name__,
                error_message=str(exc),
            )
        if not result.success or not result.provider_message_id:
            self._handle_provider_send_failure(task, result)
            return

        event_ms = int(time.time() * 1000)
        seq = self._publish_with_retry(
            lambda: self._im_client.publish_send_result(
                principal=self._worker_principal,
                token=self._worker_token,
                channel_id=task.channel_id,
                recipients=[task.principal],
                req=SendResultInput(
                    request_id=task.request_id,
                    prev_seq=task.seq,
                    status="SUCCESS",
                    provider_message_id=result.provider_message_id,
                    event_ms=event_ms,
                ),
            )
        )
        self._state.mark_send_result_success(task, provider, session_id, result.provider_message_id, seq)
        self._logger.info(
            "send_result_published request_id=%s openevent_seq=%s provider_message_id=%s",
            task.request_id,
            seq,
            result.provider_message_id,
        )

    def _handle_provider_send_failure(self, task: OutboundTask, result: SendResult) -> None:
        attempts = self._state.record_provider_send_attempt(task.request_id)
        self._log_send_failure(task, result, attempts)
        if result.retryable and attempts < self._retry.provider_send_max_attempts:
            self._state.retry_send_request(task, self._retry.provider_send_retry_delay_ms)
            return

        event_ms = int(time.time() * 1000)
        error_code = result.error_code or "PROVIDER_SEND_FAILED"
        error_message = result.error_message or "Provider send failed without provider_message_id"
        seq = self._publish_with_retry(
            lambda: self._im_client.publish_send_result(
                principal=self._worker_principal,
                token=self._worker_token,
                channel_id=task.channel_id,
                recipients=[task.principal],
                req=SendResultInput(
                    request_id=task.request_id,
                    prev_seq=task.seq,
                    status="FAILED",
                    error_code="PROVIDER_SEND_FAILED",
                    error_message=f"{error_code}: {error_message}",
                    event_ms=event_ms,
                ),
            )
        )
        self._state.mark_send_result_failed(task)
        self._logger.warning(
            "send_result_failed_published request_id=%s openevent_seq=%s error_code=%s attempts=%s",
            task.request_id,
            seq,
            "PROVIDER_SEND_FAILED",
            attempts,
        )

    def _publish_mapping_missing_result(self, task: OutboundTask) -> None:
        event_ms = int(time.time() * 1000)
        seq = self._publish_with_retry(
            lambda: self._im_client.publish_send_result(
                principal=self._worker_principal,
                token=self._worker_token,
                channel_id=task.channel_id,
                recipients=[task.principal],
                req=SendResultInput(
                    request_id=task.request_id,
                    prev_seq=task.seq,
                    status="FAILED",
                    error_code="MAPPING_MISSING",
                    error_message="send.request principal has no bot mapping in this P2P channel",
                    event_ms=event_ms,
                ),
            )
        )
        self._state.mark_send_result_failed(task)
        self._logger.warning(
            "send_result_failed_published request_id=%s openevent_seq=%s error_code=%s",
            task.request_id,
            seq,
            "MAPPING_MISSING",
        )

    def _repair_history_page(self, channel_id: int) -> None:
        provider, session_id = self._mapping.provider_session(channel_id)
        adapter = self._adapters[provider]
        config = self._mapping.provider_config_for_channel(channel_id).sync
        scan = self._history_scans[channel_id]
        if scan is None:
            raise RuntimeError("history repair is not scheduled")
        try:
            page = adapter.fetch_history_page(
                session_id=session_id,
                start_ms=scan.start_ms,
                end_ms=scan.end_ms,
                page_token=scan.page_token,
            )
        except TransientProviderError as exc:
            self._logger.warning(
                "provider_history_temporarily_failed provider=%s session_id=%s error_code=%s error=%s",
                provider,
                session_id,
                type(exc).__name__,
                exc,
            )
            self._next_history_at_ms[channel_id] = (
                int(time.time() * 1000) + config.history_retry_delay_ms
            )
            return
        except (PermanentProviderError, ProviderDataError) as exc:
            raise FatalSyncerError(
                f"provider history failed for {provider}/{session_id}"
            ) from exc
        except Exception as exc:
            raise FatalSyncerError(
                f"unclassified provider history failure for {provider}/{session_id}"
            ) from exc

        for event in page.events:
            if self._stop_event.is_set():
                return
            self._publish_provider_event(channel_id, event)
        if page.next_page_token is not None:
            scan.page_token = page.next_page_token
            self._next_history_at_ms[channel_id] = 0
            return
        self._history_highwater_ms[channel_id] = scan.end_ms
        self._history_scans[channel_id] = None
        self._next_history_at_ms[channel_id] = 0

    def _publish_provider_event(self, channel_id: int, event: ProviderEvent) -> None:
        key: InboundKey = (
            event.provider,
            event.session_id,
            channel_id,
            event.provider_message_id,
        )
        if key in self._state.inbound_seen:
            return

        try:
            principal = self._mapping.principal_for_external_user(
                channel_id,
                event.provider,
                event.sender_external_user_id,
                event.sender_identity_type,
            )
        except KeyError:
            self._logger.error(
                "mapping_missing provider=%s session_id=%s channel_id=%s identity_type=%s external_user_id=%s",
                event.provider,
                event.session_id,
                channel_id,
                event.sender_identity_type,
                event.sender_external_user_id,
            )
            raise FatalSyncerError(
                f"provider sender mapping missing for {event.provider}/{event.session_id}"
            )

        token = self._mapping.user_token(principal)
        recipients = self._mapping.peer_principals(channel_id, principal)
        prev_seq = self._state.send_result_by_provider_message.get(key)
        req = SyncRecordInput(
            provider_message_id=event.provider_message_id,
            msg_type=event.msg_type,
            content_raw=event.content_raw,
            text=event.text,
            event_ms=event.event_ms,
            ingested_ms=int(time.time() * 1000),
            prev_seq=prev_seq,
        )

        try:
            self._publish_sync_record(principal, token, channel_id, recipients, req)
        except PublishFailedError as exc:
            if not _is_resource_exhausted(exc):
                raise
            fallback = SyncRecordInput(
                provider_message_id=event.provider_message_id,
                msg_type=event.msg_type,
                content_raw=build_message_too_large_content_raw(
                    metadata={
                        "provider": event.provider,
                        "session_id": event.session_id,
                        "sender_identity_type": event.sender_identity_type,
                        "sender_external_user_id": event.sender_external_user_id,
                        "msg_type": event.msg_type,
                    }
                ),
                content_omitted=True,
                omit_reason="message_too_large",
                event_ms=event.event_ms,
                ingested_ms=int(time.time() * 1000),
                prev_seq=prev_seq,
            )
            self._publish_sync_record(principal, token, channel_id, recipients, fallback)

        self._state.mark_sync_record(key, channel_id, event.event_ms)

    def _publish_sync_record(
        self,
        principal: int,
        token: str,
        channel_id: int,
        recipients: list[int],
        req: SyncRecordInput,
    ) -> int:
        return self._publish_with_retry(
            lambda: self._im_client.publish_sync_record(
                principal=principal,
                token=token,
                channel_id=channel_id,
                recipients=recipients,
                req=req,
            )
        )

    def _publish_with_retry(self, publish):
        last_error: Exception | None = None
        for attempt in range(1, self._retry.publish_max_attempts + 1):
            try:
                return publish()
            except Exception as exc:
                last_error = exc
                if _is_resource_exhausted(exc):
                    raise
                if not isinstance(exc, PublishFailedError) or not exc.retry_safe:
                    raise FatalSyncerError("OpenEvent publish outcome is unknown or not retryable") from exc
                if attempt == self._retry.publish_max_attempts:
                    break
                backoff_ms = min(
                    self._retry.publish_initial_backoff_ms * (2 ** (attempt - 1)),
                    self._retry.publish_max_backoff_ms,
                )
                if self._stop_event.wait(backoff_ms / 1000):
                    raise SyncerStopped("syncer stopped during publish retry") from exc
        if last_error is not None:
            raise FatalSyncerError("OpenEvent publish failed after retries") from last_error
        raise FatalSyncerError("OpenEvent publish failed after retries")

    def _log_send_failure(self, task: OutboundTask, result: SendResult, attempts: int) -> None:
        self._logger.warning(
            "provider_send_failed request_id=%s channel_id=%s attempts=%s max_attempts=%s error_code=%s error=%s",
            task.request_id,
            task.channel_id,
            attempts,
            self._retry.provider_send_max_attempts,
            result.error_code,
            result.error_message,
        )


def _is_resource_exhausted(error: BaseException) -> bool:
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, RpcError):
            try:
                return current.code() == StatusCode.RESOURCE_EXHAUSTED
            except Exception:
                return False
        current = current.__cause__
    return False
