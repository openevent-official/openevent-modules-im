from __future__ import annotations

import logging
import threading
import time
from collections import deque

from grpc import RpcError, StatusCode

from openevent.im_sdk import SendResultInput, SyncRecordInput
from openevent.im_sdk.errors import PublishFailedError

from .adapters.base import ProviderAdapter
from .mapping import P2PMappingIndex
from .models import ProviderEvent, RetryConfig, SendResult
from .provider_messages import ProviderMessagePuller
from .state import InboundKey, OutboundTask, RuntimeState


class FatalSyncerError(RuntimeError):
    pass


class SyncerStopped(RuntimeError):
    pass


class SingleThreadProcessor:
    def __init__(
        self,
        *,
        im_client,
        mapping: P2PMappingIndex,
        adapters: dict[str, ProviderAdapter],
        message_pullers: dict[str, ProviderMessagePuller] | None = None,
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
        self._message_pullers = message_pullers or {}
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
        self._work_turn = 0

    def tick(self) -> bool:
        if self._stop_event.is_set():
            return False
        self._collect_provider_messages()

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

    def _collect_provider_messages(self) -> None:
        if len(self._pending_events) >= self._pending_event_limit:
            return
        for provider, puller in self._message_pullers.items():
            if len(self._pending_events) >= self._pending_event_limit:
                return
            try:
                event = puller.take_message()
            except Exception as exc:
                raise FatalSyncerError(f"{provider} provider message pull failed") from exc
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
            puller = self._message_pullers.get(event.provider)
            if puller is None:
                raise FatalSyncerError(f"provider message puller missing: {event.provider}")
            puller.acknowledge(event)
            del self._pending_events[index]
            return True
        return False

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
        except KeyError as exc:
            raise FatalSyncerError(
                f"send.request principal has no bot mapping: "
                f"request_id={task.request_id} channel_id={task.channel_id} "
                f"principal={task.principal}"
            ) from exc

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
            raise FatalSyncerError(
                f"provider send raised: request_id={task.request_id} "
                f"channel_id={task.channel_id} error={type(exc).__name__}: {exc}"
            ) from exc
        if not result.success or not result.provider_message_id:
            error_code = result.error_code or "PROVIDER_SEND_FAILED"
            error_message = result.error_message or "Provider send failed without provider_message_id"
            self._logger.error(
                "provider_send_failed request_id=%s channel_id=%s error_code=%s error=%s",
                task.request_id,
                task.channel_id,
                error_code,
                error_message,
            )
            raise FatalSyncerError(
                f"provider send failed: request_id={task.request_id} "
                f"channel_id={task.channel_id} "
                f"error_code={error_code} error={error_message}"
            )

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
                content_raw={
                    "omitted": True,
                    "reason": "message_too_large",
                    "metadata": {
                        "provider": event.provider,
                        "session_id": event.session_id,
                        "sender_identity_type": event.sender_identity_type,
                        "sender_external_user_id": event.sender_external_user_id,
                        "msg_type": event.msg_type,
                    },
                },
                content_omitted=True,
                omit_reason="message_too_large",
                event_ms=event.event_ms,
                ingested_ms=int(time.time() * 1000),
                prev_seq=prev_seq,
            )
            try:
                self._publish_sync_record(principal, token, channel_id, recipients, fallback)
            except PublishFailedError as fallback_exc:
                if _is_resource_exhausted(fallback_exc):
                    raise FatalSyncerError(
                        "degraded sync.record still exceeds the OpenEvent payload limit"
                    ) from fallback_exc
                raise

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
                if (
                    not isinstance(exc, PublishFailedError)
                    or not exc.retry_safe
                    or not _is_transient_publish_error(exc)
                ):
                    raise FatalSyncerError(
                        "OpenEvent publish outcome is unknown or not retryable"
                    ) from exc
                if attempt == self._retry.publish_max_attempts:
                    break
                if self._stop_event.wait(self._retry.publish_retry_delay_ms / 1000):
                    raise SyncerStopped("syncer stopped during publish retry") from exc
        if last_error is not None:
            raise FatalSyncerError("OpenEvent publish failed after retries") from last_error
        raise FatalSyncerError("OpenEvent publish failed after retries")


def _is_resource_exhausted(error: BaseException) -> bool:
    return _rpc_status_code(error) == StatusCode.RESOURCE_EXHAUSTED


def _is_transient_publish_error(error: BaseException) -> bool:
    return _rpc_status_code(error) in {StatusCode.UNAVAILABLE, StatusCode.DEADLINE_EXCEEDED}


def _rpc_status_code(error: BaseException) -> StatusCode | None:
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, RpcError):
            try:
                return current.code()
            except Exception:
                return None
        current = current.__cause__
    return None
