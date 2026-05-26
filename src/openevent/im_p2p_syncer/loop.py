from __future__ import annotations

import logging
import time

from grpc import RpcError, StatusCode

from openevent.im_sdk import SendResultInput, SyncRecordInput
from openevent.im_sdk.codec import build_message_too_large_content_raw
from openevent.im_sdk.errors import PublishFailedError

from .adapters.base import ProviderAdapter
from .mapping import P2PMappingIndex
from .models import ProviderEvent, RetryConfig, SendResult
from .state import InboundKey, OutboundTask, RuntimeState


class FatalSyncerError(RuntimeError):
    pass


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
    ):
        self._im_client = im_client
        self._mapping = mapping
        self._adapters = adapters
        self._retry = retry
        self._worker_principal = worker_principal
        self._worker_token = worker_token
        self._state = state
        self._logger = logger
        self._cursors: dict[int, object | None] = {channel_id: None for channel_id in mapping.channel_ids}
        self._next_poll_at_ms: dict[int, int] = {channel_id: 0 for channel_id in mapping.channel_ids}

    def enqueue_send_request(self, task: OutboundTask) -> None:
        self._state.retry_send_request(task)

    def tick(self) -> None:
        task = self._next_pending_task()
        if task is not None:
            self._process_outbound_task(task)
            return
        for channel_id in self._mapping.channel_ids:
            if self._channel_has_pending(channel_id):
                continue
            if not self._poll_due(channel_id):
                continue
            self._poll_channel(channel_id)

    def _poll_due(self, channel_id: int) -> bool:
        now_ms = int(time.time() * 1000)
        if now_ms < self._next_poll_at_ms[channel_id]:
            return False
        provider_config = self._mapping.provider_config_for_channel(channel_id)
        self._next_poll_at_ms[channel_id] = now_ms + provider_config.sync.interval_ms
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
                retryable=True,
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
        if attempts < self._retry.provider_send_max_attempts:
            self._state.retry_send_request(task)
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
                    error_message="send.request principal is not active in this P2P channel",
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

    def _poll_channel(self, channel_id: int) -> None:
        provider, session_id = self._mapping.provider_session(channel_id)
        adapter = self._adapters[provider]
        try:
            batch = adapter.sync_once(session_id, self._cursors[channel_id])
        except Exception as exc:
            self._logger.error(
                "provider_sync_failed provider=%s session_id=%s error_code=%s error=%s",
                provider,
                session_id,
                type(exc).__name__,
                exc,
            )
            return

        for event in batch.events:
            if self._channel_has_pending(channel_id):
                return
            if not self._publish_provider_event(channel_id, event):
                return
        self._cursors[channel_id] = batch.cursor

    def _publish_provider_event(self, channel_id: int, event: ProviderEvent) -> bool:
        key: InboundKey = (
            event.provider,
            event.session_id,
            channel_id,
            event.provider_message_id,
        )
        if key in self._state.inbound_seen:
            return True

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
            return False

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

        self._state.inbound_seen.add(key)
        return True

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
                if attempt == self._retry.publish_max_attempts:
                    break
                backoff_ms = min(
                    self._retry.publish_initial_backoff_ms * (2 ** (attempt - 1)),
                    self._retry.publish_max_backoff_ms,
                )
                time.sleep(backoff_ms / 1000)
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
