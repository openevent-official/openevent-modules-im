from __future__ import annotations

import heapq
import time
from dataclasses import dataclass

from openevent.im_sdk import ParsedMessage

from .errors import StateConflictError


InboundKey = tuple[str, str, int, str]


@dataclass(frozen=True)
class OutboundTask:
    seq: int
    channel_id: int
    principal: int
    request_id: str
    msg_type: str
    content: dict[str, object]


@dataclass(frozen=True)
class TerminalResult:
    seq: int
    channel_id: int
    principal: int
    recipients: tuple[int, ...]
    request_id: str
    prev_seq: int
    status: str
    provider_message_id: str | None


class RuntimeState:
    def __init__(self):
        self.inbound_seen: set[InboundKey] = set()
        self.completed_request_ids: set[str] = set()
        self.canonical_requests_by_id: dict[str, OutboundTask] = {}
        self.terminal_results_by_id: dict[str, TerminalResult] = {}
        self.requests_by_id: dict[str, OutboundTask] = {}
        self.pending_heap: list[tuple[int, str]] = []
        self.blocked_by_channel: dict[int, str] = {}
        self.provider_send_attempts: dict[str, int] = {}
        self.provider_retry_ready_at: dict[str, float] = {}
        self.send_result_by_provider_message: dict[InboundKey, int] = {}
        self.latest_event_ms_by_channel: dict[int, int] = {}

    def add_send_request(self, parsed: ParsedMessage) -> None:
        task = OutboundTask(
            seq=parsed.seq,
            channel_id=parsed.channel_id,
            principal=parsed.principal,
            request_id=parsed.request_id,
            msg_type=parsed.data["msg_type"],
            content=parsed.data["content"],
        )
        existing = self.canonical_requests_by_id.get(task.request_id)
        if existing is not None:
            if not _same_send_request(existing, task):
                raise StateConflictError(
                    f"conflicting send.request messages for request_id {task.request_id}"
                )
            return
        self.canonical_requests_by_id[task.request_id] = task
        if task.request_id in self.completed_request_ids:
            return
        self.requests_by_id[task.request_id] = task
        heapq.heappush(self.pending_heap, (task.seq, task.request_id))

    def next_pending_task(self) -> OutboundTask | None:
        deferred: list[tuple[int, str]] = []
        while self.pending_heap:
            seq, request_id = heapq.heappop(self.pending_heap)
            task = self.requests_by_id.get(request_id)
            if task is None or task.seq != seq or request_id in self.completed_request_ids:
                continue
            if time.monotonic() < self.provider_retry_ready_at.get(request_id, 0):
                deferred.append((seq, request_id))
                continue
            blocked_request_id = self.blocked_by_channel.get(task.channel_id)
            if blocked_request_id is not None and blocked_request_id != request_id:
                deferred.append((seq, request_id))
                continue
            self.blocked_by_channel[task.channel_id] = request_id
            for item in deferred:
                heapq.heappush(self.pending_heap, item)
            return task
        for item in deferred:
            heapq.heappush(self.pending_heap, item)
        return None

    def retry_send_request(self, task: OutboundTask, delay_ms: int) -> None:
        if task.request_id in self.requests_by_id and task.request_id not in self.completed_request_ids:
            self.provider_retry_ready_at[task.request_id] = time.monotonic() + delay_ms / 1000
            heapq.heappush(self.pending_heap, (task.seq, task.request_id))

    def channel_has_pending(self, channel_id: int) -> bool:
        return any(task.channel_id == channel_id for task in self.requests_by_id.values())

    def mark_send_result_success(
        self,
        task: OutboundTask,
        provider: str,
        session_id: str,
        provider_message_id: str,
        seq: int,
    ) -> None:
        self.completed_request_ids.add(task.request_id)
        self.requests_by_id.pop(task.request_id, None)
        self._unblock_channel(task.channel_id, task.request_id)
        self.provider_send_attempts.pop(task.request_id, None)
        self.provider_retry_ready_at.pop(task.request_id, None)
        self.send_result_by_provider_message[
            (provider, session_id, task.channel_id, provider_message_id)
        ] = seq

    def mark_send_result_failed(self, task: OutboundTask) -> None:
        self.completed_request_ids.add(task.request_id)
        self.requests_by_id.pop(task.request_id, None)
        self._unblock_channel(task.channel_id, task.request_id)
        self.provider_send_attempts.pop(task.request_id, None)
        self.provider_retry_ready_at.pop(task.request_id, None)

    def add_send_result(self, parsed: ParsedMessage, provider: str, session_id: str) -> None:
        result = TerminalResult(
            seq=parsed.seq,
            channel_id=parsed.channel_id,
            principal=parsed.principal,
            recipients=tuple(parsed.recipients),
            request_id=parsed.request_id,
            prev_seq=parsed.prev_seq,
            status=parsed.data["status"],
            provider_message_id=parsed.data.get("provider_message_id"),
        )
        existing_result = self.terminal_results_by_id.get(parsed.request_id)
        if existing_result is not None:
            if not _same_send_result(existing_result, result):
                raise StateConflictError(
                    f"conflicting send.result messages for request_id {parsed.request_id}"
                )
            return
        task = self.canonical_requests_by_id.get(parsed.request_id)
        if task is None:
            raise StateConflictError(
                f"send.result has no preceding send.request: {parsed.request_id}"
            )
        if parsed.prev_seq != task.seq:
            raise StateConflictError(
                f"send.result prev_seq does not match send.request: {parsed.request_id}"
            )
        if parsed.channel_id != task.channel_id:
            raise StateConflictError(
                f"send.result channel does not match send.request: {parsed.request_id}"
            )
        if parsed.recipients != [task.principal]:
            raise StateConflictError(
                f"send.result recipients do not match send.request: {parsed.request_id}"
            )
        self.completed_request_ids.add(parsed.request_id)
        self.terminal_results_by_id[parsed.request_id] = result
        self.requests_by_id.pop(parsed.request_id, None)
        self._unblock_channel(task.channel_id, task.request_id)
        self.provider_send_attempts.pop(parsed.request_id, None)
        self.provider_retry_ready_at.pop(parsed.request_id, None)
        if parsed.data.get("status") != "SUCCESS":
            return
        provider_message_id = parsed.data.get("provider_message_id")
        if isinstance(provider_message_id, str) and provider_message_id:
            self.send_result_by_provider_message[
                (provider, session_id, parsed.channel_id, provider_message_id)
            ] = parsed.seq

    def add_sync_record(self, parsed: ParsedMessage, provider: str, session_id: str) -> None:
        provider_message_id = parsed.data["provider_message_id"]
        key = (provider, session_id, parsed.channel_id, provider_message_id)
        expected_prev_seq = self.send_result_by_provider_message.get(key)
        if expected_prev_seq is not None and parsed.prev_seq != expected_prev_seq:
            raise StateConflictError(
                f"sync.record prev_seq does not match send.result: {provider_message_id}"
            )
        if expected_prev_seq is None and parsed.prev_seq is not None:
            raise StateConflictError(
                f"sync.record prev_seq has no matching send.result: {provider_message_id}"
            )
        self.inbound_seen.add(key)
        self._record_event_ms(parsed.channel_id, parsed.event_ms)

    def mark_sync_record(
        self,
        key: InboundKey,
        channel_id: int,
        event_ms: int,
    ) -> None:
        self.inbound_seen.add(key)
        self._record_event_ms(channel_id, event_ms)

    def latest_event_ms(self, channel_id: int) -> int | None:
        return self.latest_event_ms_by_channel.get(channel_id)

    def record_provider_send_attempt(self, request_id: str) -> int:
        attempts = self.provider_send_attempts.get(request_id, 0) + 1
        self.provider_send_attempts[request_id] = attempts
        return attempts

    def _unblock_channel(self, channel_id: int, request_id: str) -> None:
        if self.blocked_by_channel.get(channel_id) == request_id:
            self.blocked_by_channel.pop(channel_id, None)

    def _record_event_ms(self, channel_id: int, event_ms: int) -> None:
        current = self.latest_event_ms_by_channel.get(channel_id)
        if current is None or event_ms > current:
            self.latest_event_ms_by_channel[channel_id] = event_ms


def _same_send_request(left: OutboundTask, right: OutboundTask) -> bool:
    return (
        left.channel_id == right.channel_id
        and left.principal == right.principal
        and left.request_id == right.request_id
        and left.msg_type == right.msg_type
        and left.content == right.content
    )


def _same_send_result(left: TerminalResult, right: TerminalResult) -> bool:
    return (
        left.channel_id == right.channel_id
        and left.principal == right.principal
        and left.recipients == right.recipients
        and left.request_id == right.request_id
        and left.prev_seq == right.prev_seq
        and left.status == right.status
        and left.provider_message_id == right.provider_message_id
    )
