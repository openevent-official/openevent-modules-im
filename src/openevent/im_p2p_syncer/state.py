from __future__ import annotations

import heapq
from dataclasses import dataclass

from openevent.im_sdk import ParsedMessage


InboundKey = tuple[str, str, int, str]


@dataclass(frozen=True)
class OutboundTask:
    seq: int
    channel_id: int
    principal: int
    request_id: str
    msg_type: str
    content: dict[str, object]


class RuntimeState:
    def __init__(self):
        self.inbound_seen: set[InboundKey] = set()
        self.completed_request_ids: set[str] = set()
        self.requests_by_id: dict[str, OutboundTask] = {}
        self.pending_heap: list[tuple[int, str]] = []
        self.blocked_by_channel: dict[int, str] = {}
        self.provider_send_attempts: dict[str, int] = {}
        self.send_result_by_provider_message: dict[InboundKey, int] = {}

    @property
    def result_written(self) -> set[str]:
        return self.completed_request_ids

    @property
    def pending_outbound(self) -> dict[str, OutboundTask]:
        return self.requests_by_id

    def add_send_request(self, parsed: ParsedMessage) -> None:
        if not parsed.request_id or parsed.request_id in self.completed_request_ids:
            return
        data = parsed.data
        msg_type = data.get("msg_type")
        content = data.get("content")
        if not isinstance(msg_type, str) or not isinstance(content, dict):
            return
        task = OutboundTask(
            seq=parsed.seq,
            channel_id=parsed.channel_id,
            principal=parsed.principal,
            request_id=parsed.request_id,
            msg_type=msg_type,
            content=content,
        )
        existing = self.requests_by_id.get(task.request_id)
        if existing is not None and existing.seq <= task.seq:
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

    def retry_send_request(self, task: OutboundTask) -> None:
        if task.request_id in self.requests_by_id and task.request_id not in self.completed_request_ids:
            heapq.heappush(self.pending_heap, (task.seq, task.request_id))

    def channel_has_pending(self, channel_id: int) -> bool:
        request_id = self.blocked_by_channel.get(channel_id)
        return request_id is not None and request_id in self.requests_by_id

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
        self.send_result_by_provider_message[
            (provider, session_id, task.channel_id, provider_message_id)
        ] = seq

    def mark_send_result_failed(self, task: OutboundTask) -> None:
        self.completed_request_ids.add(task.request_id)
        self.requests_by_id.pop(task.request_id, None)
        self._unblock_channel(task.channel_id, task.request_id)
        self.provider_send_attempts.pop(task.request_id, None)

    def add_send_result(self, parsed: ParsedMessage, provider: str, session_id: str) -> None:
        if not parsed.request_id or parsed.data.get("status") not in {"SUCCESS", "FAILED"}:
            return
        task = self.requests_by_id.get(parsed.request_id)
        if task is not None and parsed.prev_seq != task.seq:
            return
        self.completed_request_ids.add(parsed.request_id)
        if task is not None:
            self.requests_by_id.pop(parsed.request_id, None)
            self._unblock_channel(task.channel_id, task.request_id)
        self.provider_send_attempts.pop(parsed.request_id, None)
        if parsed.data.get("status") != "SUCCESS":
            return
        provider_message_id = parsed.data.get("provider_message_id")
        if isinstance(provider_message_id, str) and provider_message_id:
            self.send_result_by_provider_message[
                (provider, session_id, parsed.channel_id, provider_message_id)
            ] = parsed.seq

    def add_sync_record(self, parsed: ParsedMessage, provider: str, session_id: str) -> None:
        provider_message_id = parsed.data.get("provider_message_id")
        if isinstance(provider_message_id, str) and provider_message_id:
            self.inbound_seen.add((provider, session_id, parsed.channel_id, provider_message_id))

    def record_provider_send_attempt(self, request_id: str) -> int:
        attempts = self.provider_send_attempts.get(request_id, 0) + 1
        self.provider_send_attempts[request_id] = attempts
        return attempts

    def _unblock_channel(self, channel_id: int, request_id: str) -> None:
        if self.blocked_by_channel.get(channel_id) == request_id:
            self.blocked_by_channel.pop(channel_id, None)
