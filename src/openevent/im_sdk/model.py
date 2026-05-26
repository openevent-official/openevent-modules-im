from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

JsonObject = dict[str, Any]
UInt64 = int
TimestampMs = int
DurationMs = int


@dataclass(frozen=True)
class SendRequestInput:
    request_id: str
    msg_type: str
    content: JsonObject
    event_ms: TimestampMs
    prev_seq: UInt64 | None = None


@dataclass(frozen=True)
class SendResultInput:
    request_id: str
    prev_seq: UInt64
    status: str
    event_ms: TimestampMs
    provider_message_id: str | None = None
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class SyncRecordInput:
    provider_message_id: str
    msg_type: str
    content_raw: JsonObject
    event_ms: TimestampMs
    ingested_ms: TimestampMs
    prev_seq: UInt64 | None = None
    text: str | None = None
    is_init: bool | None = None
    content_omitted: bool | None = None
    omit_reason: str | None = None
    text_preview: str | None = None
    extra_data: JsonObject = field(default_factory=dict)


@dataclass(frozen=True)
class ParsedMessage:
    seq: UInt64
    channel_id: UInt64
    principal: UInt64
    recipients: list[UInt64]
    kind: str
    payload: JsonObject
    data: JsonObject
    event_ms: TimestampMs
    ingested_ms: TimestampMs | None = None
    request_id: str | None = None
    prev_seq: UInt64 | None = None
