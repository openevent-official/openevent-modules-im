from __future__ import annotations

import json
from typing import Any

from .errors import InvalidKindError, MalformedPayloadError
from .model import JsonObject, SendRequestInput, SendResultInput, SyncRecordInput
from .normalizer import (
    reject_source_principal,
    require_non_empty_str,
    require_object,
    require_timestamp_ms,
    require_uint64,
)

VALID_KINDS = {"sync.record", "send.request", "send.result"}


def encode_payload(payload: JsonObject) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def decode_payload(payload: bytes) -> JsonObject:
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MalformedPayloadError("payload must be UTF-8 JSON") from exc
    return parse_payload_object(value)


def parse_payload_object(value: Any) -> JsonObject:
    payload = require_object(value, "payload")
    reject_source_principal(payload)

    kind = payload.get("kind")
    if not isinstance(kind, str):
        raise MalformedPayloadError("kind must be a string")
    if kind not in VALID_KINDS:
        raise InvalidKindError(f"invalid kind: {kind}")

    data = require_object(payload.get("data"), "data")
    timestamps = require_object(payload.get("timestamps"), "timestamps")
    require_timestamp_ms(timestamps.get("event_ms"), "timestamps.event_ms")

    if kind == "sync.record":
        require_timestamp_ms(timestamps.get("ingested_ms"), "timestamps.ingested_ms")
    if "prev_seq" in payload:
        require_uint64(payload["prev_seq"], "prev_seq")
    if kind in {"send.request", "send.result"}:
        require_non_empty_str(payload.get("request_id"), "request_id")

    return payload


def encode_send_request(req: SendRequestInput) -> bytes:
    request_id = require_non_empty_str(req.request_id, "request_id")
    msg_type = require_non_empty_str(req.msg_type, "data.msg_type")
    content = require_object(req.content, "data.content")
    event_ms = require_timestamp_ms(req.event_ms, "timestamps.event_ms")

    payload: JsonObject = {
        "kind": "send.request",
        "request_id": request_id,
        "data": {"msg_type": msg_type, "content": content},
        "timestamps": {"event_ms": event_ms},
    }
    if req.prev_seq is not None:
        payload["prev_seq"] = require_uint64(req.prev_seq, "prev_seq")
    return encode_payload(payload)


def encode_send_result(req: SendResultInput) -> bytes:
    request_id = require_non_empty_str(req.request_id, "request_id")
    prev_seq = require_uint64(req.prev_seq, "prev_seq")
    status = require_non_empty_str(req.status, "data.status")
    if status not in {"SUCCESS", "FAILED"}:
        raise MalformedPayloadError("data.status must be SUCCESS or FAILED")
    event_ms = require_timestamp_ms(req.event_ms, "timestamps.event_ms")

    data: JsonObject = {"status": status}
    if req.provider_message_id is not None:
        data["provider_message_id"] = require_non_empty_str(
            req.provider_message_id, "data.provider_message_id"
        )
    if req.error_code is not None:
        data["error_code"] = require_non_empty_str(req.error_code, "data.error_code")
    if req.error_message is not None:
        data["error_message"] = require_non_empty_str(req.error_message, "data.error_message")

    return encode_payload(
        {
            "kind": "send.result",
            "prev_seq": prev_seq,
            "request_id": request_id,
            "data": data,
            "timestamps": {"event_ms": event_ms},
        }
    )


def encode_sync_record(req: SyncRecordInput) -> bytes:
    provider_message_id = require_non_empty_str(
        req.provider_message_id, "data.provider_message_id"
    )
    msg_type = require_non_empty_str(req.msg_type, "data.msg_type")
    content_raw = require_object(req.content_raw, "data.content_raw")
    event_ms = require_timestamp_ms(req.event_ms, "timestamps.event_ms")
    ingested_ms = require_timestamp_ms(req.ingested_ms, "timestamps.ingested_ms")

    data: JsonObject = dict(req.extra_data)
    data.update(
        {
            "provider_message_id": provider_message_id,
            "msg_type": msg_type,
            "content_raw": content_raw,
        }
    )
    if req.text is not None:
        data["text"] = req.text
    if req.is_init is not None:
        data["is_init"] = bool(req.is_init)
    if req.content_omitted is not None:
        data["content_omitted"] = bool(req.content_omitted)
    if req.omit_reason is not None:
        data["omit_reason"] = require_non_empty_str(req.omit_reason, "data.omit_reason")
    if req.text_preview is not None:
        data["text_preview"] = req.text_preview

    payload: JsonObject = {
        "kind": "sync.record",
        "data": data,
        "timestamps": {"event_ms": event_ms, "ingested_ms": ingested_ms},
    }
    if req.prev_seq is not None:
        payload["prev_seq"] = require_uint64(req.prev_seq, "prev_seq")
    return encode_payload(payload)


def build_message_too_large_content_raw(
    *,
    original_size_bytes: int | None = None,
    metadata: JsonObject | None = None,
) -> JsonObject:
    content_raw: JsonObject = {"omitted": True, "reason": "message_too_large"}
    if original_size_bytes is not None:
        if isinstance(original_size_bytes, bool) or original_size_bytes <= 0:
            raise MalformedPayloadError("original_size_bytes must be greater than 0")
        content_raw["original_size_bytes"] = original_size_bytes
    content_raw["metadata"] = require_object(metadata or {}, "metadata")
    return content_raw
