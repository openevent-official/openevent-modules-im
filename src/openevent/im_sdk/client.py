from __future__ import annotations

from typing import Any

from .codec import decode_payload, encode_send_request, encode_send_result, encode_sync_record
from .errors import PublishFailedError
from .model import ParsedMessage, SendRequestInput, SendResultInput, SyncRecordInput
from .normalizer import normalize_recipients, require_duration_ms, require_uint64


class ImProtocolClient:
    def __init__(self, openevent_client: Any, request_result_timeout_ms: int = 60000):
        self._openevent_client = openevent_client
        self.request_result_timeout_ms = require_duration_ms(
            request_result_timeout_ms, "request_result_timeout_ms"
        )

    def publish_send_request(
        self,
        principal: int,
        token: str,
        channel_id: int,
        req: SendRequestInput,
        recipients: list[int] | None = None,
    ) -> int:
        return self._publish(
            principal=principal,
            token=token,
            channel_id=channel_id,
            payload=encode_send_request(req),
            recipients=normalize_recipients(recipients),
        )

    def publish_send_result(
        self,
        principal: int,
        token: str,
        channel_id: int,
        recipients: list[int],
        req: SendResultInput,
    ) -> int:
        return self._publish(
            principal=principal,
            token=token,
            channel_id=channel_id,
            payload=encode_send_result(req),
            recipients=normalize_recipients(recipients),
        )

    def publish_sync_record(
        self,
        principal: int,
        token: str,
        channel_id: int,
        recipients: list[int],
        req: SyncRecordInput,
    ) -> int:
        return self._publish(
            principal=principal,
            token=token,
            channel_id=channel_id,
            payload=encode_sync_record(req),
            recipients=normalize_recipients(recipients, sort_unique=True),
        )

    def parse_payload(self, payload: bytes) -> dict[str, Any]:
        return decode_payload(payload)

    def parse_message(self, message: Any) -> ParsedMessage:
        payload = self.parse_payload(message.payload)
        timestamps = payload["timestamps"]
        return ParsedMessage(
            seq=require_uint64(message.seq, "message.seq"),
            channel_id=require_uint64(message.channel_id, "message.channel_id", positive=True),
            principal=require_uint64(message.principal, "message.principal", positive=True),
            recipients=normalize_recipients(getattr(message, "recipients", [])),
            kind=payload["kind"],
            payload=payload,
            data=payload["data"],
            event_ms=timestamps["event_ms"],
            ingested_ms=timestamps.get("ingested_ms"),
            request_id=payload.get("request_id"),
            prev_seq=payload.get("prev_seq"),
        )

    def _publish(
        self,
        *,
        principal: int,
        token: str,
        channel_id: int,
        payload: bytes,
        recipients: list[int],
    ) -> int:
        principal = require_uint64(principal, "principal", positive=True)
        channel_id = require_uint64(channel_id, "channel_id", positive=True)
        try:
            response = self._openevent_client.publish_auto_seq(
                principal=principal,
                token=token,
                channel_id=channel_id,
                payload=payload,
                recipients=recipients,
            )
        except Exception as exc:  # pragma: no cover - exact gRPC errors belong to openevent-sdk
            raise PublishFailedError(str(exc)) from exc
        if not hasattr(response, "seq"):
            raise PublishFailedError("PublishAutoSeq response missing seq")
        return require_uint64(response.seq, "seq", positive=True)


def create_client(openevent_client: Any, request_result_timeout_ms: int = 60000) -> ImProtocolClient:
    return ImProtocolClient(
        openevent_client=openevent_client,
        request_result_timeout_ms=request_result_timeout_ms,
    )
