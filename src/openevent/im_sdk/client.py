from __future__ import annotations

from typing import Any

from grpc import RpcError, StatusCode

from .codec import decode_payload, encode_send_request, encode_send_result, encode_sync_record
from .errors import PublishFailedError
from .model import ParsedMessage, SendRequestInput, SendResultInput, SyncRecordInput
from .normalizer import normalize_recipients, require_uint64


_GUARANTEED_NOT_COMMITTED = {
    StatusCode.UNAUTHENTICATED,
    StatusCode.PERMISSION_DENIED,
    StatusCode.NOT_FOUND,
    StatusCode.INVALID_ARGUMENT,
    StatusCode.RESOURCE_EXHAUSTED,
}


class ImProtocolClient:
    def __init__(self, openevent_client: Any):
        self._openevent_client = openevent_client

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
        pre_publish_max_seq = self._get_pre_publish_max_seq(principal, token)
        try:
            response = self._openevent_client.publish_auto_seq(
                principal=principal,
                token=token,
                channel_id=channel_id,
                payload=payload,
                recipients=recipients,
            )
            if not hasattr(response, "seq"):
                raise RuntimeError("PublishAutoSeq response missing seq")
            return require_uint64(response.seq, "seq", positive=True)
        except Exception as exc:
            status_code = _rpc_status_code(exc)
            if status_code in _GUARANTEED_NOT_COMMITTED:
                raise PublishFailedError(str(exc)) from exc
            return self._reconcile_uncertain_publish(
                principal=principal,
                token=token,
                channel_id=channel_id,
                recipients=recipients,
                payload=payload,
                pre_publish_max_seq=pre_publish_max_seq,
                publish_error=exc,
            )

    def _get_pre_publish_max_seq(self, principal: int, token: str) -> int:
        try:
            response = self._openevent_client.get_status(principal, token)
            return require_uint64(response.max_seq, "max_seq")
        except Exception as exc:
            raise PublishFailedError(
                f"failed to read pre-publish max_seq: {exc}",
                retry_safe=True,
            ) from exc

    def _reconcile_uncertain_publish(
        self,
        *,
        principal: int,
        token: str,
        channel_id: int,
        recipients: list[int],
        payload: bytes,
        pre_publish_max_seq: int,
        publish_error: Exception,
    ) -> int:
        try:
            status = self._openevent_client.get_status(principal, token)
            reconcile_max_seq = require_uint64(status.max_seq, "max_seq")
            if reconcile_max_seq < pre_publish_max_seq:
                raise RuntimeError("GetStatus max_seq moved backwards during publish reconciliation")

            from_seq = pre_publish_max_seq + 1
            while from_seq <= reconcile_max_seq:
                response = self._openevent_client.fetch(
                    principal=principal,
                    token=token,
                    from_seq=from_seq,
                    limit=1000,
                    only_my_recipient=False,
                    channels=[channel_id],
                )
                for message in response.messages:
                    if int(message.seq) > reconcile_max_seq:
                        continue
                    if _matches_publish(message, principal, channel_id, recipients, payload):
                        return require_uint64(message.seq, "message.seq", positive=True)
                next_seq = require_uint64(response.next_seq, "next_seq", positive=True)
                if next_seq <= from_seq:
                    raise RuntimeError("Fetch did not advance during publish reconciliation")
                from_seq = next_seq
        except Exception as exc:
            raise PublishFailedError(
                f"PublishAutoSeq outcome is unknown and reconciliation failed: {exc}",
                outcome_unknown=True,
            ) from publish_error

        raise PublishFailedError(
            f"PublishAutoSeq failed but reconciliation proved it was not committed: {publish_error}",
            retry_safe=True,
        ) from publish_error


def create_client(openevent_client: Any) -> ImProtocolClient:
    return ImProtocolClient(openevent_client=openevent_client)


def _rpc_status_code(error: BaseException) -> StatusCode | None:
    if not isinstance(error, RpcError):
        return None
    try:
        return error.code()
    except Exception:
        return None


def _matches_publish(
    message: Any,
    principal: int,
    channel_id: int,
    recipients: list[int],
    payload: bytes,
) -> bool:
    return (
        int(message.principal) == principal
        and int(message.channel_id) == channel_id
        and list(message.recipients) == recipients
        and bytes(message.payload) == payload
    )
