from __future__ import annotations

import json
import time
from typing import Any

from ..ids import stable_lark_openapi_uuid
from ..models import AdapterHealth, ProviderConfig, ProviderEvent, SendResult, SyncBatch


class LarkOpenAPIAdapter:
    def __init__(self, config: ProviderConfig):
        self._config = config
        self._client = self._build_client(config)

    def provider_name(self) -> str:
        return self._config.name

    def stop_sync(self) -> None:
        return None

    def health_check(self) -> AdapterHealth:
        return AdapterHealth(ok=True)

    def send_message(
        self,
        session_id: str,
        sender_external_user_id: str,
        msg_type: str,
        content: dict[str, object],
        request_id: str,
    ) -> SendResult:
        if msg_type != "text":
            return SendResult(
                success=False,
                retryable=True,
                error_code="UNSUPPORTED_MSG_TYPE",
                error_message=f"unsupported msg_type: {msg_type}",
            )
        try:
            from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody

            request = (
                CreateMessageRequest.builder()
                .receive_id_type("chat_id")
                .request_body(
                    CreateMessageRequestBody.builder()
                    .receive_id(session_id)
                    .msg_type(msg_type)
                    .content(json.dumps(content, ensure_ascii=False, separators=(",", ":")))
                    .uuid(stable_lark_openapi_uuid(request_id))
                    .build()
                )
                .build()
            )
            response = self._client.im.v1.message.create(request)
        except Exception as exc:
            return SendResult(
                success=False,
                retryable=True,
                error_code=type(exc).__name__,
                error_message=str(exc),
            )

        if not response.success():
            return SendResult(
                success=False,
                retryable=True,
                error_code=str(getattr(response, "code", "")),
                error_message=str(getattr(response, "msg", "")),
            )
        message_id = getattr(getattr(response, "data", None), "message_id", None)
        if not message_id:
            return SendResult(
                success=False,
                retryable=True,
                error_code="MISSING_MESSAGE_ID",
                error_message=f"{self._config.name} response missing message_id",
            )
        return SendResult(success=True, provider_message_id=str(message_id))

    def sync_once(self, session_id: str, cursor: object | None) -> SyncBatch:
        try:
            from lark_oapi.api.im.v1 import ListMessageRequest
        except Exception as exc:
            return SyncBatch(events=[], cursor={"error": str(exc)})

        now_ms = int(time.time() * 1000)
        if isinstance(cursor, dict) and isinstance(cursor.get("event_ms"), int):
            start_ms = cursor["event_ms"] + 1
        else:
            start_ms = max(0, now_ms - self._config.sync.startup_lookback_ms)
        end_ms = now_ms
        page_token = None
        events: list[ProviderEvent] = []

        while True:
            builder = (
                ListMessageRequest.builder()
                .container_id_type("chat")
                .container_id(session_id)
                .start_time(start_ms // 1000)
                .end_time(max(end_ms // 1000, start_ms // 1000 + 1))
                .sort_type("ByCreateTimeAsc")
                .page_size(self._config.sync.page_size)
            )
            if page_token:
                builder = builder.page_token(page_token)
            response = self._client.im.v1.message.list(builder.build())
            if not response.success():
                raise RuntimeError(f"{self._config.name} list messages failed: {response.code} {response.msg}")

            data = getattr(response, "data", None)
            for item in getattr(data, "items", []) or []:
                event = self._message_to_event(session_id, item)
                if event is not None:
                    events.append(event)

            if not getattr(data, "has_more", False):
                break
            page_token = getattr(data, "page_token", None)
            if not page_token:
                break

        next_cursor = {"event_ms": max([event.event_ms for event in events], default=end_ms)}
        return SyncBatch(events=events, cursor=next_cursor)

    def _build_client(self, config: ProviderConfig):
        try:
            import lark_oapi as lark
        except ImportError as exc:
            raise RuntimeError("lark-oapi is required for LarkOpenAPIAdapter") from exc

        builder = (
            lark.Client.builder()
            .app_id(config.credentials["app_id"])
            .app_secret(config.credentials["app_secret"])
        )
        api_base_url = config.options.get("api_base_url")
        if api_base_url:
            builder = builder.domain(api_base_url)
        return builder.build()

    def _message_to_event(self, session_id: str, item: Any) -> ProviderEvent | None:
        message_id = getattr(item, "message_id", None)
        msg_type = getattr(item, "msg_type", None)
        create_time = getattr(item, "create_time", None)
        sender = getattr(item, "sender", None)
        sender_external_user_id = getattr(sender, "id", None)
        sender_id_type = getattr(sender, "id_type", None)
        sender_identity_type = _normalize_lark_openapi_sender_type(getattr(sender, "sender_type", None))
        if not message_id or not msg_type or not create_time or not sender_external_user_id:
            return None
        if sender_identity_type == "user" and sender_id_type and sender_id_type != "open_id":
            return None

        try:
            event_ms = int(create_time)
        except (TypeError, ValueError):
            return None

        body = getattr(item, "body", None)
        raw_content = getattr(body, "content", None) if body is not None else None
        content_raw = self._parse_content_raw(raw_content)
        text = content_raw.get("text") if isinstance(content_raw.get("text"), str) else None
        return ProviderEvent(
            provider=self.provider_name(),
            session_id=session_id,
            provider_message_id=str(message_id),
            sender_external_user_id=str(sender_external_user_id),
            msg_type=str(msg_type),
            content_raw=content_raw,
            text=text,
            event_ms=event_ms,
            sender_identity_type=sender_identity_type,
        )

    def _parse_content_raw(self, raw_content: Any) -> dict[str, Any]:
        if isinstance(raw_content, dict):
            return raw_content
        if isinstance(raw_content, str):
            try:
                value = json.loads(raw_content)
            except json.JSONDecodeError:
                return {"content": raw_content}
            return value if isinstance(value, dict) else {"content": value}
        return {"content": raw_content}


def _normalize_lark_openapi_sender_type(value: Any) -> str:
    if not isinstance(value, str):
        return "user"
    normalized = value.lower()
    if normalized in {"app", "bot"}:
        return "bot"
    return "user"
