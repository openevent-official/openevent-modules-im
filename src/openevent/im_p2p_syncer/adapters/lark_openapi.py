from __future__ import annotations

import json
import logging
import queue
import time
from typing import Any

from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout

from ..ids import stable_lark_openapi_uuid
from ..lark_event_stream import LarkEventStream
from ..errors import PermanentProviderError, ProviderDataError, TransientProviderError
from ..models import HistoryPage, ProviderConfig, ProviderEvent, SendResult


_LOGGER = logging.getLogger(__name__)
_LARK_RETRYABLE_CODES = {99991402, 11020, 11021}


class LarkOpenAPIAdapter:
    def __init__(self, config: ProviderConfig):
        self._config = config
        self._client = self._build_client(config)
        self._events: queue.Queue[ProviderEvent] = queue.Queue(
            maxsize=config.sync.event_queue_size
        )
        self._allowed_sessions: set[str] = set()
        self._event_stream: LarkEventStream | None = None

    def provider_name(self) -> str:
        return self._config.name

    def start_event_stream(self, session_ids: set[str]) -> None:
        if self._event_stream is not None:
            return
        self._allowed_sessions = set(session_ids)
        domain = str(
            self._config.options.get(
                "api_base_url",
                "https://open.feishu.cn" if self._config.name == "feishu" else "https://open.larksuite.com",
            )
        )
        self._event_stream = LarkEventStream(
            app_id=self._config.credentials["app_id"],
            app_secret=self._config.credentials["app_secret"],
            domain=domain,
            connect_timeout_seconds=float(
                self._config.options.get("event_connect_timeout_seconds", 30.0)
            ),
            reconnect_timeout_seconds=float(
                self._config.options.get("event_reconnect_timeout_seconds", 300.0)
            ),
            on_message=self._handle_message_event,
        )
        self._event_stream.start()

    def stop_event_stream(self) -> None:
        if self._event_stream is not None:
            self._event_stream.stop()
            self._event_stream = None

    def event_stream_error(self) -> BaseException | None:
        if self._event_stream is None:
            return None
        return self._event_stream.error()

    def event_stream_generation(self) -> int:
        if self._event_stream is None:
            return 0
        return self._event_stream.reconnect_generation()

    def take_event(self) -> ProviderEvent | None:
        try:
            return self._events.get_nowait()
        except queue.Empty:
            return None

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
                error_code="UNSUPPORTED_MSG_TYPE",
                error_message=f"unsupported msg_type: {msg_type}",
            )
        try:
            from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody

            content_json = json.dumps(content, ensure_ascii=False, separators=(",", ":"))
            request = (
                CreateMessageRequest.builder()
                .receive_id_type("chat_id")
                .request_body(
                    CreateMessageRequestBody.builder()
                    .receive_id(session_id)
                    .msg_type(msg_type)
                    .content(content_json)
                    .uuid(stable_lark_openapi_uuid(request_id))
                    .build()
                )
                .build()
            )
            response = self._client.im.v1.message.create(request)
        except Exception as exc:
            _LOGGER.exception(
                "lark_message_create_exception request_id=%s session_id=%s error_code=%s",
                request_id,
                session_id,
                type(exc).__name__,
            )
            return SendResult(
                success=False,
                error_code=type(exc).__name__,
                error_message=str(exc),
            )

        _log_lark_response(
            "lark_message_create_response",
            response,
            request_id=request_id,
            session_id=session_id,
        )
        if not response.success():
            return SendResult(
                success=False,
                error_code=str(getattr(response, "code", "")),
                error_message=str(getattr(response, "msg", "")),
            )
        message_id = getattr(getattr(response, "data", None), "message_id", None)
        if not message_id:
            return SendResult(
                success=False,
                error_code="MISSING_MESSAGE_ID",
                error_message=f"{self._config.name} response missing message_id",
            )
        return SendResult(success=True, provider_message_id=str(message_id))

    def fetch_history_page(
        self,
        session_id: str,
        start_ms: int,
        end_ms: int,
        page_token: str | None,
    ) -> HistoryPage:
        try:
            from lark_oapi.api.im.v1 import ListMessageRequest
        except Exception as exc:
            raise RuntimeError("lark-oapi is required for Lark history repair") from exc

        builder = (
            ListMessageRequest.builder()
            .container_id_type("chat")
            .container_id(session_id)
            .start_time(start_ms // 1000)
            .end_time(max((end_ms + 999) // 1000, start_ms // 1000 + 1))
            .sort_type("ByCreateTimeAsc")
            .page_size(self._config.sync.page_size)
        )
        if page_token:
            builder = builder.page_token(page_token)
        try:
            response = self._client.im.v1.message.list(builder.build())
        except (RequestsConnectionError, RequestsTimeout) as exc:
            raise TransientProviderError(
                f"{self._config.name} list messages temporarily failed"
            ) from exc
        if not response.success():
            error = f"{self._config.name} list messages failed: {response.code} {response.msg}"
            if _is_retryable_lark_response(response):
                raise TransientProviderError(error)
            raise PermanentProviderError(error)

        data = getattr(response, "data", None)
        if data is None:
            raise ProviderDataError(f"{self._config.name} list messages response missing data")
        events = []
        for item in getattr(data, "items", []) or []:
            sender = getattr(item, "sender", None)
            sender_type = getattr(sender, "sender_type", None)
            sender_identity_type = _normalize_lark_openapi_sender_type(sender_type)
            if sender_identity_type is None:
                raise ProviderDataError(
                    f"{self._config.name} list messages returned a malformed sender"
                )
            if getattr(item, "chat_id", None) != session_id:
                raise ProviderDataError(
                    f"{self._config.name} list messages returned an item for another chat"
                )
            event = self._message_to_event(session_id, item)
            if event is None:
                raise ProviderDataError(
                    f"{self._config.name} list messages returned a malformed item"
                )
            events.append(event)

        next_page_token = None
        if getattr(data, "has_more", False):
            next_page_token = getattr(data, "page_token", None)
            if not next_page_token:
                raise ProviderDataError(
                    f"{self._config.name} list messages returned has_more without page_token"
                )
        return HistoryPage(events=events, next_page_token=next_page_token)

    def _handle_message_event(self, data: Any) -> None:
        event_data = getattr(data, "event", None)
        message = getattr(event_data, "message", None)
        sender = getattr(event_data, "sender", None)
        session_id = getattr(message, "chat_id", None)
        if not session_id:
            raise ProviderDataError(
                f"{self._config.name} received a message event without chat_id"
            )
        if session_id not in self._allowed_sessions:
            return
        provider_event = self._event_message_to_event(str(session_id), message, sender)
        if provider_event is None:
            raise ProviderDataError(f"{self._config.name} received a malformed message event")
        try:
            self._events.put_nowait(provider_event)
        except queue.Full as exc:
            raise RuntimeError(f"{self._config.name} message event queue is full") from exc

    def _event_message_to_event(
        self,
        session_id: str,
        message: Any,
        sender: Any,
    ) -> ProviderEvent | None:
        sender_id = getattr(sender, "sender_id", None)
        sender_identity_type = _normalize_lark_openapi_sender_type(
            getattr(sender, "sender_type", None)
        )
        if sender_identity_type is None:
            return None
        sender_external_user_id = (
            getattr(sender_id, "open_id", None)
            if sender_identity_type == "user"
            else self._config.credentials["app_id"]
        )
        return self._build_provider_event(
            session_id=session_id,
            message_id=getattr(message, "message_id", None),
            msg_type=getattr(message, "message_type", None),
            create_time=getattr(message, "create_time", None),
            sender_external_user_id=sender_external_user_id,
            sender_identity_type=sender_identity_type,
            raw_content=getattr(message, "content", None),
        )

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
        builder = builder.timeout(float(config.options.get("timeout_seconds", 10.0)))
        return builder.build()

    def _message_to_event(self, session_id: str, item: Any) -> ProviderEvent | None:
        message_id = getattr(item, "message_id", None)
        msg_type = getattr(item, "msg_type", None)
        create_time = getattr(item, "create_time", None)
        sender = getattr(item, "sender", None)
        sender_external_user_id = getattr(sender, "id", None)
        sender_id_type = getattr(sender, "id_type", None)
        sender_identity_type = _normalize_lark_openapi_sender_type(getattr(sender, "sender_type", None))
        if sender_identity_type is None:
            return None
        if not message_id or not msg_type or create_time is None or not sender_external_user_id:
            return None
        expected_id_type = "open_id" if sender_identity_type == "user" else "app_id"
        if sender_id_type != expected_id_type:
            return None

        body = getattr(item, "body", None)
        raw_content = getattr(body, "content", None) if body is not None else None
        return self._build_provider_event(
            session_id=session_id,
            message_id=message_id,
            msg_type=msg_type,
            create_time=create_time,
            sender_external_user_id=sender_external_user_id,
            sender_identity_type=sender_identity_type,
            raw_content=raw_content,
        )

    def _build_provider_event(
        self,
        *,
        session_id: str,
        message_id: Any,
        msg_type: Any,
        create_time: Any,
        sender_external_user_id: Any,
        sender_identity_type: str,
        raw_content: Any,
    ) -> ProviderEvent | None:
        if not message_id or not msg_type or create_time is None or not sender_external_user_id:
            return None
        try:
            event_ms = int(create_time)
        except (TypeError, ValueError):
            return None
        if event_ms < 0:
            return None
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


def _normalize_lark_openapi_sender_type(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.lower()
    if normalized in {"app", "bot"}:
        return "bot"
    if normalized == "user":
        return "user"
    return None


def _is_retryable_lark_response(response: Any) -> bool:
    try:
        code = int(getattr(response, "code", 0))
    except (TypeError, ValueError):
        code = 0
    if code in _LARK_RETRYABLE_CODES:
        return True
    raw = getattr(response, "raw", None)
    status_code = getattr(raw, "status_code", 0)
    return status_code == 429 or 500 <= status_code < 600


def _log_lark_response(event: str, response: Any, **context: Any) -> None:
    data = getattr(response, "data", None)
    items = getattr(data, "items", None)
    if isinstance(items, list):
        message_summary: Any = [_summarize_lark_message(item) for item in items[:5]]
    else:
        message_summary = _summarize_lark_message(data)
    _LOGGER.info(
        "%s context=%s success=%s code=%s msg=%s log_id=%s troubleshooter=%s data=%s",
        event,
        context,
        _safe_response_success(response),
        getattr(response, "code", None),
        getattr(response, "msg", None),
        _safe_response_call(response, "get_log_id"),
        _safe_response_call(response, "get_troubleshooter"),
        message_summary,
    )


def _summarize_lark_message(item: Any) -> dict[str, Any] | None:
    if item is None:
        return None
    sender = getattr(item, "sender", None)
    return {
        "message_id": getattr(item, "message_id", None),
        "chat_id": getattr(item, "chat_id", None),
        "msg_type": getattr(item, "msg_type", None),
        "create_time": getattr(item, "create_time", None),
        "update_time": getattr(item, "update_time", None),
        "deleted": getattr(item, "deleted", None),
        "updated": getattr(item, "updated", None),
        "sender_id": getattr(sender, "id", None),
        "sender_id_type": getattr(sender, "id_type", None),
        "sender_type": getattr(sender, "sender_type", None),
    }


def _safe_response_call(response: Any, method_name: str) -> Any:
    method = getattr(response, method_name, None)
    if not callable(method):
        return None
    try:
        return method()
    except Exception:
        return None


def _safe_response_success(response: Any) -> Any:
    return _safe_response_call(response, "success")
