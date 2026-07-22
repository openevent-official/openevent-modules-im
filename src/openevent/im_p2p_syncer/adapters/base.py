from __future__ import annotations

from typing import Protocol

from ..models import HistoryPage, ProviderEvent, SendResult


class ProviderAdapter(Protocol):
    def provider_name(self) -> str:
        ...

    def start_event_stream(self, session_ids: set[str]) -> None:
        ...

    def stop_event_stream(self) -> None:
        ...

    def event_stream_error(self) -> BaseException | None:
        ...

    def event_stream_generation(self) -> int:
        ...

    def take_event(self) -> ProviderEvent | None:
        ...

    def fetch_history_page(
        self,
        session_id: str,
        start_ms: int,
        end_ms: int,
        page_token: str | None,
    ) -> HistoryPage:
        ...

    def send_message(
        self,
        session_id: str,
        sender_external_user_id: str,
        msg_type: str,
        content: dict[str, object],
        request_id: str,
    ) -> SendResult:
        ...
