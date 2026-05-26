from __future__ import annotations

from typing import Protocol

from ..models import AdapterHealth, SendResult, SyncBatch


class ProviderAdapter(Protocol):
    def provider_name(self) -> str:
        ...

    def stop_sync(self) -> None:
        ...

    def sync_once(self, session_id: str, cursor: object | None) -> SyncBatch:
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

    def health_check(self) -> AdapterHealth:
        ...
