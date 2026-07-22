from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class WorkerConfig:
    principal: int
    token: str
    shutdown_timeout_ms: int = 10000


@dataclass(frozen=True)
class OpenEventConfig:
    target: str
    rpc_timeout_seconds: float = 10.0


@dataclass(frozen=True)
class RetryConfig:
    publish_max_attempts: int = 5
    publish_initial_backoff_ms: int = 200
    publish_max_backoff_ms: int = 5000
    provider_send_max_attempts: int = 5
    provider_send_retry_delay_ms: int = 1000
    idle_sleep_ms: int = 200


@dataclass(frozen=True)
class ProviderSyncConfig:
    history_retry_delay_ms: int = 1000
    history_overlap_ms: int = 300000
    history_lookback_ms: int = 300000
    page_size: int = 50
    event_queue_size: int = 1000


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    sync: ProviderSyncConfig
    credentials: dict[str, Any] = field(default_factory=dict)
    options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MappingEntry:
    provider: str
    identity_type: str
    external_user_id: str
    principal: int
    session_id: str
    channel_id: int


@dataclass(frozen=True)
class SyncerConfig:
    version: str
    worker: WorkerConfig
    openevent: OpenEventConfig
    retry: RetryConfig
    principal_tokens: dict[int, str]
    providers: dict[str, ProviderConfig]
    mappings: list[MappingEntry]
    logging: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProviderEvent:
    provider: str
    session_id: str
    provider_message_id: str
    sender_external_user_id: str
    msg_type: str
    content_raw: dict[str, Any]
    event_ms: int
    text: str | None = None
    sender_identity_type: str = "user"


@dataclass(frozen=True)
class HistoryPage:
    events: list[ProviderEvent]
    next_page_token: str | None = None


@dataclass(frozen=True)
class SendResult:
    success: bool
    provider_message_id: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    retryable: bool = False
