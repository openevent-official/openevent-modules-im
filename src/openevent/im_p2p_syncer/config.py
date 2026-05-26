from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from openevent.im_sdk import MalformedPayloadError
from openevent.im_sdk.normalizer import require_uint64

from .models import (
    MappingEntry,
    OpenEventConfig,
    ProviderConfig,
    ProviderSyncConfig,
    RetryConfig,
    SyncerConfig,
    WorkerConfig,
)


class ConfigError(ValueError):
    pass


LARK_OPENAPI_ADAPTERS = {"feishu", "lark"}


def load_config(path: str | Path) -> SyncerConfig:
    with Path(path).open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file)
    return parse_config(raw)


def parse_config(raw: dict[str, Any]) -> SyncerConfig:
    if not isinstance(raw, dict):
        raise ConfigError("config root must be a YAML mapping")
    version = _str(raw.get("version"), "version")
    if version != "v1":
        raise ConfigError("version must be v1")

    worker_raw = _obj(raw.get("worker"), "worker")
    worker = WorkerConfig(
        name=_str(worker_raw.get("name"), "worker.name"),
        principal=_positive_uint64(worker_raw.get("principal"), "worker.principal"),
        token=_str(worker_raw.get("token"), "worker.token"),
        request_result_timeout_ms=_positive_int(
            worker_raw.get("request_result_timeout_ms", 60000),
            "worker.request_result_timeout_ms",
        ),
        shutdown_timeout_ms=_non_negative_int(
            worker_raw.get("shutdown_timeout_ms", 10000),
            "worker.shutdown_timeout_ms",
        ),
    )

    openevent_raw = _obj(raw.get("openevent"), "openevent")
    openevent = OpenEventConfig(
        target=_str(openevent_raw.get("target"), "openevent.target"),
    )

    retry_raw = _obj(raw.get("retry", {}), "retry")
    retry = RetryConfig(
        publish_max_attempts=_positive_int(
            retry_raw.get("publish_max_attempts", 5),
            "retry.publish_max_attempts",
        ),
        publish_initial_backoff_ms=_non_negative_int(
            retry_raw.get("publish_initial_backoff_ms", 200),
            "retry.publish_initial_backoff_ms",
        ),
        publish_max_backoff_ms=_non_negative_int(
            retry_raw.get("publish_max_backoff_ms", 5000),
            "retry.publish_max_backoff_ms",
        ),
        provider_send_max_attempts=_positive_int(
            retry_raw.get("provider_send_max_attempts", 5),
            "retry.provider_send_max_attempts",
        ),
        idle_sleep_ms=_non_negative_int(retry_raw.get("idle_sleep_ms", 200), "retry.idle_sleep_ms"),
    )

    principal_tokens = _parse_principal_tokens(raw.get("principal_tokens"), worker.principal)
    providers = _parse_providers(raw.get("providers"))
    mappings = _parse_mappings(raw.get("mappings"))
    _validate_provider_refs(providers, mappings)

    return SyncerConfig(
        version=version,
        worker=worker,
        openevent=openevent,
        retry=retry,
        principal_tokens=principal_tokens,
        providers=providers,
        mappings=mappings,
        logging=_obj(raw.get("logging", {}), "logging"),
    )


def _parse_principal_tokens(raw: Any, worker_principal: int) -> dict[int, str]:
    if not isinstance(raw, list) or not raw:
        raise ConfigError("principal_tokens must be a non-empty array")
    result: dict[int, str] = {}
    for index, item in enumerate(raw):
        item = _obj(item, f"principal_tokens[{index}]")
        principal = _positive_uint64(item.get("principal"), f"principal_tokens[{index}].principal")
        if principal == worker_principal:
            raise ConfigError("principal_tokens must not contain worker.principal")
        if principal in result:
            raise ConfigError("duplicate principal_tokens[].principal")
        result[principal] = _str(item.get("token"), f"principal_tokens[{index}].token")
    return result


def _parse_providers(raw: Any) -> dict[str, ProviderConfig]:
    if not isinstance(raw, list) or not raw:
        raise ConfigError("providers must be a non-empty array")
    result: dict[str, ProviderConfig] = {}
    for index, item in enumerate(raw):
        item = _obj(item, f"providers[{index}]")
        name = _str(item.get("name"), f"providers[{index}].name")
        if name in result:
            raise ConfigError("duplicate providers[].name")
        sync_raw = _obj(item.get("sync"), f"providers[{index}].sync")
        sync = ProviderSyncConfig(
            mode=_str(sync_raw.get("mode"), f"providers[{index}].sync.mode"),
            interval_ms=_positive_int(
                sync_raw.get("interval_ms", 5000),
                f"providers[{index}].sync.interval_ms",
            ),
            page_size=_positive_int(sync_raw.get("page_size", 50), f"providers[{index}].sync.page_size"),
            startup_lookback_ms=_non_negative_int(
                sync_raw.get("startup_lookback_ms", 300000),
                f"providers[{index}].sync.startup_lookback_ms",
            ),
        )
        credentials = _obj(item.get("credentials"), f"providers[{index}].credentials")
        adapter = _str(item.get("adapter"), f"providers[{index}].adapter")
        if adapter in LARK_OPENAPI_ADAPTERS:
            if name != adapter:
                raise ConfigError(f"providers[{index}].adapter must match name for feishu/lark providers")
            if sync.mode != "poll":
                raise ConfigError(f"{name} provider sync.mode must be poll")
            _str(credentials.get("app_id"), f"providers[{index}].credentials.app_id")
            _str(credentials.get("app_secret"), f"providers[{index}].credentials.app_secret")
        result[name] = ProviderConfig(
            name=name,
            enabled=_bool(item.get("enabled", True), f"providers[{index}].enabled"),
            adapter=adapter,
            sync=sync,
            credentials=credentials,
            options=_obj(item.get("options", {}), f"providers[{index}].options"),
        )
    return result


def _parse_mappings(raw: Any) -> list[MappingEntry]:
    if not isinstance(raw, list) or not raw:
        raise ConfigError("mappings must be a non-empty array")
    result: list[MappingEntry] = []
    for index, item in enumerate(raw):
        item = _obj(item, f"mappings[{index}]")
        result.append(
            MappingEntry(
                provider=_str(item.get("provider"), f"mappings[{index}].provider"),
                identity_type=_identity_type(
                    item.get("identity_type", "user"), f"mappings[{index}].identity_type"
                ),
                external_user_id=_str(
                    item.get("external_user_id"), f"mappings[{index}].external_user_id"
                ),
                principal=_positive_uint64(item.get("principal"), f"mappings[{index}].principal"),
                session_id=_str(item.get("session_id"), f"mappings[{index}].session_id"),
                channel_id=_positive_uint64(item.get("channel_id"), f"mappings[{index}].channel_id"),
                status=_str(item.get("status"), f"mappings[{index}].status"),
            )
        )
    return result


def _validate_provider_refs(providers: dict[str, ProviderConfig], mappings: list[MappingEntry]) -> None:
    for mapping in mappings:
        if mapping.status == "active" and mapping.provider not in providers:
            raise ConfigError(f"mapping references unknown provider: {mapping.provider}")
        provider = providers.get(mapping.provider)
        if provider and not provider.enabled:
            raise ConfigError(f"mapping references disabled provider: {mapping.provider}")


def _obj(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"{field} must be an object")
    return value


def _str(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{field} must be a non-empty string")
    return value


def _identity_type(value: Any, field: str) -> str:
    value = _str(value, field)
    if value not in {"user", "bot"}:
        raise ConfigError(f"{field} must be user or bot")
    return value


def _bool(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise ConfigError(f"{field} must be bool")
    return value


def _uint64(value: Any, field: str) -> int:
    try:
        return require_uint64(value, field)
    except MalformedPayloadError as exc:
        raise ConfigError(str(exc)) from exc


def _positive_uint64(value: Any, field: str) -> int:
    try:
        return require_uint64(value, field, positive=True)
    except MalformedPayloadError as exc:
        raise ConfigError(str(exc)) from exc


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigError(f"{field} must be a positive integer")
    return value


def _non_negative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConfigError(f"{field} must be a non-negative integer")
    return value
