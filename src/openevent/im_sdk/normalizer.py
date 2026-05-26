from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .errors import MalformedPayloadError

UINT64_MAX = 2**64 - 1


def require_uint64(value: Any, field_name: str, *, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MalformedPayloadError(f"{field_name} must be uint64")
    if value < 0 or value > UINT64_MAX:
        raise MalformedPayloadError(f"{field_name} out of uint64 range")
    if positive and value == 0:
        raise MalformedPayloadError(f"{field_name} must be greater than 0")
    return value


def require_timestamp_ms(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MalformedPayloadError(f"{field_name} must be a non-negative timestamp")
    return value


def require_duration_ms(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MalformedPayloadError(f"{field_name} must be a non-negative duration")
    return value


def require_non_empty_str(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise MalformedPayloadError(f"{field_name} must be a non-empty string")
    return value


def require_object(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise MalformedPayloadError(f"{field_name} must be a JSON object")
    return value


def normalize_recipients(
    recipients: Iterable[Any] | None,
    *,
    sort_unique: bool = False,
) -> list[int]:
    if recipients is None:
        return []
    result = [require_uint64(item, "recipients[]") for item in recipients]
    if sort_unique:
        return sorted(set(result))
    return result


def reject_source_principal(value: Any) -> None:
    if isinstance(value, dict):
        if "source_principal" in value:
            raise MalformedPayloadError("source_principal is forbidden")
        for child in value.values():
            reject_source_principal(child)
    elif isinstance(value, list):
        for child in value:
            reject_source_principal(child)
