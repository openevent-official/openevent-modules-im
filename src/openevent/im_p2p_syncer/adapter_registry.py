from __future__ import annotations

from .adapters.base import ProviderAdapter
from .adapters.lark_openapi import LarkOpenAPIAdapter
from .config import ConfigError
from .models import ProviderConfig


def create_adapter(config: ProviderConfig) -> ProviderAdapter:
    if config.adapter in {"feishu", "lark"}:
        return LarkOpenAPIAdapter(config)
    raise ConfigError(f"unsupported provider adapter: {config.adapter}")
