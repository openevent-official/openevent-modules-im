from __future__ import annotations

from collections import defaultdict

from .config import ConfigError
from .models import MappingEntry, SyncerConfig


class P2PMappingIndex:
    def __init__(self, config: SyncerConfig):
        self._config = config
        self._active = [item for item in config.mappings if item.status == "active"]
        self._by_channel: dict[int, list[MappingEntry]] = defaultdict(list)
        self._channel_session: dict[int, tuple[str, str]] = {}
        self._session_channel: dict[tuple[str, str], int] = {}
        self._external_by_channel: dict[tuple[int, str, str, str], MappingEntry] = {}
        self._principal_by_channel: dict[tuple[int, int], MappingEntry] = {}
        self._build()

    @property
    def channel_ids(self) -> list[int]:
        return sorted(self._by_channel)

    def owns_channel(self, channel_id: int) -> bool:
        return channel_id in self._by_channel

    def entries_for_channel(self, channel_id: int) -> list[MappingEntry]:
        return list(self._by_channel[channel_id])

    def provider_session(self, channel_id: int) -> tuple[str, str]:
        return self._channel_session[channel_id]

    def provider_config_for_channel(self, channel_id: int):
        provider, _ = self.provider_session(channel_id)
        return self._config.providers[provider]

    def sender_external_user_id(
        self,
        channel_id: int,
        principal: int,
        identity_type: str | None = None,
    ) -> str:
        entry = self._principal_by_channel.get((channel_id, principal))
        if entry is None:
            raise KeyError(f"principal {principal} is not active in channel {channel_id}")
        if identity_type is not None and entry.identity_type != identity_type:
            raise KeyError(
                f"principal {principal} is not active as {identity_type} in channel {channel_id}"
            )
        return entry.external_user_id

    def principal_for_external_user(
        self,
        channel_id: int,
        provider: str,
        external_user_id: str,
        identity_type: str = "user",
    ) -> int:
        entry = self._external_by_channel.get((channel_id, provider, identity_type, external_user_id))
        if entry is None:
            raise KeyError(
                f"external {identity_type} {external_user_id} is not active in channel {channel_id}"
            )
        return entry.principal

    def peer_principals(self, channel_id: int, principal: int) -> list[int]:
        return sorted(item.principal for item in self._by_channel[channel_id] if item.principal != principal)

    def user_token(self, principal: int) -> str:
        return self._config.principal_tokens[principal]

    def _build(self) -> None:
        for item in self._active:
            self._by_channel[item.channel_id].append(item)

        for channel_id, entries in self._by_channel.items():
            principals = {item.principal for item in entries}
            identity_types = {item.identity_type for item in entries}
            if len(principals) != 2 or len(entries) != 2:
                raise ConfigError(
                    f"active p2p channel {channel_id} must have exactly two distinct principals"
                )
            if identity_types != {"user", "bot"}:
                raise ConfigError(f"active p2p channel {channel_id} must have one user and one bot")
            for item in entries:
                if item.principal not in self._config.principal_tokens:
                    raise ConfigError(f"principal {item.principal} has no token")
                external_key = (channel_id, item.provider, item.identity_type, item.external_user_id)
                if external_key in self._external_by_channel:
                    raise ConfigError("duplicate (channel_id, provider, identity_type, external_user_id)")
                self._external_by_channel[external_key] = item
                principal_key = (channel_id, item.principal)
                if principal_key in self._principal_by_channel:
                    raise ConfigError("duplicate (channel_id, principal)")
                self._principal_by_channel[principal_key] = item

            sessions = {(item.provider, item.session_id) for item in entries}
            if len(sessions) != 1:
                raise ConfigError(f"channel {channel_id} maps to multiple provider sessions")
            session = next(iter(sessions))
            if session in self._session_channel and self._session_channel[session] != channel_id:
                raise ConfigError("duplicate (provider, session_id) across channels")
            self._session_channel[session] = channel_id
            self._channel_session[channel_id] = session
