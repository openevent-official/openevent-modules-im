from __future__ import annotations

import json
import logging
import time
from typing import Any

from openevent.im_sdk import ImProtocolClient, create_client

from .adapter_registry import create_adapter
from .config import ConfigError
from .loop import SingleThreadProcessor
from .mapping import P2PMappingIndex
from .models import SyncerConfig
from .state import RuntimeState


class P2PSyncer:
    def __init__(self, config: SyncerConfig, openevent_client: Any):
        self.config = config
        self.openevent_client = openevent_client
        self.im_client: ImProtocolClient = create_client(
            openevent_client,
            request_result_timeout_ms=config.worker.request_result_timeout_ms,
        )
        self.mapping = P2PMappingIndex(config)
        self.adapters = {
            name: create_adapter(provider)
            for name, provider in config.providers.items()
            if provider.enabled
        }
        for name, adapter in self.adapters.items():
            if adapter.provider_name() != name:
                raise ConfigError(f"adapter provider_name mismatch: {name}")
        self.state = RuntimeState()
        self.stopped = False
        self.logger = logging.getLogger("openevent.im_p2p_syncer")
        self.processor = SingleThreadProcessor(
            im_client=self.im_client,
            mapping=self.mapping,
            adapters=self.adapters,
            retry=config.retry,
            worker_principal=config.worker.principal,
            worker_token=config.worker.token,
            state=self.state,
            logger=self.logger,
        )
        self.next_event_seq = 0

    def start(self) -> None:
        self._validate_channels()
        scan_end_seq = self._scan_history()
        self._restore_pending_requests()
        self.next_event_seq = scan_end_seq + 1
        self._consume_subscription(self.next_event_seq)

    def stop(self) -> None:
        self.stopped = True
        for adapter in self.adapters.values():
            adapter.stop_sync()

    def _validate_channels(self) -> None:
        for channel_id in self.mapping.channel_ids:
            response = self.openevent_client.get_channel(
                self.config.worker.principal,
                self.config.worker.token,
                channel_id,
            )
            channel = response.channel
            if channel.protocol != "im.v1":
                raise ConfigError(f"channel {channel_id} protocol must be im.v1")
            try:
                description = json.loads(channel.description)
            except json.JSONDecodeError as exc:
                raise ConfigError(f"channel {channel_id} description must be JSON") from exc
            provider, session_id = self.mapping.provider_session(channel_id)
            if description.get("provider") != provider:
                raise ConfigError(f"channel {channel_id} description.provider mismatch")
            if description.get("session_id") != session_id:
                raise ConfigError(f"channel {channel_id} description.session_id mismatch")
            if description.get("session_type") != "p2p":
                raise ConfigError(f"channel {channel_id} description.session_type must be p2p")
            self._validate_channel_members(channel_id, channel)

    def _validate_channel_members(self, channel_id: int, channel: Any) -> None:
        members = set(getattr(channel, "members", []))
        visibility = getattr(channel, "visibility", 0)
        if visibility == 0:
            return
        required = {self.config.worker.principal}
        required.update(item.principal for item in self.mapping.entries_for_channel(channel_id))
        missing = required - members
        if missing:
            raise ConfigError(f"channel {channel_id} missing members: {sorted(missing)}")

    def _scan_history(self) -> int:
        status = self.openevent_client.get_status(
            self.config.worker.principal,
            self.config.worker.token,
        )
        scan_end_seq = int(status.max_seq)
        if scan_end_seq == 0:
            return 0

        from_seq = 1
        while from_seq <= scan_end_seq:
            response = self.openevent_client.fetch(
                self.config.worker.principal,
                self.config.worker.token,
                from_seq=from_seq,
                limit=1000,
                only_my_recipient=False,
            )
            for message in response.messages:
                if message.seq <= scan_end_seq:
                    self._process_history_message(message)
            next_seq = int(response.next_seq)
            if next_seq <= from_seq:
                raise RuntimeError("Fetch did not advance next_seq during history scan")
            from_seq = next_seq
        return scan_end_seq

    def _process_history_message(self, message: Any) -> None:
        try:
            parsed = self.im_client.parse_message(message)
        except Exception as exc:
            self.logger.warning("history_parse_failed seq=%s error=%s", getattr(message, "seq", 0), exc)
            return
        if not self.mapping.owns_channel(parsed.channel_id):
            return
        provider, session_id = self.mapping.provider_session(parsed.channel_id)
        if parsed.kind == "send.request":
            self.state.add_send_request(parsed)
        elif parsed.kind == "send.result" and parsed.principal == self.config.worker.principal:
            self.state.add_send_result(parsed, provider, session_id)
        elif parsed.kind == "sync.record":
            self.state.add_sync_record(parsed, provider, session_id)

    def _restore_pending_requests(self) -> None:
        return None

    def _consume_subscription(self, from_seq: int) -> None:
        next_seq = from_seq
        while not self.stopped:
            self.processor.tick()
            try:
                response = self.openevent_client.fetch(
                    self.config.worker.principal,
                    self.config.worker.token,
                    from_seq=next_seq,
                    limit=1000,
                    only_my_recipient=False,
                )
                if not response.messages:
                    next_seq = int(response.next_seq)
                    time.sleep(self.config.retry.idle_sleep_ms / 1000)
                    continue
                for message in response.messages:
                    self._process_live_message(message)
                    next_seq = int(message.seq) + 1
                    self.next_event_seq = next_seq
            except Exception as exc:
                if self.stopped:
                    break
                self.logger.warning("fetch_live_failed next_seq=%s error=%s", next_seq, exc)
                time.sleep(self.config.retry.idle_sleep_ms / 1000)
                continue
            self.processor.tick()

    def _process_live_message(self, message: Any) -> None:
        try:
            parsed = self.im_client.parse_message(message)
        except Exception as exc:
            self.logger.warning("live_parse_failed seq=%s error=%s", getattr(message, "seq", 0), exc)
            return
        if parsed.kind != "send.request" or not self.mapping.owns_channel(parsed.channel_id):
            return
        self.state.add_send_request(parsed)
