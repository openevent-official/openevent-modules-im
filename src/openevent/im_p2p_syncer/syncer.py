from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any

from grpc import RpcError, StatusCode

from openevent.im_sdk import ImProtocolClient, create_client
from openevent.sdk.proto import openevent_pb2

from .adapter_registry import create_adapter
from .config import ConfigError
from .errors import StateConflictError
from .loop import FatalSyncerError, SingleThreadProcessor, SyncerStopped
from .mapping import P2PMappingIndex
from .models import SyncerConfig
from .state import RuntimeState


class P2PSyncer:
    def __init__(self, config: SyncerConfig, openevent_client: Any):
        self.config = config
        self.openevent_client = openevent_client
        self.im_client: ImProtocolClient = create_client(openevent_client)
        self.mapping = P2PMappingIndex(config)
        self.adapters = {
            name: create_adapter(provider)
            for name, provider in config.providers.items()
        }
        for name, adapter in self.adapters.items():
            if adapter.provider_name() != name:
                raise ConfigError(f"adapter provider_name mismatch: {name}")
        self.state = RuntimeState()
        self._stop_event = threading.Event()
        self._stop_lock = threading.Lock()
        self._stopped = False
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
            stop_event=self._stop_event,
        )

    def start(self) -> None:
        try:
            self._validate_channels()
            history_anchor_ms = int(time.time() * 1000)
            self._start_event_streams()
            scan_end_seq = self._scan_history()
            self.processor.initialize_history_repair(history_anchor_ms)
            self._complete_initial_history_repair()
            self._poll_events(scan_end_seq + 1)
        except SyncerStopped:
            if not self._stop_event.is_set():
                raise
        except BaseException:
            self.stop()
            raise

    def stop(self) -> None:
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True
            self._stop_event.set()
            for adapter in self.adapters.values():
                adapter.stop_event_stream()

    def _start_event_streams(self) -> None:
        for provider, adapter in self.adapters.items():
            session_ids = {
                session_id
                for channel_id in self.mapping.channel_ids
                for mapped_provider, session_id in [self.mapping.provider_session(channel_id)]
                if mapped_provider == provider
            }
            adapter.start_event_stream(session_ids)

    def _validate_channels(self) -> None:
        for channel_id in self.mapping.channel_ids:
            if self._stop_event.is_set():
                raise SyncerStopped("syncer stopped during channel validation")
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

    def _complete_initial_history_repair(self) -> None:
        while self.processor.history_repair_pending():
            if self._stop_event.is_set():
                raise SyncerStopped("syncer stopped during provider history repair")
            if not self.processor.tick():
                self._stop_event.wait(self.config.retry.idle_sleep_ms / 1000)

    def _validate_channel_members(self, channel_id: int, channel: Any) -> None:
        if channel.visibility != openevent_pb2.VISIBILITY_PRIVATE:
            raise ConfigError(f"channel {channel_id} visibility must be private")
        members = set(getattr(channel, "members", []))
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
            if self._stop_event.is_set():
                raise SyncerStopped("syncer stopped during history scan")
            response = self.openevent_client.fetch(
                self.config.worker.principal,
                self.config.worker.token,
                from_seq=from_seq,
                limit=1000,
                only_my_recipient=False,
                channels=self._fetch_channels(),
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
            seq = getattr(message, "seq", 0)
            raise RuntimeError(f"failed to parse OpenEvent history message seq={seq}") from exc
        if not self.mapping.owns_channel(parsed.channel_id):
            return
        self._validate_message_roles(parsed)
        provider, session_id = self.mapping.provider_session(parsed.channel_id)
        if parsed.kind == "send.request":
            self.state.add_send_request(parsed)
        elif parsed.kind == "send.result":
            self.state.add_send_result(parsed, provider, session_id)
        elif parsed.kind == "sync.record":
            self.state.add_sync_record(parsed, provider, session_id)

    def _poll_events(self, from_seq: int) -> None:
        next_seq = from_seq
        snapshot_last_seq: int | None = None
        snapshot_had_messages = False
        prefetched_messages: list[Any] = []
        prefetched_last_seq: int | None = None
        while not self._stop_event.is_set():
            if snapshot_last_seq is None and prefetched_last_seq is not None:
                snapshot_last_seq = prefetched_last_seq
                prefetched_last_seq = None
                for message in prefetched_messages:
                    self._process_live_message(message)
                snapshot_had_messages = bool(prefetched_messages)
                prefetched_messages = []
            else:
                try:
                    response = self.openevent_client.fetch(
                        self.config.worker.principal,
                        self.config.worker.token,
                        from_seq=next_seq,
                        limit=1000,
                        only_my_recipient=False,
                        channels=self._fetch_channels(),
                    )
                except Exception as exc:
                    if self._stop_event.is_set():
                        break
                    if not _is_transient_fetch_error(exc):
                        raise FatalSyncerError(
                            f"OpenEvent Fetch failed permanently at next_seq={next_seq}"
                        ) from exc
                    self.logger.warning("fetch_live_failed next_seq=%s error=%s", next_seq, exc)
                    self._stop_event.wait(self.config.retry.idle_sleep_ms / 1000)
                    continue
                if snapshot_last_seq is None:
                    snapshot_last_seq = int(response.last_seq)
                current_messages = []
                for message in response.messages:
                    if int(message.seq) <= snapshot_last_seq:
                        current_messages.append(message)
                    else:
                        prefetched_messages.append(message)
                if prefetched_messages:
                    response_last_seq = int(response.last_seq)
                    prefetched_last_seq = max(prefetched_last_seq or 0, response_last_seq)
                for message in current_messages:
                    self._process_live_message(message)
                snapshot_had_messages = snapshot_had_messages or bool(current_messages)
                next_seq = self._validated_next_seq(response, next_seq)
            if next_seq <= snapshot_last_seq:
                continue
            self.processor.tick()
            if not snapshot_had_messages:
                self._stop_event.wait(self.config.retry.idle_sleep_ms / 1000)
            snapshot_last_seq = None
            snapshot_had_messages = False

    @staticmethod
    def _validated_next_seq(response: Any, from_seq: int) -> int:
        next_seq = int(response.next_seq)
        last_seq = int(response.last_seq)
        if next_seq <= last_seq and next_seq <= from_seq:
            raise RuntimeError("Fetch did not advance next_seq")
        return next_seq

    def _fetch_channels(self) -> tuple[int, ...]:
        return tuple(self.mapping.channel_ids)

    def _process_live_message(self, message: Any) -> None:
        try:
            parsed = self.im_client.parse_message(message)
        except Exception as exc:
            seq = getattr(message, "seq", 0)
            raise FatalSyncerError(f"failed to parse live OpenEvent message seq={seq}") from exc
        if not self.mapping.owns_channel(parsed.channel_id):
            return
        try:
            self._validate_message_roles(parsed)
        except StateConflictError as exc:
            raise FatalSyncerError(
                f"invalid live OpenEvent message roles at seq={parsed.seq}"
            ) from exc
        if parsed.kind != "send.request":
            return
        try:
            self.state.add_send_request(parsed)
        except StateConflictError as exc:
            raise FatalSyncerError(
                f"conflicting live OpenEvent state at seq={parsed.seq}"
            ) from exc

    def _validate_message_roles(self, parsed: Any) -> None:
        participant_principals = {
            item.principal for item in self.mapping.entries_for_channel(parsed.channel_id)
        }
        if parsed.kind == "sync.record":
            if parsed.principal not in participant_principals:
                raise StateConflictError("sync.record principal is not a channel participant")
            expected_recipients = self.mapping.peer_principals(
                parsed.channel_id, parsed.principal
            )
            if parsed.recipients != expected_recipients:
                raise StateConflictError("sync.record recipients do not match the P2P peer")
        elif parsed.kind == "send.result":
            if parsed.principal != self.config.worker.principal:
                raise StateConflictError("send.result principal is not the sync worker")


def _is_transient_fetch_error(error: BaseException) -> bool:
    if not isinstance(error, RpcError):
        return False
    try:
        return error.code() in {StatusCode.UNAVAILABLE, StatusCode.DEADLINE_EXCEEDED}
    except Exception:
        return False
