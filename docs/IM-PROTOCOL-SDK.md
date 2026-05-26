# IM Protocol SDK Usage

[中文版](IM-PROTOCOL-SDK_cn.md)

> Version: v0.1
> Status: usable
> Scope: Python callers that publish, parse, and handle `im.v1` protocol
> messages.

The stable protocol specification is [`IM_PROTOCOL.md`](IM_PROTOCOL.md). This
document describes only the SDK's public responsibilities, public APIs, data
models, and integration patterns.

## 1. Responsibilities

The SDK is responsible for:

- Providing `im.v1` data models, encoding, parsing, and publish helpers.
- Wrapping standard publish entry points based on an installed `openevent-sdk`.
- Standardizing protocol writes to reduce duplicate payload construction in
  business modules and sync workers.
- Providing a timeout helper for `send.request -> send.result`.

The SDK is not responsible for:

- Creating or modifying OpenEvent channels.
- Calling Feishu/Lark, DingTalk, or other provider APIs.
- Managing business state, provider cursors, mapping tables, or idempotency
  state.
- Proving that callers satisfy all cross-message, cross-channel, or worker-
  specific protocol semantics.

## 2. Integration Points

```text
Business module
  -> openevent.sdk (direct subscription is allowed)
  -> openevent.im_sdk (publish, parse, encoding helpers)

IM sync worker
  -> openevent.sdk (direct subscription is allowed)
  -> openevent.im_sdk (publish, parse, encoding helpers)
  -> provider sdk
```

Integration constraints:

1. Business modules writing `send.request` SHOULD use the SDK or an equivalent
   encoding flow.
2. Sync workers publishing `sync.record` or `send.result` SHOULD use the SDK or
   an equivalent encoding flow.
3. SDK initialization MUST explicitly inject `openevent.sdk.OpenEventClient` or
   an equivalent OpenEvent client object.
4. Protocol semantics are defined by [`IM_PROTOCOL.md`](IM_PROTOCOL.md); callers
   are responsible for following them.
5. The runtime environment must provide a compatible `openevent-sdk`.

## 3. Public API

Current stable Python SDK API:

```python
UInt64 = int          # 0 <= value <= 2**64 - 1
TimestampMs = int     # Unix epoch milliseconds, value >= 0
DurationMs = int      # elapsed milliseconds, value >= 0
JsonObject = dict[str, object]

client = create_client(
    openevent_client: OpenEventClient,
    request_result_timeout_ms: DurationMs = 60000,
) -> ImProtocolClient

client.publish_send_request(
    principal: UInt64,
    token: str,
    channel_id: UInt64,
    req: SendRequestInput,
    recipients: list[UInt64] | None = None,
) -> UInt64

client.publish_send_result(
    principal: UInt64,
    token: str,
    channel_id: UInt64,
    recipients: list[UInt64],
    req: SendResultInput,
) -> UInt64

client.publish_sync_record(
    principal: UInt64,
    token: str,
    channel_id: UInt64,
    recipients: list[UInt64],
    req: SyncRecordInput,
) -> UInt64

client.parse_payload(payload: bytes) -> JsonObject
client.parse_message(message: Message) -> ParsedMessage

is_request_timeout(request_event_ms: TimestampMs, now_ms: TimestampMs, timeout_ms: DurationMs) -> bool
```

OpenEvent ID fields such as `principal`, `channel_id`, `seq`, and
`recipients[]` are `uint64`. Python APIs use `int`, but SDKs and callers must
respect the `UInt64` range.

Data models:

- `SendRequestInput`
- `SendResultInput`
- `SyncRecordInput`
- `ParsedMessage`

Data model field types follow [`IM_PROTOCOL.md`](IM_PROTOCOL.md). Fields such as
`request_id`, `msg_type`, `provider_message_id`, `status`, `error_code`, and
`error_message` are strings; `content` and `content_raw` are JSON objects;
`event_ms` and `ingested_ms` are `TimestampMs`; `prev_seq` is `UInt64`.

API notes:

- All write APIs explicitly receive `principal` and `token`.
- `publish_send_request(...)` exposes OpenEvent `recipients`; callers decide its
  value. If omitted or `None`, the SDK publishes an empty list. This field only
  expresses OpenEvent targeted visibility and does not define the IM send target.
- `publish_send_result(...)` requires callers to pass the original
  `send.request` principal as recipients. The SDK normalizes list structure and
  `UInt64` ranges.
- `publish_sync_record(...)` passes through caller-provided `recipients` and
  should normalize, deduplicate, and sort `UInt64` values.
- The SDK uses the OpenEvent message top-level `principal` for source identity
  and does not write `source_principal` into payload.

## 4. Error Types

Public SDK error types:

- `INVALID_KIND`
- `MALFORMED_PAYLOAD`
- `PUBLISH_FAILED`
- `UNSUPPORTED_PROTOCOL_VERSION`

OpenEvent publish failures are exposed as `PUBLISH_FAILED`. Invalid payloads,
invalid field types, or unsupported protocol versions use their corresponding
SDK errors.

## 5. Examples

Publish `send.request`:

```python
from openevent.im_sdk import SendRequestInput, create_client

client = create_client(openevent_client)
seq = client.publish_send_request(
    principal=90002,
    token="tok-bot-90002",
    channel_id=10001,
    req=SendRequestInput(
        request_id="req_001",
        msg_type="text",
        content={"text": "hello"},
        event_ms=1710000001000,
    ),
)
```

Parse an OpenEvent message:

```python
from openevent.im_sdk import create_client

client = create_client(openevent_client)
parsed = client.parse_message(message)
if parsed.kind == "sync.record":
    provider_message_id = parsed.data["provider_message_id"]
```

Check business-observed timeout:

```python
from openevent.im_sdk import is_request_timeout

timed_out = is_request_timeout(
    request_event_ms=1710000001000,
    now_ms=1710000062000,
    timeout_ms=60000,
)
```

## 6. Integration Flow

Business module:

1. Call SDK `publish_send_request(...)`.
2. Record the returned OpenEvent `seq` and business state.
3. Use `openevent-sdk` directly for subscription if needed.
4. Call SDK `parse_message(...)` on received messages.

Sync worker:

1. Read OpenEvent messages with `openevent-sdk` and filter `send.request`.
2. Parse payloads with SDK `parse_message(...)`.
3. After the send action completes, write results with SDK
   `publish_send_result(...)`; the caller ensures recipients match the original
   `send.request.principal`.
4. Publish inbound events with SDK `publish_sync_record(...)`. If OpenEvent
   rejects the full payload, construct and retry with a degraded record according
   to the protocol.

## 7. Versioning

1. SDK versions are bound to protocol versions: `im.v1` -> SDK `v1.x`.
2. Breaking changes use a new protocol: `im.v2` + SDK `v2.x`.
3. A single major version only accepts backward-compatible extensions.
