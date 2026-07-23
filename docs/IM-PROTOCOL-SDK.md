# IM Protocol SDK Usage

[中文版](IM-PROTOCOL-SDK_cn.md)

> Version: v0.4
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
JsonObject = dict[str, object]

client = create_client(openevent_client: OpenEventClient) -> ImProtocolClient

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
client.parse_message(message: EventMessage) -> ParsedMessage
```

Every publish records the current OpenEvent `max_seq` before calling
`PublishAutoSeq`. If the response is uncertain, the client fixes a later
`max_seq` watermark and scans that interval with `Fetch`. A matching message is
returned as success; a new publish may be attempted only after the scan proves
that the original call did not commit. Failure to complete reconciliation is
reported as an unknown outcome and must not be retried directly.

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

- `ImProtocolError`
- `InvalidKindError`
- `MalformedPayloadError`
- `PublishFailedError`

`ImProtocolError` is the base class for invalid protocol inputs.
`InvalidKindError` reports invalid kinds, `MalformedPayloadError` reports
malformed payloads or fields, and `PublishFailedError` reports OpenEvent publish
failures.

`PublishFailedError.retry_safe` is true only when the failed attempt is known not
to have committed, including a completed reconciliation that found no matching
message. `PublishFailedError.outcome_unknown` is true when reconciliation could
not prove either outcome. Callers may retry only when `retry_safe` is true and
must stop or surface the unknown result when `outcome_unknown` is true.

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

1. The SDK ships in the `openevent-modules-im` package. The current SDK release
   is `v0.4`, and the package version is `0.4.0`.
2. SDK release versions and protocol versions evolve independently. SDK `v0.4`
   currently supports `im.v1`.
3. Breaking protocol changes require a new protocol version; `im.v1` accepts
   only backward-compatible extensions.
