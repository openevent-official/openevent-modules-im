# IM Protocol im.v1

[中文版](IM_PROTOCOL_cn.md)

> Status: stable specification
> Scope: IM payloads and channel descriptions for OpenEvent channels with
> `protocol="im.v1"`.

This document defines only the public `im.v1` protocol rules. SDK APIs are
documented in [`IM-PROTOCOL-SDK.md`](IM-PROTOCOL-SDK.md), and P2P direct-message
sync worker usage is documented in [`IM-P2P-SYNCER.md`](IM-P2P-SYNCER.md).

## 1. Protocol Boundary

Business modules writing `send.request` SHOULD use
`ImProtocolClient.publish_send_request(...)`. Sync workers writing
`sync.record` or `send.result` SHOULD use the SDK or an equivalent encoding
flow. The SDK is the recommended encoding entry point, but it does not prove
that callers satisfy every cross-message, cross-channel, or worker-specific
semantic rule.

If a caller bypasses the SDK, constructs invalid `im.v1` payloads, or writes
OpenEvent top-level `principal`, `recipients`, or `channel_id` fields that
violate this protocol, the caller is violating the protocol. These inputs are
not treated as defects in `im.v1` or the sync worker. SDKs and workers may
reject, ignore, or log invalid messages, but the protocol does not provide
compatibility semantics for them.

## 2. Channel

Every IM channel MUST set:

```text
protocol = "im.v1"
```

`description` MUST be a JSON string:

```json
{
  "version": "v1",
  "provider": "lark",
  "session_id": "session_xxx",
  "session_type": "p2p|group",
  "updated_at_ms": 1710000000000,
  "metadata": {}
}
```

Field rules:

- `version`: string, currently fixed to `v1`.
- `provider`: string, IM provider name, such as `feishu`, `lark`, `dingtalk`,
  `line`, or `wechatwork`.
- `session_id`: string, stable provider session ID. If a provider has no stable
  session ID, use a business-defined stable ID.
- `session_type`: string, currently `p2p` or `group`; extensible later.
- `updated_at_ms`: integer, Unix epoch milliseconds, non-negative.
- `metadata`: optional object for provider-specific or scenario-specific static
  extension data.

The same `channel_id` can bind only one `(provider, session_id)`. The target of
`send.request` is determined by the IM session bound to `channel_id`, not by
OpenEvent `recipients`.

The protocol does not require participant lists in `description`. If a sync
worker needs participants, such as P2P principal pairs, group member snapshots,
or provider user IDs, it can store them in worker config or `metadata`, and must
declare the source of truth in the worker's public usage document.

## 3. Envelope

All `im.v1` payloads are UTF-8 JSON objects:

```json
{
  "kind": "sync.record",
  "prev_seq": 123456,
  "request_id": "req_xxx",
  "data": {},
  "timestamps": {
    "event_ms": 1710000000000,
    "ingested_ms": 1710000000123
  }
}
```

Type rules:

- `uint64` is a JSON integer in `0 <= value <= 2^64 - 1`. IM OpenEvent
  `channel_id` and `principal` values must be greater than 0.
- OpenEvent top-level `principal`, `channel_id`, `seq`, and `recipients[]` are
  all `uint64`.
- Payload fields that reference OpenEvent seq, such as `prev_seq`, also use
  `uint64`.
- Payload timestamps are JSON integers in Unix epoch milliseconds and must be
  non-negative.
- Object fields must be JSON objects, not arrays, strings, or null.

Common fields:

| Field | Rule |
| --- | --- |
| `kind` | string, required, one of `sync.record`, `send.request`, `send.result` |
| `data` | object, required, business payload |
| `timestamps.event_ms` | integer, required, source event time in Unix epoch milliseconds |
| `timestamps.ingested_ms` | integer, required for `sync.record` |
| `prev_seq` | uint64, conditionally required by kind rules |
| `request_id` | string, required for `send.request` and `send.result` |

OpenEvent top-level fields carry identity and targeted visibility, and are not
duplicated in payload:

| kind | OpenEvent `principal` | OpenEvent `recipients` |
| --- | --- | --- |
| `sync.record` | principal mapped from the IM sender | May be empty; workers may impose scenario constraints, such as P2P recipient principal |
| `send.request` | business caller principal | Caller-defined; SDK defaults to empty; does not express the IM send target |
| `send.result` | sync worker principal | Single element: the original `send.request` OpenEvent `principal` |

Payload MUST NOT contain `source_principal`. SDKs or workers must treat any
`im.v1` payload containing `source_principal` as invalid.

`im.v1` does not define a fixed payload size limit; the OpenEvent deployment
does. If publishing a full-fidelity `sync.record` is rejected because the payload
is too large, the sync worker MUST rewrite it as a degraded `sync.record` and
retry, preserving metadata useful for idempotency, troubleshooting, and future
compensation.

## 4. Kinds

### 4.1 `sync.record`

`sync.record` represents a real IM record synchronized from an IM provider into
OpenEvent.

```json
{
  "kind": "sync.record",
  "data": {
    "provider_message_id": "msg_xxx",
    "msg_type": "text",
    "text": "hello",
    "content_raw": {},
    "is_init": true
  },
  "timestamps": {
    "event_ms": 1710000000000,
    "ingested_ms": 1710000000123
  }
}
```

Rules:

- `request_id` SHOULD NOT be set.
- `prev_seq` is optional. If the provider message ID can be associated with a
  corresponding `send.result`, it MUST be set to that `send.result.seq`.
- `data.provider_message_id`: required non-empty string used for inbound
  idempotency.
- Worker inbound idempotency keys MUST include at least provider, provider
  session ID, OpenEvent `channel_id`, and `provider_message_id`.
- `data.msg_type`: required non-empty string.
- `data.content_raw`: required object, preserving provider raw content.
- `data.text`: optional string for text messages.
- `data.is_init`: optional bool; `true` marks a sync initialization boundary.

If a full-fidelity `sync.record` is rejected because the payload is too large,
`sync.record.data` MUST be rewritten to a degraded form.

The canonical degraded-record marker is:

- degraded: `data.content_omitted == true` and
  `data.omit_reason == "message_too_large"`;
- normal: no `content_omitted` or `omit_reason`; if present,
  `content_omitted` MUST be `false`.

Degraded record data example:

```json
{
  "provider_message_id": "msg_xxx",
  "msg_type": "text",
  "content_raw": {
    "omitted": true,
    "reason": "message_too_large",
    "original_size_bytes": 20971520,
    "metadata": {}
  },
  "content_omitted": true,
  "omit_reason": "message_too_large"
}
```

Degraded record rules:

- The worker first tries the full-fidelity `sync.record`. If OpenEvent rejects
  it because the payload exceeds the deployment limit, the worker constructs and
  publishes a degraded record.
- A degraded record still represents a real provider message. It participates in
  inbound idempotency and can advance the provider sync cursor after successful
  publication.
- `data.provider_message_id`, `data.msg_type`, and `data.content_raw` are
  required. OpenEvent top-level fields, `timestamps.event_ms`, and `prev_seq`
  keep their normal rules.
- `data.content_omitted` must be `true`; `data.omit_reason` must be
  `message_too_large`.
- `data.content_raw.omitted` must be `true`; `data.content_raw.reason` must be
  `message_too_large`.
- `data.content_raw.original_size_bytes` SHOULD be a positive integer when the
  provider or worker can determine it.
- `data.content_raw.metadata` MUST contain only lightweight metadata that will
  not exceed the payload limit again, such as provider session ID, sender ID,
  message type, filename, MIME type, provider-visible size, file key, or URL.
- For text messages whose text causes the limit breach, `data.text` MUST NOT
  contain the full text. It may be omitted or replaced with a bounded preview
  such as `data.text_preview`.
- Degraded records MUST NOT include the complete provider raw body, complete
  attachment bytes, complete base64 content, or complete overlong text.
- If the degraded record is still rejected as too large, the worker MUST further
  trim metadata or previews and retry. If it still cannot publish, it must stop
  advancing the provider cursor and alert; it must not silently drop the message.

### 4.2 `send.request`

`send.request` asks an IM sync worker to send a message to the provider.

```json
{
  "kind": "send.request",
  "prev_seq": 123456,
  "request_id": "req_xxx",
  "data": {
    "msg_type": "text",
    "content": { "text": "hello" }
  },
  "timestamps": { "event_ms": 1710000001000 }
}
```

Rules:

- `request_id`: required non-empty globally unique string.
- `prev_seq`: optional uint64; this protocol does not define its business
  semantics.
- `data.msg_type`: required non-empty string.
- `data.content`: required object.
- `request_id` is the protocol deduplication key for `send.request`; callers
  MUST NOT generate multiple different `request_id` values for the same business
  send action.
- `send.request` does not parse mentions.

### 4.3 `send.result`

`send.result` is the sync worker result for a `send.request`.

```json
{
  "kind": "send.result",
  "prev_seq": 345678,
  "request_id": "req_xxx",
  "data": {
    "status": "SUCCESS",
    "provider_message_id": "msg_xxx"
  },
  "timestamps": { "event_ms": 1710000001500 }
}
```

Rules:

- `prev_seq`: required uint64, equal to the corresponding `send.request.seq`.
- `request_id`: required non-empty string, equal to the corresponding
  `send.request.request_id`.
- One `send.request` can have only one successful `send.result`.
- `data.status`: required string, `SUCCESS` or `FAILED`.
- `data.provider_message_id`: SHOULD be set on `SUCCESS`.
- `data.error_code` and `data.error_message`: SHOULD be set on `FAILED`.

`FAILED` means the `send.request` has been terminated and must not be
automatically advanced again by the same worker. Retry, dead-letter, and manual
termination policies are declared by the concrete sync worker public document.

`send.result` expresses only the send action result. The provider-side real
message callback is still synchronized later as `sync.record`. If a worker can
identify that a `sync.record` is the real message corresponding to a successful
`send.result`, that `sync.record.prev_seq` MUST be set to the corresponding
`send.result.seq`.

## 5. Lifecycle Semantics

An IM sync worker receives messages according to channel visibility and processes
`send.request` messages within its responsibility scope.

Send and sync tasks within the same IM channel MUST advance serially. If there
is an unfinished `send.request` in the channel, the worker MUST retry or
terminate it according to the concrete worker document and MUST NOT continue
synchronizing later provider messages as new `sync.record` messages. Only after
the request has succeeded with a stable `provider_message_id`, or has been
terminated with a failure result, may the worker continue later sync state.

After restart or recovery, the worker MUST continue unfinished `send.request`
messages. Business callers do not need to, and should not, write another
`send.request` for the same business action.

If no `send.result` is observed after `REQUEST_RESULT_TIMEOUT_MS`, business
modules may only conclude that the send is still pending, retrying, or blocked
inside the IM sync worker. They should not submit a new `send.request` for the
same business action. Retry and failure termination are owned by the concrete IM
sync worker. The current protocol does not define manual cancellation or
dead-letter kinds.

## 6. Versioning

- `im.v1` only accepts backward-compatible extensions.
- Breaking changes must use a new channel protocol, such as `im.v2`.
- SDK major versions should match protocol major versions.
