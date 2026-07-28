# IM P2P Syncer Usage

[中文版](IM-P2P-SYNCER_cn.md)

> Version: v0.4
> Status: usable
> Scope: synchronize IM provider P2P direct-message sessions and OpenEvent
> `im.v1` channels in both directions.

The stable protocol specification is [`IM_PROTOCOL.md`](IM_PROTOCOL.md). This
document is for deployment and integration. It describes the public scope,
protocol constraints, configuration, and operational checks of the P2P sync
worker.

## 1. Scope

IM P2P Syncer is an independent process module that performs two-way sync
between IM provider direct-message sessions and OpenEvent channels:

One configured mapping pair binds one OpenEvent `im.v1` channel to one provider P2P
session. They are two representations of the same message stream. Synchronization
is scoped by the session and channel, not by sender direction: actual user and
bot/app messages in the provider session must all become `sync.record` events.

1. Writes provider direct-message records into OpenEvent as `sync.record`.
2. Listens for OpenEvent `send.request` and sends messages through the provider.
3. Writes send action results back into OpenEvent as `send.result`.
4. Continues synchronizing the provider's real message callback as ordinary
   `sync.record` events.

Current scope:

- Supports only `session_type="p2p"`.
- Does not support group chats, channels, group bot broadcast, or group member
  management.
- Synchronizes message records, not later edits, recalls, or other message-state
  changes.
- Does not parse mentions and does not use `recipients` to express mentions.
- OpenEvent channels are preconfigured; the worker does not create or modify
  channels automatically.

## 2. P2P Protocol Usage

Protocol fields, payload envelope, channel `description`, and common kind rules
are defined by [`IM_PROTOCOL.md`](IM_PROTOCOL.md). P2P worker scenario
constraints:

| Object | P2P Constraint |
| --- | --- |
| Processed channel | `protocol == "im.v1"`, `description.session_type == "p2p"`, present in mappings, private, and containing the user, bot, and sync worker principals |
| `send.request` | Consumed only; target session is determined by `channel_id -> (provider, session_id)`; the sender principal must match a mapping in the channel |
| `sync.record` | Published by the worker; OpenEvent `principal` is the mapped sender principal; `recipients` is the P2P peer principal |
| `send.result` | Published only after a successful provider send; OpenEvent `principal` is the worker principal and `recipients` is the original `send.request` principal |
| channel ACL | The channel must be private and include both P2P principals and the sync worker principal; ACL is the permission boundary, `recipients` is not |

Provider send failures do not require business callers to resubmit. Any failed
Provider response, send exception, missing provider message ID, or sender-mapping
failure immediately exits the worker without writing `send.result`. The original
stable `send.request` remains unfinished and is recovered after repair and
restart. The worker writes `send.result` only for success.

A send failure exits the worker and therefore pauses all of its configured
channels. Operators must repair the cause before restart and configure
process-manager backoff to avoid a rapid restart loop.

This fail-stop policy has no OpenEvent failure terminal: operators must use the
process exit and logs to diagnose the cause. A deterministic bad request is
retried after every restart until its configuration or payload is repaired;
there is no dead-letter or manual-skip mechanism in this worker.

If the Provider accepts a send but its response is lost, recovery reuses the
same request-derived Provider UUID. Avoiding duplicate delivery therefore
depends on the Provider honoring that idempotency key.

## 3. Mapping Model

### 3.1 Principal

Human-bot direct-message scenarios involve three principal types:

| Type | Usage |
| --- | --- |
| Business caller principal | Writes `send.request`; in P2P this must be the mapped bot principal in the channel; maps to the configured app/bot sending identity in Feishu/Lark outbound sends |
| Sync worker principal | Reads OpenEvent messages, queries channels, publishes `send.result` |
| Session participant principal | Provider user or bot mapped into OpenEvent; publishes `sync.record`; each P2P channel must contain one user principal and one bot principal |

`principal_tokens` manages OpenEvent tokens for session participant principals,
so the worker can publish `sync.record` as the user or bot. The worker
principal/token is configured separately under `worker` and is not included in
`principal_tokens`.

### 3.2 P2P Mapping

`mappings` is the P2P direct-message routing table. Each mapping describes one
provider external identity inside an OpenEvent channel and binds it to an
OpenEvent principal. `identity_type="user"` represents a human user;
`identity_type="bot"` represents an app/bot identity. A human-bot P2P channel
must have exactly two mappings: one user and one bot. Every provider and mapping
listed in the config is enabled; remove an entry from the config to disable it.

Field types:

| Field | Type |
| --- | --- |
| `provider` | string |
| `identity_type` | string, `user` or `bot`; defaults to `user` |
| `external_user_id` | string |
| `principal` | uint64 |
| `session_id` | string |
| `channel_id` | uint64 |

```yaml
mappings:
  - provider: lark
    identity_type: user
    external_user_id: ou_source
    principal: 10001
    session_id: oc_p2p_10001_bot
    channel_id: 10001
  - provider: lark
    identity_type: bot
    external_user_id: cli_xxx
    principal: 90002
    session_id: oc_p2p_10001_bot
    channel_id: 10001
```

Constraints:

- In the same `channel_id`, `(provider, identity_type, external_user_id)` must
  be unique.
- The same provider external identity may appear in multiple `channel_id`
  values.
- The same `(provider, session_id)` must map to exactly one `channel_id`.
- The same `channel_id` must map to exactly one `(provider, session_id)`.
- Each P2P channel must resolve to exactly two different participant
  principals, one `user` and one `bot`.
- For a Feishu/Lark bot mapping, `external_user_id` must equal the
  provider's `credentials.app_id`, so callback app/bot senders resolve to the
  configured bot principal.
- Outbound `send.request` OpenEvent `principal` must resolve to the bot
  mapping in the same channel, proving that the principal is the current bot
  participant.
- The provider message puller applies the same sender rules to history and
  subscription messages: user senders map to the user principal, and app/bot
  senders map to the bot principal. The processor does not distinguish their
  source. An unknown sender or malformed message fails the worker and must not
  be silently skipped or acknowledged.
- Channels absent from the mapping config must not be processed, even if visible
  to the sync worker principal.

## 4. Run

After `make install`, the wheel provides:

```bash
im-p2p-syncer --config /etc/openevent/im-sync-worker.yaml
```

The process stops the Lark WebSocket and attempts graceful shutdown on `SIGINT` or `SIGTERM`. Business callers
write `send.request` to OpenEvent; the worker consumes requests in P2P channels
and writes `send.result` after successful delivery. Provider real message callbacks are synchronized as
ordinary `sync.record` and do not trigger another provider send.

If an unfinished `send.request` exists in a P2P channel, the worker must deliver
it before advancing later provider sync for that channel. A fatal failure leaves
it unfinished for recovery after restart. Business callers should observe the
successful `send.result` and must not write another `send.request` for the same
business send action.

## 5. Feishu/Lark Support

Current provider support is `provider=feishu` and `provider=lark`, both limited
to `session_type="p2p"`. They use the same Lark OpenAPI adapter shape. The main
difference is the open platform domain and tenant region.

Field mapping:

| Common Field | Provider Source | Notes |
| --- | --- | --- |
| `provider` | `feishu` or `lark` | Must match config and channel description |
| `session_id` | provider direct-message `chat_id` | `mappings[].session_id` |
| `provider_message_id` | provider `message_id` | Used for inbound sync, history fetch, and successful send response |
| `sender_external_user_id` | provider sender ID | Must match `mappings[].identity_type + mappings[].external_user_id` by sender type |
| `msg_type` | provider `msg_type` | Baseline implementation supports at least `text` |
| `content_raw` | provider raw message/event object or content object | Preserved for fidelity and extension |
| `text` | text message content `text` | Set only for text messages |
| `event_ms` | provider message creation time | Use provider milliseconds directly or convert to Unix epoch milliseconds |

Feishu/Lark constraints:

- The runtime dependency is pinned to `lark-oapi==1.6.4` because WebSocket
  shutdown compatibility depends on that SDK version's runtime behavior.
- Use `providers[].credentials.app_id` and `providers[].credentials.app_secret`
  to initialize the official SDK client.
- API endpoint uses `providers[].options.api_base_url`; Feishu usually uses
  `https://open.feishu.cn`, and Lark usually uses
  `https://open.larksuite.com`.
- Bot capability and the `im.message.receive_v1` event subscription must be
  enabled. Real-time inbound messages use the official SDK WebSocket and do not
  require a public Webhook endpoint.
- One worker supports one Feishu/Lark provider. `providers[].name` selects both
  the provider identity and adapter type and must be `feishu` or `lark`. Multiple mapped P2P
  `chat_id` values for the same app share that WebSocket.
- Outbound sends support at least `msg_type="text"` with
  `content={"text": "..."}`. The platform does not send as arbitrary user
  `open_id`; the worker must first verify that `send.request.principal` belongs
  to the bot mapping in the P2P channel, then send to the direct-message
  `chat_id` using the configured app/bot identity.
- The app/bot identity is configured by `providers[].credentials` and described
  in `mappings[]` with `identity_type="bot"`; its `external_user_id` must equal
  `credentials.app_id`. The user mapping's `external_user_id` is the human
  user's `open_id`. Both `im.message.receive_v1` events and `message.list`
  history map user and app/bot senders to their respective principals and publish
  `sync.record`; neither path filters by sender direction.
- A successful `message.create` carrying `message_id` is accepted directly; the
  worker does not issue an additional `message.get` confirmation request.
- Any failed `message.create` response or exception leaves the request unfinished
  and terminates the worker immediately.

The provider message puller wraps the `im.message.receive_v1` subscription and
`message.list` handoff into one acknowledged message stream. The SDK callback
validates subscription events and writes them to a bounded queue. A full queue
or malformed mapped message fails the callback and terminates the worker for a
process-manager restart. Mapping, idempotency, business state, and OpenEvent
publication remain single-threaded.

Only the message puller uses `message.list`, at initial startup and after a
WebSocket reconnect. A stable connection does not run periodic history scans.
The puller first establishes the WebSocket and buffers subscription events,
then completes a fixed history window for each session starting at its confirmed
`event_ms` minus `history_overlap_ms`. With no confirmed message it uses
`history_lookback_ms`. On restart, confirmed event times and the message-ID
dedup set are rebuilt from OpenEvent `sync.record` history. An explicitly
temporary history-query failure preserves that session's query window and page
token and retries after `history_retry_delay_ms`; permanent, malformed, and
unclassified failures terminate the worker.

The puller exposes at most one unacknowledged message per session. It does not
deliver buffered subscription events for a session until that session's handoff
window and already delivered history messages are complete, while other
sessions may continue. The processor sees ordinary provider messages only; it
has no repair or stable state. History and subscription messages enter the same
queue and follow the same per-channel serialization. An unfinished
`send.request` blocks provider messages only in its own channel, while other
channels continue inbound and outbound work. The processor acknowledges a
provider message to the puller only after publication succeeds or idempotent
state proves it was already processed.

OpenEvent `Fetch` retries only `UNAVAILABLE` and `DEADLINE_EXCEEDED`; other
Fetch errors terminate the worker.

Provider `message_id` is the idempotency and send-result association key.
`page_token` is scoped to one history query; no timestamp, page token, or
message ID is treated as a provider sequence. Overlapping history queries
reduce delayed visibility loss, but Lark publishes no maximum visibility delay,
so they cannot guarantee recovery from an arbitrarily delayed message.

OpenEvent `seq` is the order in which records are durably written, not provider
event time. An older message returned by the history API may be written at a
larger `seq`; consumers must use `timestamps.event_ms` for message business time.

## 6. Reliability and Fail-Stop Behavior

| Scenario | Strategy |
| --- | --- |
| Inbound provider sender has no P2P mapping | Do not publish; fail the worker and restart it after config repair |
| Mapped provider message is malformed | Fail the worker; do not acknowledge or skip the message |
| Conflicting `send.request` content for one `request_id`, or mismatched `send.result.prev_seq` | Fail the worker; do not restore from partial state or resend the request |
| Principal token missing | Do not publish; log error and wait for config repair |
| Channel protocol, description, private visibility, or membership mismatch | Startup failure |
| Lark WebSocket startup failure or permanent exit | Exit the worker for process-manager restart; never silently degrade to history polling only |
| Real-time event queue full or callback failure | Fail the callback and terminate the worker; the message puller repeats connection handoff after process restart |
| Explicit temporary history-query failure inside the message puller | Preserve that session's query window and page token and retry after `history_retry_delay_ms`; do not introduce a global repair phase or block other channels |
| Permanent, malformed, or unclassified history-query failure inside the message puller | Do not acknowledge related messages; exit the worker |
| `send.request.principal` missing from mapping | Leave the request unfinished and exit the worker without writing `send.result` |
| Provider send failure | Leave the request unfinished and exit immediately without writing `send.result` |
| OpenEvent publish is proven not committed and the underlying status is `UNAVAILABLE` or `DEADLINE_EXCEEDED` | Retry at the fixed `retry.publish_retry_delay_ms` interval; exit after `retry.publish_max_attempts` |
| OpenEvent publish outcome remains unknown after reconciliation | Do not republish; exit immediately to avoid a duplicate |
| OpenEvent rejects full `sync.record` as too large | Rewrite and publish degraded `sync.record` with lightweight metadata and `message_too_large` fields |
| OpenEvent still rejects the degraded `sync.record` as too large | Do not trim, retry, or acknowledge the provider message; fail the worker |

## 7. Configuration

All runtime parameters are read from a YAML config file. The worker does not
depend on environment variables. See `p2p_config.yaml` for a sample.

### 7.1 Example

The example below shows required config for a Lark P2P channel. Replace the
OpenEvent endpoint, principals, tokens, Lark credentials, Lark `open_id/chat_id`,
and OpenEvent `channel_id`.

```yaml
version: v1

worker:
  principal: 90001
  token: tok-syncer-90001
  shutdown_timeout_ms: 10000

openevent:
  target: 127.0.0.1:9527

principal_tokens:
  - principal: 10001
    token: tok-user-10001
  - principal: 90002
    token: tok-bot-90002

providers:
  - name: lark
    sync:
      history_retry_delay_ms: 1000
      history_overlap_ms: 300000
      history_lookback_ms: 300000
      page_size: 50
      event_queue_size: 1000
    credentials:
      app_id: cli_xxx
      app_secret: app-secret-xxx
    options:
      api_base_url: https://open.larksuite.com
      timeout_seconds: 10
      event_connect_timeout_seconds: 30
      event_reconnect_timeout_seconds: 300

mappings:
  - provider: lark
    identity_type: user
    external_user_id: ou_source
    principal: 10001
    session_id: oc_p2p_10001_bot
    channel_id: 10001
  - provider: lark
    identity_type: bot
    external_user_id: cli_xxx
    principal: 90002
    session_id: oc_p2p_10001_bot
    channel_id: 10001
```

### 7.2 Fields and Validation

| Field | Required | Notes |
| --- | --- | --- |
| `version` | yes | string, config schema version, must be `v1` |
| `worker.principal` | yes | worker OpenEvent principal, uint64 > 0 |
| `worker.token` | yes | worker OpenEvent token, non-empty string |
| `worker.shutdown_timeout_ms` | no | maximum graceful shutdown wait after signal, default `10000`; timeout exits unsuccessfully |
| `openevent.target` | yes | public OpenEvent gRPC endpoint |
| `retry.publish_max_attempts` | no | OpenEvent write retry limit, default `5` |
| `retry.publish_retry_delay_ms` | no | fixed retry delay when a publish is proven not committed and its underlying status is `UNAVAILABLE` or `DEADLINE_EXCEEDED`, default `200`, non-negative |
| `retry.idle_sleep_ms` | no | main-loop idle sleep, default `200` |
| `logging.*` | no | basic logging configuration |
| `principal_tokens[]` | yes | user/bot OpenEvent principal tokens; excludes `worker.principal` |
| `providers[]` | yes | exactly one provider; every listed provider is enabled |
| `providers[].name` | yes | provider identity and adapter type; `feishu` or `lark` |
| `providers[].sync.history_retry_delay_ms` | no | fixed retry delay after an explicitly temporary startup/reconnect history query in the message puller, default `1000`, non-negative; it does not schedule stable-connection scans |
| `providers[].sync.history_overlap_ms` | no | message-puller handoff overlap before the confirmed `event_ms`, default `300000`, non-negative |
| `providers[].sync.history_lookback_ms` | no | initial message-puller handoff lookback with no confirmed provider message, default `300000`, positive |
| `providers[].sync.page_size` | no | history page size, default `50`, range `1..50` |
| `providers[].sync.event_queue_size` | no | bounded real-time event queue size, default `1000`, positive |
| `providers[].credentials` | yes | Feishu/Lark require non-empty `app_id` and `app_secret` |
| `providers[].options` | no | `api_base_url` is the REST/WebSocket platform domain; `timeout_seconds` is the REST timeout; `event_connect_timeout_seconds` is the WebSocket startup timeout; `event_reconnect_timeout_seconds` is the continuous-disconnect limit before worker failure, default `300` seconds; configured timeouts must be positive |
| `mappings[]` | yes | non-empty P2P mappings; every entry is enabled and must satisfy uniqueness and user+bot constraints |

All `*_ms` fields are duration values in milliseconds, not Unix timestamps.

At startup the worker also queries OpenEvent channels and validates that
`protocol`, `description.provider`, `description.session_id`,
`description.session_type`, and mappings are consistent.

## 8. Operations

Important log fields:

- `provider`
- `session_id`
- `channel_id`
- `provider_message_id`
- `request_id`
- `principal`
- `openevent_seq`
- `kind`
- `error_code`

## 9. Deployment Checklist

1. `worker.principal` and `worker.token` are set.
2. `principal_tokens[].principal` has no duplicates, and each `channel_id` has no duplicate `(provider, identity_type, external_user_id)` mapping.
3. Each `(provider, session_id)` maps to only one `channel_id`.
4. Each `channel_id` maps to only one `(provider, session_id)`.
5. Every channel has `protocol == im.v1`.
6. Every channel has `description.session_type == p2p`.
7. Description `provider/session_id` values match the mapping.
8. Every P2P channel resolves to two distinct participant principals, one user and one bot.
9. Every channel is private and contains the user, bot, and sync worker principals.
10. Every Feishu/Lark bot mapping uses its Provider `credentials.app_id` as `external_user_id`.
11. Every participant principal referenced by a mapping has a token.
12. The sync worker principal can read and write the channel.
13. The Lark app subscribes to `im.message.receive_v1`, and history-read permissions cover connection handoff.
14. The process manager restarts the worker after a permanent WebSocket or fatal send failure.
15. Restart backoff and alerts are configured so a permanent bad request or configuration error does not cause a rapid restart loop.
