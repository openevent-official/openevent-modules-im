# IM P2P Syncer Usage

[中文版](IM-P2P-SYNCER_cn.md)

> Version: v0.3
> Status: usable
> Scope: synchronize IM provider P2P direct-message sessions into OpenEvent
> `im.v1` channels.

The stable protocol specification is [`IM_PROTOCOL.md`](IM_PROTOCOL.md). This
document is for deployment and integration. It describes the public scope,
protocol constraints, configuration, and operational checks of the P2P sync
worker.

## 1. Scope

IM P2P Syncer is an independent process module that performs two-way sync
between IM provider direct-message sessions and OpenEvent channels:

1. Writes provider direct-message records into OpenEvent as `sync.record`.
2. Listens for OpenEvent `send.request` and sends messages through the provider.
3. Writes send action results back into OpenEvent as `send.result`.
4. Continues synchronizing the provider's real message callback as ordinary
   `sync.record` events.

Current scope:

- Supports only `session_type="p2p"`.
- Does not support group chats, channels, group bot broadcast, or group member
  management.
- Does not parse mentions and does not use `recipients` to express mentions.
- OpenEvent channels are preconfigured; the worker does not create or modify
  channels automatically.

## 2. P2P Protocol Usage

Protocol fields, payload envelope, channel `description`, and common kind rules
are defined by [`IM_PROTOCOL.md`](IM_PROTOCOL.md). P2P worker scenario
constraints:

| Object | P2P Constraint |
| --- | --- |
| Processed channel | `protocol == "im.v1"`, `description.session_type == "p2p"`, and present in active mappings |
| `send.request` | Consumed only; target session is determined by `channel_id -> (provider, session_id)`; the sender principal must match an active mapping in the channel |
| `sync.record` | Published by the worker; OpenEvent `principal` is the mapped sender principal; `recipients` is the P2P peer principal |
| `send.result` | Published by the worker; OpenEvent `principal` is the worker principal; `recipients` is the original `send.request` principal |
| channel ACL | Must include both P2P principals and the sync worker principal; ACL is the permission boundary, `recipients` is not |

Provider send failures do not require business callers to resubmit. The worker
retries the current `send.request` within the channel until provider send
succeeds and a `send.result(status=SUCCESS)` is written, or until
`retry.provider_send_max_attempts` is reached. After the retry limit, the worker
writes `send.result(status=FAILED, error_code=PROVIDER_SEND_FAILED)` and clears
the request. If `send.request.principal` is missing from active mappings in the
channel, the worker writes `send.result(status=FAILED, error_code=MAPPING_MISSING)`
and terminates the request.

Provider failures block that P2P channel until success or final failure.
Credential expiration, missing provider permission, unavailable target sessions,
or long-term rate limits eventually produce final failure results after the
retry limit. Business callers must observe `send.result` and decide whether to
repair manually, create a new business action, or keep the failure state.

## 3. Mapping Model

### 3.1 Principal

Human-bot direct-message scenarios involve three principal types:

| Type | Usage |
| --- | --- |
| Business caller principal | Writes `send.request`; in P2P this must be the active bot principal in the channel; maps to the configured app/bot sending identity in Feishu/Lark outbound sends |
| Sync worker principal | Reads OpenEvent messages, queries channels, publishes `send.result` |
| Session participant principal | Provider user or bot mapped into OpenEvent; publishes `sync.record`; each active P2P channel must contain one user principal and one bot principal |

`principal_tokens` manages OpenEvent tokens for session participant principals,
so the worker can publish `sync.record` as the user or bot. The worker
principal/token is configured separately under `worker` and is not included in
`principal_tokens`.

### 3.2 P2P Mapping

`mappings` is the P2P direct-message routing table. Each mapping describes one
provider external identity inside an OpenEvent channel and binds it to an
OpenEvent principal. `identity_type="user"` represents a human user;
`identity_type="bot"` represents an app/bot identity. A human-bot P2P channel
must have exactly two active mappings: one user and one bot.

Field types:

| Field | Type |
| --- | --- |
| `provider` | string |
| `identity_type` | string, `user` or `bot`; defaults to `user` |
| `external_user_id` | string |
| `principal` | uint64 |
| `session_id` | string |
| `channel_id` | uint64 |
| `status` | string, currently `active` |

```yaml
mappings:
  - provider: lark
    identity_type: user
    external_user_id: ou_source
    principal: 10001
    session_id: oc_p2p_10001_bot
    channel_id: 10001
    status: active
  - provider: lark
    identity_type: bot
    external_user_id: cli_xxx
    principal: 90002
    session_id: oc_p2p_10001_bot
    channel_id: 10001
    status: active
```

Constraints:

- Mappings with `status != "active"` are inactive.
- In the same `channel_id`, `(provider, identity_type, external_user_id)` must
  be unique.
- The same provider external identity may appear in multiple `channel_id`
  values.
- The same `(provider, session_id)` must map to exactly one `channel_id`.
- The same `channel_id` must map to exactly one `(provider, session_id)`.
- Each active P2P channel must resolve to exactly two different participant
  principals, one `user` and one `bot`.
- Outbound `send.request` OpenEvent `principal` must resolve to an active bot
  mapping in the same channel, proving that the principal is the current bot
  participant.
- Inbound message senders must map by provider sender type to the corresponding
  `identity_type`; if mapping fails, `sync.record` must not be published.
- Channels absent from the mapping config must not be processed, even if visible
  to the sync worker principal.

## 4. Run

After `make install`, the wheel provides:

```bash
im-p2p-syncer --config /etc/openevent/im-sync-worker.yaml
```

The process attempts graceful shutdown on `SIGINT` or `SIGTERM`. Business callers
write `send.request` to OpenEvent; the worker consumes requests in P2P channels
and writes `send.result`. Provider real message callbacks are synchronized as
ordinary `sync.record` and do not trigger another provider send.

If an unfinished `send.request` exists in a P2P channel, the worker processes or
terminates it before advancing later provider sync for that channel. Business
callers should observe `send.result` and should not write another
`send.request` for the same business send action.

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

- Use `providers[].credentials.app_id` and `providers[].credentials.app_secret`
  to initialize the official SDK client.
- API endpoint uses `providers[].options.api_base_url`; Feishu usually uses
  `https://open.feishu.cn`, and Lark usually uses
  `https://open.larksuite.com`.
- Outbound sends support at least `msg_type="text"` with
  `content={"text": "..."}`. The platform does not send as arbitrary user
  `open_id`; the worker must first verify that `send.request.principal` belongs
  to the active bot mapping in the P2P channel, then send to the direct-message
  `chat_id` using the configured app/bot identity.
- The app/bot identity is configured by `providers[].credentials` and described
  in `mappings[]` with `identity_type="bot"`. `identity_type="user"`
  `external_user_id` is the human user's `open_id`. Callback messages are mapped
  by sender type to user or bot principal and then published as `sync.record`.
- Invalid credentials, missing permissions, missing `session_id`, and other
  long-term failures are retried until `retry.provider_send_max_attempts`, then
  written as a final failure result and alerted.

Current sync mode is `providers[].sync.mode = "poll"`, using
`providers[].sync.interval_ms` to poll provider direct-message history. Provider
`message_id` is the inbound idempotency key and send-result association key; it
should not be treated as an official incremental cursor.

## 6. Reliability and Failure Results

| Scenario | Strategy |
| --- | --- |
| P2P mapping missing | Do not publish or process; log error and wait for config repair |
| Principal token missing | Do not publish; log error and wait for config repair |
| Channel protocol or description mismatch | Startup failure |
| `send.request.principal` missing from active mapping | Write `send.result(status=FAILED, error_code=MAPPING_MISSING)` and terminate request |
| Provider send failure | Count attempts; after `retry.provider_send_max_attempts`, write `send.result(status=FAILED, error_code=PROVIDER_SEND_FAILED)` and terminate request |
| OpenEvent write failure | Retry according to `retry.publish_*`; exit after retry limit and wait for process manager or manual repair |
| OpenEvent rejects full `sync.record` as too large | Rewrite and publish degraded `sync.record` with lightweight metadata and `message_too_large` fields |

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
  name: im-sync-p2p-lark
  principal: 90001
  token: tok-syncer-90001

openevent:
  target: 127.0.0.1:9527

principal_tokens:
  - principal: 10001
    token: tok-user-10001
  - principal: 90002
    token: tok-bot-90002

providers:
  - name: lark
    adapter: lark
    sync:
      mode: poll
    credentials:
      app_id: cli_xxx
      app_secret: app-secret-xxx
    options:
      api_base_url: https://open.larksuite.com

mappings:
  - provider: lark
    identity_type: user
    external_user_id: ou_source
    principal: 10001
    session_id: oc_p2p_10001_bot
    channel_id: 10001
    status: active
  - provider: lark
    identity_type: bot
    external_user_id: cli_xxx
    principal: 90002
    session_id: oc_p2p_10001_bot
    channel_id: 10001
    status: active
```

### 7.2 Fields and Validation

| Field | Required | Notes |
| --- | --- | --- |
| `version` | yes | string, config schema version, must be `v1` |
| `worker.name` | yes | worker instance name for logs, metrics, and troubleshooting |
| `worker.principal` | yes | worker OpenEvent principal, uint64 > 0 |
| `worker.token` | yes | worker OpenEvent token, non-empty string |
| `worker.request_result_timeout_ms` | no | recommended business observation timeout for `send.request -> send.result`, default `60000` |
| `worker.shutdown_timeout_ms` | no | graceful shutdown wait after signal, default `10000` |
| `openevent.target` | yes | public OpenEvent gRPC endpoint |
| `retry.publish_max_attempts` | no | OpenEvent write retry limit, default `5` |
| `retry.publish_initial_backoff_ms` | no | initial publish retry backoff, default `200` |
| `retry.publish_max_backoff_ms` | no | maximum publish retry backoff, default `5000` |
| `retry.provider_send_max_attempts` | no | provider send retry limit per request, default `5` |
| `retry.idle_sleep_ms` | no | idle polling sleep, default `200` |
| `logging.*` | no | basic logging configuration |
| `principal_tokens[]` | yes | user/bot OpenEvent principal tokens; excludes `worker.principal` |
| `providers[]` | yes | provider adapter, sync parameters, credentials, and options |
| `providers[].enabled` | no | bool, default `true` |
| `providers[].adapter` | yes | adapter type |
| `providers[].sync.mode` | yes | Feishu/Lark must use `poll` |
| `providers[].sync.interval_ms` | no | polling interval in milliseconds |
| `providers[].sync.page_size` | no | provider page size |
| `providers[].sync.startup_lookback_ms` | no | startup lookback window in milliseconds |
| `providers[].credentials` | yes | Feishu/Lark require non-empty `app_id` and `app_secret` |
| `providers[].options` | no | provider options; `api_base_url`, if set, must be non-empty |
| `mappings[]` | yes | P2P mappings; active mappings must satisfy uniqueness and user+bot constraints |

All `*_ms` fields are duration values in milliseconds, not Unix timestamps.

At startup the worker also queries OpenEvent channels and validates that
`protocol`, `description.provider`, `description.session_id`,
`description.session_type`, and active mappings are consistent.

## 8. Operations

Important log fields:

- `provider`
- `session_id`
- `channel_id`
- `request_id`
- `provider_message_id`
- `principal`
- `seq`

Deployment checklist:

- OpenEvent is reachable.
- Every configured principal has a valid token.
- Every active mapping references an existing `im.v1` P2P channel.
- Channel ACL contains user, bot, and sync worker principals.
- Provider credentials have permissions to read and send direct messages.
- Runtime logs and process manager restarts are configured.
