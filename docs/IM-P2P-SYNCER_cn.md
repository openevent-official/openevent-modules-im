# IM P2P Syncer 使用说明

[English version](IM-P2P-SYNCER.md)

> 版本：v0.3  
> 状态：可用  
> 适用范围：把 IM Provider 的 P2P 单聊会话同步到 OpenEvent `im.v1` channel。

稳定协议规格见 [`IM_PROTOCOL_cn.md`](IM_PROTOCOL_cn.md)。本文面向部署和集成使用，描述 P2P Sync Worker 的公开使用范围、协议约束、配置方式和运维检查。

## 1. 使用范围

IM P2P Syncer 是独立进程模块，在 IM 平台单聊会话和 OpenEvent channel 之间做双向同步：

1. 将 IM 平台单聊消息写入 OpenEvent `sync.record`
2. 监听 OpenEvent `send.request`，调用对应 IM 平台发送单聊消息
3. 将发送动作结果写回 OpenEvent `send.result`
4. 后续把 Provider 真实消息回流继续同步为普通 `sync.record`

当前范围：

- 只支持 `session_type="p2p"`
- 不支持群聊、频道、群机器人群发、群成员管理
- 不解析 `@`，不使用 `recipients` 表达提及
- OpenEvent channel 预配置创建，Worker 不自动创建或修改 channel

## 2. P2P 协议用法

协议字段、payload envelope、channel `description` 和各 kind 的通用规则统一见
[`IM_PROTOCOL_cn.md`](IM_PROTOCOL_cn.md)。P2P Worker 的场景约束如下：

| 对象 | P2P 约束 |
| --- | --- |
| 可处理 channel | `protocol == "im.v1"`，`description.session_type == "p2p"`，且在 active mapping 中 |
| `send.request` | 只消费，不发布；目标会话由 `channel_id -> (provider, session_id)` 决定；OpenEvent 发送方由 `send.request.principal` 在该 channel 的 active mapping 中校验 |
| `sync.record` | 由 Worker 发布；OpenEvent `principal` 为发送者映射 principal，`recipients` 为单聊对端 principal |
| `send.result` | 由 Worker 发布；OpenEvent `principal` 为 Worker principal，`recipients` 为对应 `send.request` 发起 principal |
| channel ACL | 必须覆盖单聊双方 principal 与 Sync Worker principal；ACL 是权限边界，`recipients` 不是权限边界 |

Provider 发送失败不要求业务方重发。Worker 在对应 channel 内重试当前
`send.request`，直到 Provider 发送成功并写出 `send.result(status=SUCCESS)`，或
同一 request 的 Provider 发送尝试次数达到 `retry.provider_send_max_attempts`。超过上限后，
Worker 写出 `send.result(status=FAILED, error_code=PROVIDER_SEND_FAILED)`，清除该 request，
后续不再自动处理它；若 `send.request.principal` 不在该 channel 的 active mapping 内，
Worker 写出 `send.result(status=FAILED, error_code=MAPPING_MISSING)` 并终止该 request。

这意味着 Provider 失败在达到上限前会阻塞该 P2P channel。凭据失效、Provider 权限缺失、
目标会话不可用、限流长期未恢复等情况会在超过上限后形成最终失败结果；业务方需要观察
`send.result` 并按业务语义决定是否人工修复、创建新的业务动作或保持失败状态。

## 3. 映射模型

### 3.1 Principal

人机单聊场景涉及三类 principal：

| 类型 | 用途 |
| --- | --- |
| 业务调用 principal | 写入 `send.request`；P2P 中必须同时是该 channel active mapping 内的 bot principal；在 Feishu/Lark 出站中映射为配置的应用/机器人发送身份 |
| Sync Worker principal | 读取 OpenEvent 消息、查询 channel、发布 `send.result` |
| 会话参与方 principal | Provider 用户或机器人映射到 OpenEvent 后的身份，用于发布 `sync.record`；每个 active P2P channel 必须包含一个 user principal 和一个 bot principal |

`principal_tokens` 管理会话参与方 principal 的 OpenEvent token，供 Worker 以用户或机器人身份发布
`sync.record`。Worker principal/token 独立放在 `worker` 配置中，不放入 `principal_tokens`。

### 3.2 P2P Mapping

`mappings` 是 P2P 单聊路由表。每条 mapping 描述某个 OpenEvent channel 内的一个
Provider 外部身份，并绑定到对应 OpenEvent principal。`identity_type="user"` 表示
人类用户；`identity_type="bot"` 表示应用/机器人身份。同一个人机单聊 channel
必须正好有两条 active mapping：一条 user，一条 bot。

字段类型：

| 字段 | 类型 |
| --- | --- |
| `provider` | string |
| `identity_type` | string，`user` 或 `bot`，缺省按 `user` 处理 |
| `external_user_id` | string |
| `principal` | uint64 |
| `session_id` | string |
| `channel_id` | uint64 |
| `status` | string，当前有效值为 `active` |

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

约束：

- `status != "active"` 的映射不生效
- 同一个 `channel_id` 内，`(provider, identity_type, external_user_id)` 必须唯一
- 同一个 Provider 外部身份可以出现在多个不同 `channel_id` 中
- 同一个 `(provider, session_id)` 必须只对应一个 `channel_id`
- 同一个 `channel_id` 必须只对应一个 `(provider, session_id)`
- 同一 active p2p channel 必须正好能推导出两个不同的参与方 principal，并且身份类型必须是一条 `user`、一条 `bot`
- 出站 `send.request` 的 OpenEvent `principal` 必须能在同一 `channel_id` 的 active bot mapping 中反查到 Provider `external_user_id`，用于证明该 principal 是当前机器人参与方
- 入站消息发送者必须按 Provider sender type 映射到对应 `identity_type`；无法映射时，禁止发布 `sync.record`
- 不在映射配置内的 channel，即使 Sync Worker principal 可见，也不得处理

## 4. 运行方式

通过 `make install` 安装生成的 wheel 后提供命令入口：

```bash
im-p2p-syncer --config /etc/openevent/im-sync-worker.yaml
```

进程收到 `SIGINT` 或 `SIGTERM` 时会尝试优雅退出。业务方通过 OpenEvent 写入 `send.request`，Worker 负责在 P2P channel 内消费请求并回写 `send.result`；Provider 侧真实消息回流仍同步为普通 `sync.record`，不会触发新的 Provider 发送。

同一个 P2P channel 内有未完成 `send.request` 时，Worker 会先处理或终止该请求，再继续推进该 channel 的后续同步。业务方应观察 `send.result`，不要为同一业务发送动作重复写入新的 `send.request`。

## 5. Feishu/Lark 支持

当前 Provider 支持 `provider=feishu` 和 `provider=lark`，只支持 `session_type="p2p"`。
两者使用同一套 Lark OpenAPI 适配器，差异主要是开放平台域名和应用所属租户区域。

字段映射：

| 通用字段 | Provider 来源 | 说明 |
| --- | --- | --- |
| `provider` | `feishu` 或 `lark` | 必须与配置、channel description 一致 |
| `session_id` | string，Provider 单聊 `chat_id` | 即 `mappings[].session_id` |
| `provider_message_id` | string，Provider `message_id` | 入站、历史拉取和发送成功响应均使用该字段 |
| `sender_external_user_id` | string，Provider 发送者 ID | 必须能按发送者类型匹配 `mappings[].identity_type + mappings[].external_user_id` |
| `msg_type` | string，Provider `msg_type` | 基础实现至少支持 `text` |
| `content_raw` | object，Provider 原始 message/event 对象或消息内容子对象 | 用于保真保存和后续扩展 |
| `text` | string，文本消息 content 中的 `text` | 仅文本消息填写 |
| `event_ms` | integer，Provider 消息创建时间 | 若 Provider 返回毫秒时间戳则直接使用，否则转换为 Unix epoch 毫秒 |

Feishu/Lark 约束：

- 使用 `providers[].credentials.app_id` 与 `providers[].credentials.app_secret` 初始化官方 SDK client
- API endpoint 使用 `providers[].options.api_base_url`；`feishu` 通常为 `https://open.feishu.cn`，`lark` 通常为 `https://open.larksuite.com`
- 出站发送基础实现至少支持 `msg_type="text"`，`content={"text": "..."}`。平台不按普通用户 `open_id` 代发；Worker 必须先校验 `send.request.principal` 属于该 P2P channel 的 bot mapping，再使用配置中的应用/机器人身份，由该应用/机器人向 `session_id` 对应的单聊 `chat_id` 发送消息
- 应用/机器人身份由 `providers[].credentials` 指定，并在 `mappings[]` 中用 `identity_type="bot"` 显式描述；`identity_type="user"` 的 `external_user_id` 表示人类用户的 `open_id`。回流消息按 sender type 分别映射到 user 或 bot principal 后发布 `sync.record`
- 凭据无效、权限缺失、session_id 不存在等长期失败会持续重试到 `retry.provider_send_max_attempts`，超过上限后由 Worker 写出最终失败结果并告警

当前同步模式为 `providers[].sync.mode = "poll"`，按 `providers[].sync.interval_ms` 周期同步 Provider 单聊消息。Provider `message_id` 是入站幂等键和发送结果关联键，不应被当作官方增量游标。

## 6. 可靠性与失败结果

| 场景 | 策略 |
| --- | --- |
| p2p mapping 缺失 | 禁止发布或处理，记录错误，等待配置修复 |
| principal token 缺失 | 禁止发布，记录错误，等待配置修复 |
| channel protocol 或 description 不匹配 | 启动失败 |
| `send.request.principal` 不在 active mapping 内 | 写出 `send.result(status=FAILED, error_code=MAPPING_MISSING)` 并结束该 request |
| Provider 发送失败 | 累计同一 request 的发送尝试次数；达到 `retry.provider_send_max_attempts` 后写出 `send.result(status=FAILED, error_code=PROVIDER_SEND_FAILED)` 并结束该 request |
| OpenEvent 写入失败 | 按 `retry.publish_*` 重试，超过上限后退出进程，等待进程管理器重启或人工排查 |
| OpenEvent 因 payload 超限拒绝完整 `sync.record` | 改写并发布超大消息降级 `sync.record`，只记录 `provider_message_id`、消息类型、时间、发送者等轻量 meta，并按 [`IM_PROTOCOL_cn.md`](IM_PROTOCOL_cn.md) 写入 `message_too_large` 降级字段 |

## 7. 配置

Sync Worker 所有运行参数必须从 YAML 配置文件读取，不依赖环境变量。样例见 `p2p_config.yaml`，配置结构如下。

### 7.1 配置示例

下面示例只展示 Lark P2P 单聊 channel 的必填配置。实际部署时需要替换
OpenEvent endpoint、principal、token、Lark 应用凭据、Lark `open_id/chat_id` 和
OpenEvent `channel_id`。

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

### 7.2 字段与校验

| 字段 | 必填 | 说明与校验 |
| --- | --- | --- |
| `version` | 是 | string，配置 schema 版本，必须为 `v1` |
| `worker.name` | 是 | string，Worker 实例名，用于日志、指标和排障 |
| `worker.principal` | 是 | uint64，Sync Worker 自身 OpenEvent principal，必须大于 0 |
| `worker.token` | 是 | string，Sync Worker 自身 OpenEvent token，必须非空 |
| `worker.request_result_timeout_ms` | 否 | integer，业务观察 `send.request` 到 `send.result` 的建议超时时间，默认 `60000`，必须大于 0 |
| `worker.shutdown_timeout_ms` | 否 | integer，收到退出信号后的优雅关闭等待时间，默认 `10000`，必须大于等于 0 |
| `openevent.target` | 是 | string，OpenEvent 公共 gRPC endpoint，必须非空 |
| `retry.publish_max_attempts` | 否 | integer，OpenEvent 写入失败最大尝试次数，默认 `5`，必须大于 0 |
| `retry.publish_initial_backoff_ms` | 否 | integer，OpenEvent 写入失败初始退避毫秒数，默认 `200`，必须大于等于 0 |
| `retry.publish_max_backoff_ms` | 否 | integer，OpenEvent 写入失败最大退避毫秒数，默认 `5000`，必须大于等于 0 |
| `retry.provider_send_max_attempts` | 否 | integer，单条 `send.request` Provider 发送失败最大尝试次数，默认 `5`，必须大于 0 |
| `retry.idle_sleep_ms` | 否 | integer，空闲轮询休眠，默认 `200`，必须大于等于 0 |
| `logging.*` | 否 | object，日志级别等基础配置 |
| `principal_tokens[]` | 是 | array，用户 OpenEvent principal token 数组；`principal` 为 uint64 且唯一，`token` 为非空 string，且不得包含 `worker.principal` |
| `providers[]` | 是 | array，Provider Adapter、同步参数、凭据与 options；`name` 为非空 string 且唯一 |
| `providers[].enabled` | 否 | bool，缺省为 `true` |
| `providers[].adapter` | 是 | string，Adapter 类型 |
| `providers[].sync.mode` | 是 | string，Feishu/Lark provider 必须为 `poll` |
| `providers[].sync.interval_ms` | 否 | integer，轮询间隔毫秒，必须大于 0 |
| `providers[].sync.page_size` | 否 | integer，Provider 分页大小，必须大于 0 |
| `providers[].sync.startup_lookback_ms` | 否 | integer，启动时 Provider 拉取回看窗口毫秒数，必须大于等于 0 |
| `providers[].credentials` | 是 | object，Feishu/Lark provider 必须包含非空 string `app_id` 与 `app_secret` |
| `providers[].options` | 否 | object，Provider 选项；`api_base_url` 若填写必须为非空 string |
| `mappings[]` | 是 | array，P2P 单聊映射数组；字段类型见 3.2，active mapping 必须满足唯一性和 user+bot 双方约束 |

所有 `*_ms` 配置字段均为毫秒 duration，不是 Unix 时间戳。

启动时还必须反查 OpenEvent channel 并校验 `protocol`、`description.provider`、
`description.session_id`、`description.session_type` 与 active mapping 一致。

## 8. 运维

关键日志字段：

- `provider`
- `session_id`
- `channel_id`
- `provider_message_id`
- `request_id`
- `principal`
- `openevent_seq`
- `kind`
- `error_code`

关键指标：

- `im_inbound_events_total`
- `im_sync_record_published_total`
- `im_send_request_consumed_total`
- `im_send_result_published_total`
- `im_provider_send_fail_total`
- `im_mapping_missing_total`
- `im_channel_validation_fail_total`
- `im_end_to_end_latency_ms`

## 9. 上线检查

1. `worker.principal` 与 `worker.token` 完整
2. 配置文件无重复 `principal_tokens[].principal`；同一 `channel_id` 内无重复 `(provider, identity_type, external_user_id)`
3. 同一个 `(provider, session_id)` 只对应一个 `channel_id`
4. 同一个 `channel_id` 只对应一个 `(provider, session_id)`
5. 每个 channel `protocol == im.v1`
6. 每个 channel `description.session_type == p2p`
7. description 的 `provider/session_id` 与 mapping 一致
8. 每个 active p2p channel 能推导出两个不同的参与方 principal，且身份类型为一条 user、一条 bot
9. active mapping 引用的参与方 principal 都存在 token
10. Sync Worker principal 可读写 channel
