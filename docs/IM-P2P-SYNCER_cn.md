# IM P2P Syncer 使用说明

[English version](IM-P2P-SYNCER.md)

> 版本：v0.4
> 状态：可用
> 适用范围：在 IM Provider 的 P2P 单聊会话与 OpenEvent `im.v1` channel 之间做双向同步。

稳定协议规格见 [`IM_PROTOCOL_cn.md`](IM_PROTOCOL_cn.md)。本文面向部署和集成使用，描述 P2P Sync Worker 的公开使用范围、协议约束、配置方式和运维检查。

## 1. 使用范围

IM P2P Syncer 是独立进程模块，在 IM 平台单聊会话和 OpenEvent channel 之间做双向同步：

一组配置 mapping 对应一个 OpenEvent `im.v1` channel 和一个 Provider P2P 会话，两者表示同一条消息流。同步范围由会话和 channel 决定，不由消息发送方向决定：IM 会话中实际存在的 user 消息和 bot/app 消息都必须同步为 `sync.record`。

1. 将 IM 平台单聊消息写入 OpenEvent `sync.record`
2. 监听 OpenEvent `send.request`，调用对应 IM 平台发送单聊消息
3. 将发送动作结果写回 OpenEvent `send.result`
4. 后续把 Provider 真实消息回流继续同步为普通 `sync.record`

当前范围：

- 只支持 `session_type="p2p"`
- 不支持群聊、频道、群机器人群发、群成员管理
- 同步消息记录，不同步消息编辑、撤回等后续状态变化
- 不解析 `@`，不使用 `recipients` 表达提及
- OpenEvent channel 预配置创建，Worker 不自动创建或修改 channel

## 2. P2P 协议用法

协议字段、payload envelope、channel `description` 和各 kind 的通用规则统一见
[`IM_PROTOCOL_cn.md`](IM_PROTOCOL_cn.md)。P2P Worker 的场景约束如下：

| 对象 | P2P 约束 |
| --- | --- |
| 可处理 channel | `protocol == "im.v1"`，`description.session_type == "p2p"`，在 mapping 中，且为包含 user、bot 和 Sync Worker 三方 principal 的 private channel |
| `send.request` | 只消费，不发布；目标会话由 `channel_id -> (provider, session_id)` 决定；OpenEvent 发送方由 `send.request.principal` 在该 channel 的 mapping 中校验 |
| `sync.record` | 由 Worker 发布；OpenEvent `principal` 为发送者映射 principal，`recipients` 为单聊对端 principal |
| `send.result` | 只在 Provider 发送成功后由 Worker 发布；OpenEvent `principal` 为 Worker principal，`recipients` 为对应 `send.request` 发起 principal |
| channel ACL | channel 必须为 private，并覆盖单聊双方 principal 与 Sync Worker principal；ACL 是权限边界，`recipients` 不是权限边界 |

Provider 发送失败不要求业务方重发。任何 Provider 失败响应、发送异常、成功响应缺少
`provider_message_id` 或发送 principal 缺少 mapping，都会立即使 Worker 退出且不写 `send.result`。
原稳定 `send.request` 保持未完成，修复并重启后恢复同一 request。当前 Worker 只为成功发送写
`send.result`。

发送失败会退出 Worker，因而暂停它配置的全部 channel。运维必须先修复失败原因再重启，并为进程
管理器配置退避，避免快速重启循环。

该 fail-stop 策略不会在 OpenEvent 中留下失败终态，运维必须通过进程退出状态和日志定位原因。永久
坏 request 会在每次重启后再次执行，直到配置或 payload 被修复；当前 Worker 不提供 dead-letter
或人工跳过机制。

如果 Provider 已接受发送但响应丢失，恢复会复用同一 request 派生的 Provider UUID；能否避免重复
投递仍依赖 Provider 正确实现该幂等键。

## 3. 映射模型

### 3.1 Principal

人机单聊场景涉及三类 principal：

| 类型 | 用途 |
| --- | --- |
| 业务调用 principal | 写入 `send.request`；P2P 中必须同时是该 channel mapping 内的 bot principal；在 Feishu/Lark 出站中映射为配置的应用/机器人发送身份 |
| Sync Worker principal | 读取 OpenEvent 消息、查询 channel、发布 `send.result` |
| 会话参与方 principal | Provider 用户或机器人映射到 OpenEvent 后的身份，用于发布 `sync.record`；每个 P2P channel 必须包含一个 user principal 和一个 bot principal |

`principal_tokens` 管理会话参与方 principal 的 OpenEvent token，供 Worker 以用户或机器人身份发布
`sync.record`。Worker principal/token 独立放在 `worker` 配置中，不放入 `principal_tokens`。

### 3.2 P2P Mapping

`mappings` 是 P2P 单聊路由表。每条 mapping 描述某个 OpenEvent channel 内的一个
Provider 外部身份，并绑定到对应 OpenEvent principal。`identity_type="user"` 表示
人类用户；`identity_type="bot"` 表示应用/机器人身份。同一个人机单聊 channel
必须正好有两条 mapping：一条 user，一条 bot。配置文件中列出的 Provider 和 mapping 全部生效；需要禁用时直接从配置中删除。

字段类型：

| 字段 | 类型 |
| --- | --- |
| `provider` | string |
| `identity_type` | string，`user` 或 `bot`，缺省按 `user` 处理 |
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

约束：

- 同一个 `channel_id` 内，`(provider, identity_type, external_user_id)` 必须唯一
- 同一个 Provider 外部身份可以出现在多个不同 `channel_id` 中
- 同一个 `(provider, session_id)` 必须只对应一个 `channel_id`
- 同一个 `channel_id` 必须只对应一个 `(provider, session_id)`
- 同一 p2p channel 必须正好能推导出两个不同的参与方 principal，并且身份类型必须是一条 `user`、一条 `bot`
- Feishu/Lark bot mapping 的 `external_user_id` 必须等于对应 Provider 的 `credentials.app_id`，保证回流的 app/bot sender 能稳定映射到该 bot principal
- 出站 `send.request` 的 OpenEvent `principal` 必须能在同一 `channel_id` 的 bot mapping 中反查到 Provider `external_user_id`，用于证明该 principal 是当前机器人参与方
- Provider 消息拉取器对历史接口和实时订阅使用相同 sender 规则：user 发送者映射到 user principal，app/bot 发送者映射到 bot principal；上层处理器不区分消息来源。无法映射或消息格式非法时，禁止发布并使 Worker 失败退出，不能静默跳过或确认该消息
- 不在映射配置内的 channel，即使 Sync Worker principal 可见，也不得处理

## 4. 运行方式

通过 `make install` 安装生成的 wheel 后提供命令入口：

```bash
im-p2p-syncer --config /etc/openevent/im-sync-worker.yaml
```

进程收到 `SIGINT` 或 `SIGTERM` 时会停止 Lark 长连接并尝试优雅退出。业务方通过 OpenEvent 写入 `send.request`，Worker 负责在 P2P channel 内消费请求并在成功发送后回写 `send.result`；Provider 侧真实消息回流仍同步为普通 `sync.record`，不会触发新的 Provider 发送。

同一个 P2P channel 内有未完成 `send.request` 时，Worker 必须先成功发送该请求，再继续推进该 channel 的后续同步。fatal 失败会保留未完成 request，供修复后重启恢复。业务方应观察成功 `send.result`，不要为同一业务发送动作重复写入新的 `send.request`。

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

- 运行依赖固定为 `lark-oapi==1.6.4`，因为 WebSocket 关闭兼容逻辑依赖该版本的运行行为
- 使用 `providers[].credentials.app_id` 与 `providers[].credentials.app_secret` 初始化官方 SDK client
- API endpoint 使用 `providers[].options.api_base_url`；`feishu` 通常为 `https://open.feishu.cn`，`lark` 通常为 `https://open.larksuite.com`
- 必须在开放平台启用机器人能力并订阅 `im.message.receive_v1`；实时入站使用官方 SDK WebSocket 长连接，不需要部署公网 Webhook
- 一个 Worker 只允许配置一个 Feishu/Lark provider；`providers[].name` 同时决定 Provider 标识和 Adapter 类型，值只能为 `feishu` 或 `lark`。同一应用下的多个 P2P `chat_id` 可以共用这条长连接
- 出站发送基础实现至少支持 `msg_type="text"`，`content={"text": "..."}`。平台不按普通用户 `open_id` 代发；Worker 必须先校验 `send.request.principal` 属于该 P2P channel 的 bot mapping，再使用配置中的应用/机器人身份，由该应用/机器人向 `session_id` 对应的单聊 `chat_id` 发送消息
- 应用/机器人身份由 `providers[].credentials` 指定，并在 `mappings[]` 中用 `identity_type="bot"` 显式描述；其 `external_user_id` 必须等于 `credentials.app_id`。`identity_type="user"` 的 `external_user_id` 表示人类用户的 `open_id`。`im.message.receive_v1` 实时事件和 `message.list` 历史消息都按 sender type 将 user、app/bot 分别映射到 user、bot principal 后发布 `sync.record`，不能按发送方向过滤
- `message.create` 成功并返回 `message_id` 后直接按成功处理，不再调用 `message.get` 做额外确认
- 任何 `message.create` 失败响应或异常都直接保留 request 并退出 Worker

Provider 消息拉取器把 `im.message.receive_v1` 订阅和 `message.list` 连接交接封装成同一条带确认的消息流。SDK 回调先校验实时事件并写入有界队列；队列满或映射内消息格式错误时回调抛错，使本次投递失败，同时使 Worker 退出并由进程管理器重启。业务状态、映射、幂等和 OpenEvent 发布仍只在 Worker 单线程主循环中修改。

`message.list` 只由消息拉取器在首次启动和 WebSocket 重连后使用，稳定连接期间不做周期扫描。拉取器先建立 WebSocket 并缓冲实时事件，再按 session 从已确认 `event_ms` 向前重叠 `history_overlap_ms` 拉完固定历史窗口；没有已确认消息时回看 `history_lookback_ms`。重启时已确认 `event_ms` 和 `message_id` 幂等集合从 OpenEvent 历史 `sync.record` 恢复。明确临时的历史查询失败保留原查询窗口和 `page_token`，等待 `history_retry_delay_ms` 后重试；永久错误、坏数据和未分类异常使 Worker 退出。

拉取器对每个 session 最多向上层交付一条未确认消息。同一 session 的交接窗口和已交付历史消息完成前，不交付该 session 缓冲的实时消息；其他 session 可以继续拉取和交付。上层处理器收到的都是普通 Provider 消息，不存在补偿态或稳定态：历史消息与订阅消息进入同一队列，统一执行同 channel 串行规则。未完成的 `send.request` 只阻塞所在 channel 的 Provider 消息，其他 channel 的入站和出站继续推进。Provider 消息成功发布或按幂等状态确认已处理后，处理器才向拉取器确认该消息。

OpenEvent `Fetch` 只对 `UNAVAILABLE` 和 `DEADLINE_EXCEEDED` 重试，其他错误使 Worker 退出。

Provider `message_id` 是入站幂等键和发送结果关联键。`page_token` 只用于同一历史查询窗口的分页；时间戳、`page_token` 和 `message_id` 都不作为 Provider 提供的持久 sequence。重叠历史查询降低延迟可见导致的漏消息风险，但由于 Lark 未声明最大可见延迟，不能提供无限延迟下的绝对不漏保证。

OpenEvent `seq` 表示记录持久写入的顺序，不表示 IM 消息业务时间。历史接口返回的较早消息可能以更大的 `seq` 写入；消费者必须用 `timestamps.event_ms` 理解消息业务时间。

## 6. 可靠性与 Fail-Stop 行为

| 场景 | 策略 |
| --- | --- |
| 入站 Provider sender 无 P2P mapping | 禁止发布并使 Worker 失败退出，等待配置修复后由进程管理器重启 |
| 映射内 Provider 消息格式非法 | Worker 失败退出，不确认或跳过消息 |
| 同一 `request_id` 的 `send.request` 内容冲突，或 `send.result.prev_seq` 不匹配 | Worker 失败退出，禁止按不完整状态恢复并重复发送 |
| principal token 缺失 | 禁止发布，记录错误，等待配置修复 |
| channel protocol、description、private visibility 或成员不匹配 | 启动失败 |
| Lark 长连接启动失败或永久退出 | Worker 失败退出，由进程管理器重启；不静默退化为纯历史轮询 |
| 实时事件队列满或回调失败 | 回调抛错，本次事件确认失败；Worker 退出，重启后的消息拉取器重新执行连接交接 |
| 消息拉取器内部发生明确临时的历史查询失败 | 保留该 session 的查询窗口和 `page_token`，等待 `history_retry_delay_ms` 后重试；不引入全局恢复阶段，也不阻塞其他 channel |
| 消息拉取器内部发生永久、坏数据或未分类的历史查询失败 | 不确认相关消息，Worker 退出 |
| `send.request.principal` 不在 mapping 内 | 保留未完成 request，不写 `send.result`，Worker 退出 |
| Provider 发送失败 | 保留未完成 request，不写 `send.result`，Worker 立即退出 |
| OpenEvent 发布已确认未提交，且底层状态为 `UNAVAILABLE` 或 `DEADLINE_EXCEEDED` | 按 `retry.publish_retry_delay_ms` 固定间隔有限重试，超过 `retry.publish_max_attempts` 后退出进程 |
| OpenEvent 发布结果不确定且对账失败 | 不再发布，立即退出，避免产生重复消息 |
| OpenEvent 因 payload 超限拒绝完整 `sync.record` | 改写并发布超大消息降级 `sync.record`，只记录 `provider_message_id`、消息类型、时间、发送者等轻量 meta，并按 [`IM_PROTOCOL_cn.md`](IM_PROTOCOL_cn.md) 写入 `message_too_large` 降级字段 |
| OpenEvent 仍因 payload 超限拒绝降级 `sync.record` | 不再裁剪或重试，不确认该 Provider 消息，Worker 失败退出 |

## 7. 配置

Sync Worker 所有运行参数必须从 YAML 配置文件读取，不依赖环境变量。样例见 `p2p_config.yaml`，配置结构如下。

### 7.1 配置示例

下面示例只展示 Lark P2P 单聊 channel 的必填配置。实际部署时需要替换
OpenEvent endpoint、principal、token、Lark 应用凭据、Lark `open_id/chat_id` 和
OpenEvent `channel_id`。

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

### 7.2 字段与校验

| 字段 | 必填 | 说明与校验 |
| --- | --- | --- |
| `version` | 是 | string，配置 schema 版本，必须为 `v1` |
| `worker.principal` | 是 | uint64，Sync Worker 自身 OpenEvent principal，必须大于 0 |
| `worker.token` | 是 | string，Sync Worker 自身 OpenEvent token，必须非空 |
| `worker.shutdown_timeout_ms` | 否 | integer，收到退出信号后的最大优雅关闭等待时间，默认 `10000`，必须大于等于 0；超时以失败状态退出 |
| `openevent.target` | 是 | string，OpenEvent 公共 gRPC endpoint，必须非空 |
| `retry.publish_max_attempts` | 否 | integer，OpenEvent 写入失败最大尝试次数，默认 `5`，必须大于 0 |
| `retry.publish_retry_delay_ms` | 否 | integer，明确未提交且底层状态为 `UNAVAILABLE` 或 `DEADLINE_EXCEEDED` 时的固定重试间隔，默认 `200`，必须大于等于 0 |
| `retry.idle_sleep_ms` | 否 | integer，主循环空闲休眠，默认 `200`，必须大于等于 0 |
| `logging.*` | 否 | object，日志级别等基础配置 |
| `principal_tokens[]` | 是 | array，用户 OpenEvent principal token 数组；`principal` 为 uint64 且唯一，`token` 为非空 string，且不得包含 `worker.principal` |
| `providers[]` | 是 | array，必须正好一个 Provider；列出的 Provider 全部生效 |
| `providers[].name` | 是 | string，同时作为 Provider 标识和 Adapter 类型，只能是 `feishu` 或 `lark` |
| `providers[].sync.history_retry_delay_ms` | 否 | integer，消息拉取器的启动/重连历史查询发生明确临时失败后的固定重试间隔，默认 `1000`，必须大于等于 0；稳定连接不会按此字段周期扫描 |
| `providers[].sync.history_overlap_ms` | 否 | integer，消息拉取器每次连接交接向已确认 `event_ms` 之前重叠的窗口，默认 `300000`，必须大于等于 0 |
| `providers[].sync.history_lookback_ms` | 否 | integer，无已确认 Provider 消息时，消息拉取器首次连接交接的回看窗口，默认 `300000`，必须大于 0 |
| `providers[].sync.page_size` | 否 | integer，历史接口分页大小，默认 `50`，取值 `1..50` |
| `providers[].sync.event_queue_size` | 否 | integer，实时事件有界队列容量，默认 `1000`，必须大于 0 |
| `providers[].credentials` | 是 | object，Feishu/Lark provider 必须包含非空 string `app_id` 与 `app_secret` |
| `providers[].options` | 否 | object；`api_base_url` 为 REST 与 WebSocket 开放平台域名；`timeout_seconds` 为 REST 超时；`event_connect_timeout_seconds` 为长连接启动超时；`event_reconnect_timeout_seconds` 为持续断连后触发 Worker 失败的上限，默认 `300` 秒；字段填写时必须有效，超时必须大于 0 |
| `mappings[]` | 是 | 非空 array，P2P 单聊映射数组；每条都生效，并必须满足唯一性和 user+bot 双方约束 |

所有 `*_ms` 配置字段均为毫秒 duration，不是 Unix 时间戳。

启动时还必须反查 OpenEvent channel 并校验 `protocol`、`description.provider`、
`description.session_id`、`description.session_type` 与 mapping 一致。

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

## 9. 上线检查

1. `worker.principal` 与 `worker.token` 完整
2. 配置文件无重复 `principal_tokens[].principal`；同一 `channel_id` 内无重复 `(provider, identity_type, external_user_id)`
3. 同一个 `(provider, session_id)` 只对应一个 `channel_id`
4. 同一个 `channel_id` 只对应一个 `(provider, session_id)`
5. 每个 channel `protocol == im.v1`
6. 每个 channel `description.session_type == p2p`
7. description 的 `provider/session_id` 与 mapping 一致
8. 每个 p2p channel 能推导出两个不同的参与方 principal，且身份类型为一条 user、一条 bot
9. 每个 channel 是 private channel，成员包含 user、bot 和 Sync Worker 三方 principal
10. Feishu/Lark bot mapping 的 `external_user_id` 等于对应 Provider 的 `credentials.app_id`
11. mapping 引用的参与方 principal 都存在 token
12. Sync Worker principal 可读写 channel
13. Lark 应用已订阅 `im.message.receive_v1`，且历史消息读取权限满足连接交接范围
14. 进程管理器会在长连接永久失败或 fatal 发送失败导致 Worker 退出后重启进程
15. 进程管理器已配置重启退避和告警，永久坏 request 或配置错误不会形成快速重启循环
