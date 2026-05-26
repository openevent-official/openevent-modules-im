# IM Protocol im.v1

[English version](IM_PROTOCOL.md)

> 状态：稳定规格
> 适用范围：OpenEvent channel `protocol="im.v1"` 的 IM payload 与 channel description

本文只定义 `im.v1` 的公开协议规则。SDK 公开 API 见
[`IM-PROTOCOL-SDK_cn.md`](IM-PROTOCOL-SDK_cn.md)，P2P 单聊同步 Worker 的使用方式见
[`IM-P2P-SYNCER_cn.md`](IM-P2P-SYNCER_cn.md)。

## 1. 协议边界

业务模块写入 `send.request` SHOULD 通过
`ImProtocolClient.publish_send_request(...)`；Sync Worker 写入
`sync.record` / `send.result` SHOULD 通过 SDK 或等价编码流程。SDK 是推荐编码入口，
但不证明调用方是否遵守所有跨消息、跨 channel 或具体 Sync Worker 的协议语义。

绕过 SDK、直接构造非法 `im.v1` payload，或在 OpenEvent 顶层字段中写入不符合
本协议约束的 `principal` / `recipients` / `channel_id`，属于调用方违反协议；
这类输入不作为 `im.v1` 协议或 Sync Worker 的缺陷处理。SDK 或 Sync Worker
可以拒绝、忽略或记录这类非法消息，但协议不要求为其提供兼容语义。

## 2. Channel

所有 IM channel MUST 设置：

```text
protocol = "im.v1"
```

`description` MUST 是 JSON 字符串：

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

字段约束：

- `version`：string，当前固定为 `v1`
- `provider`：string，IM 平台名，如 `feishu` / `lark` / `dingtalk` / `line` / `wechatwork`
- `session_id`：string，Provider 会话 ID；若 Provider 无稳定会话 ID，可使用业务约定生成的稳定 ID
- `session_type`：string，会话类型，当前协议定义 `p2p` / `group`，后续可扩展
- `updated_at_ms`：integer，Unix epoch 毫秒时间戳，必须为非负整数
- `metadata`：object，可选，用于承载 Provider 或具体场景需要的静态扩展信息

同一个 `channel_id` 只能绑定一个 `(provider, session_id)`。`send.request`
的目标由 `channel_id` 绑定的 IM 会话决定，不由 OpenEvent `recipients` 决定。

协议层不强制 description 保存参与者列表。若某个 Sync Worker 需要参与者信息，
例如单聊双方 principal、群成员快照或 Provider 用户 ID 列表，可以由该 Worker 的配置或
`metadata` 扩展字段承载，并在对应公开使用文档中声明唯一数据源。

## 3. Envelope

所有 `im.v1` payload 都是 UTF-8 JSON object：

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

类型约定：

- `uint64` 表示 JSON integer，范围为 `0 <= value <= 2^64 - 1`；IM 消息使用的
  OpenEvent `channel_id` 和 `principal` 必须大于 0。
- OpenEvent 顶层 `principal`、`channel_id`、`seq`、`recipients[]` 均为 `uint64`。
- payload 内引用 OpenEvent seq 的字段，例如 `prev_seq`，也按 `uint64` 处理。
- payload 内时间戳字段均为 JSON integer，单位为 Unix epoch 毫秒，必须大于等于 0。
- payload 内 object 字段必须是 JSON object，不得使用 array、string 或 null 代替。

公共字段：

| 字段 | 规则 |
| --- | --- |
| `kind` | string，必填，取值为 `sync.record` / `send.request` / `send.result` |
| `data` | object，必填，业务载荷 |
| `timestamps.event_ms` | integer，必填，源事件发生时间，Unix epoch 毫秒 |
| `timestamps.ingested_ms` | integer，`sync.record` 必填，Unix epoch 毫秒 |
| `prev_seq` | uint64，条件必填，见各 kind 规则 |
| `request_id` | string，`send.request` / `send.result` 必填 |

OpenEvent Message 顶层字段承载身份和定向可见性，不放入 payload：

| kind | OpenEvent `principal` | OpenEvent `recipients` |
| --- | --- | --- |
| `sync.record` | IM 发送者映射后的参与方 principal | 可为空；具体 Sync Worker 可按场景约束，例如 P2P 填写单聊对端 principal |
| `send.request` | 提交发送请求的业务调用方 principal | 调用方可填写；SDK 默认空；不表达 IM 发送目标 |
| `send.result` | Sync Worker principal | 单元素列表，值为对应 `send.request` 的 OpenEvent `principal` |

payload 中不得包含 `source_principal`。若收到包含 `source_principal` 的
`im.v1` payload，SDK 或 Sync Worker 必须判定为非法消息。

`im.v1` payload 上限由 OpenEvent 服务端配置决定。IM 协议不定义固定上限，也不要求
SDK 或 Sync Worker 在发布前查询或预判该上限。若完整保真 `sync.record` 发布时被
OpenEvent 以 payload 超限拒绝，Sync Worker MUST 改写为超大消息降级 `sync.record`
后重新发布，保留可用于幂等、排障和后续补偿的元信息。

## 4. Kinds

### 4.1 `sync.record`

`sync.record` 表示从 IM Provider 同步进 OpenEvent 的真实 IM 记录。

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

规则：

- `request_id` 不应填写
- `prev_seq` 可选；若能通过 Provider 消息 ID 关联到对应 `send.result`，MUST 填写该 `send.result.seq`
- `data.provider_message_id` string，必填且非空，用于入站幂等
- 具体 Sync Worker 的入站幂等键 MUST 至少包含 Provider 标识、Provider 会话 ID、OpenEvent `channel_id` 和 `provider_message_id`
- `data.msg_type` string，必填且非空，表示 Provider 消息类型
- `data.content_raw` object，必填，用于保真保存 Provider 原始内容
- `data.text` string，可选，仅文本消息使用
- `data.is_init` bool，可选；`true` 表示同步初始化边界，后续记录与此前不保证连续

若完整保真 `sync.record` 因 payload 超限被 OpenEvent 拒绝，`sync.record.data`
MUST 使用降级形态重新发布。

降级记录的规范判定字段是外层 `data.content_omitted` 与 `data.omit_reason`：

- 降级记录：`data.content_omitted == true` 且 `data.omit_reason == "message_too_large"`
- 正常记录：不设置 `content_omitted` / `omit_reason`；若显式设置，`content_omitted` MUST 为 `false`

降级记录示例：

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

具体规则：

- 触发条件：Sync Worker 先尝试发布完整保真 `sync.record`；若 OpenEvent 拒绝写入并
  表明 payload 超过服务端限制，Worker 再构造降级记录并重新发布。
- 降级记录仍表示一条真实 Provider 消息；它参与正常入站幂等，成功发布后也允许推进
  Provider 同步游标。
- `data.provider_message_id` string、`data.msg_type` string、`data.content_raw` object 必填；OpenEvent
  顶层 `principal` / `recipients`、`timestamps.event_ms`、`prev_seq` 规则保持不变。
- `data.content_omitted` bool，必须为 `true`；`data.omit_reason` string，必须为
  `message_too_large`。
- `data.content_raw.omitted` bool，必须为 `true`；`data.content_raw.reason` string，必须为
  `message_too_large`。
- `data.content_raw.original_size_bytes` integer，若 Provider 能提供或 Worker 能计算则
  MUST 填写大于 0 的整数；无法获得时可以省略。
- `data.content_raw.metadata` object，MUST 只保存不超限的轻量元信息，例如
  Provider 会话 ID、发送者 ID、消息类型、文件名、MIME 类型、Provider 可见的内容长度、
  文件 key 或 URL 引用；不得包含导致 payload 再次超限的大字段。
- 对文本类消息，若文本内容本身导致超限，`data.text` 不得填写完整文本；可以省略
  `data.text`，或只填写不会导致超限的摘要字段，例如 `data.text_preview`。
- 降级记录不得包含完整 Provider 原始 body、完整附件二进制、完整 base64 内容或完整超长文本。
- 如果降级后 OpenEvent 仍以 payload 超限拒绝写入，Sync Worker MUST 继续裁剪
  `metadata` 或摘要字段并重试；仍无法写入时，必须停止推进该 Provider 游标并告警，
  不得静默丢弃该消息。

### 4.2 `send.request`

`send.request` 表示业务模块请求 IM Sync Worker 向 Provider 发送消息。

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

规则：

- `request_id` string，必填、非空且全局唯一
- `prev_seq` uint64，可选，本协议不限制其业务语义
- `data.msg_type` string，必填且非空
- `data.content` object，必填
- `request_id` 是 `send.request` 的协议去重键；业务方不得为同一业务发送动作生成多个不同 `request_id`
- `send.request` 不解析 `@`

### 4.3 `send.result`

`send.result` 表示 IM Sync Worker 对某条 `send.request` 的发送动作回写结果。

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

规则：

- `prev_seq` uint64，必填，值为对应 `send.request.seq`
- `request_id` string，必填且非空，值与对应 `send.request.request_id` 相同
- 一个 `send.request` 只允许对应一个成功 `send.result`
- `data.status` string，必填，取值为 `SUCCESS` 或 `FAILED`
- `data.provider_message_id` string，在 `SUCCESS` 时 SHOULD 填写
- `data.error_code` string / `data.error_message` string，在 `FAILED` 时 SHOULD 填写

`FAILED` 表示该 `send.request` 已被终止，不得再由同一个 Worker 自动推进。
持续重试、死信和人工终止策略由具体 Sync Worker 的公开使用文档声明。

`send.result` 只表达发送动作执行成功结果；Provider 侧真实消息回调仍会作为后续
`sync.record` 同步。如果某个 Sync Worker 能通过 Provider 消息 ID 判断一条
`sync.record` 是某条成功 `send.result` 对应的真实消息回流，则该
`sync.record.prev_seq` MUST 填写对应 `send.result.seq`。

## 5. 生命周期语义

IM Sync Worker 按 channel 可见性接收消息，并处理自身负责范围内的 `send.request`。

同一个 IM channel 内的发送与同步任务 MUST 串行推进。若 channel 内存在尚未成功或终止的
`send.request`，Worker MUST 按具体 Sync Worker 文档声明的策略重试或终止该请求，不得继续同步该
channel 后续 Provider 消息为新的 `sync.record`。只有该请求已经向 Provider 发送成功、
并拿到稳定 `provider_message_id` 后，Worker 才能写出成功 `send.result`；若具体 Sync Worker
声明了失败终止策略，Worker 可以写出失败 `send.result` 终止该 request。请求成功或终止后，
Worker 才能继续推进该 channel 的后续同步状态。

Worker 重启或恢复后，仍然必须继续处理尚未进入终态的 `send.request`。业务方不需要、也不应为同一业务动作补写新的 `send.request`。

若超过 `REQUEST_RESULT_TIMEOUT_MS` 仍未观察到 `send.result`，业务模块只能判定该发送仍在
IM Sync Worker 内等待重试或阻塞，不应提交同一业务动作的新 `send.request`。发送重试和失败终止由
具体 IM Sync Worker 负责；当前协议没有定义人工取消或死信 kind。

## 6. 版本策略

- `im.v1` 只做向后兼容增强
- 破坏性变更使用新的 channel protocol，如 `im.v2`
- SDK 主版本应与协议主版本保持一致
