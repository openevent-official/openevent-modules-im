# IM 协议 SDK 使用说明

[English version](IM-PROTOCOL-SDK.md)

> 版本：v0.1  
> 状态：可用  
> 适用范围：Python 调用方发布、解析和处理 `im.v1` 协议消息。

稳定协议规格见 [`IM_PROTOCOL_cn.md`](IM_PROTOCOL_cn.md)。本文只描述 SDK 的公开职责、公开 API、数据模型和集成方式。

## 1. 职责边界

SDK 负责：

- 提供 `im.v1` 数据模型、编码、解析和发布辅助
- 封装基于环境中已安装 `openevent-sdk` 的标准发布入口
- 固化协议写入口径，减少业务模块和 Sync Worker 重复构造 payload
- 提供 `send.request -> send.result` 的超时判定工具

SDK 不负责：

- 创建或修改 OpenEvent channel
- 调用 Feishu/Lark、钉钉等 Provider API
- 管理业务状态、Provider 游标、映射表或幂等状态
- 替调用方证明跨消息、跨 channel 或具体 Sync Worker 的协议语义一定成立

## 2. 集成位置

```text
业务模块
  -> openevent.sdk（订阅可直接使用）
  -> openevent.im_sdk（发布、解析、编码辅助）

IM Sync Worker
  -> openevent.sdk（订阅可直接使用）
  -> openevent.im_sdk（发布、解析、编码辅助）
  -> provider sdk
```

集成约束：

1. 业务模块写 `send.request` SHOULD 通过 SDK 或等价编码流程
2. Sync Worker 发布 `sync.record/send.result` SHOULD 通过 SDK 或等价编码流程
3. SDK 初始化 MUST 显式注入 `openevent.sdk.OpenEventClient` 或等价 OpenEvent 客户端对象
4. 协议语义以 [`IM_PROTOCOL_cn.md`](IM_PROTOCOL_cn.md) 为准，调用方负责保证自己遵守协议
5. 运行环境提供兼容的 `openevent-sdk`

## 3. 公开 API

当前 Python SDK 对外稳定接口：

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

说明：OpenEvent 的 `principal`、`channel_id`、`seq`、`recipients[]` 等 ID 字段语义均为
`uint64`。Python API 使用 `int` 承载这些值，但 SDK 和调用方必须按 `UInt64` 范围处理。

数据模型：

- `SendRequestInput`
- `SendResultInput`
- `SyncRecordInput`
- `ParsedMessage`

数据模型字段类型以 [`IM_PROTOCOL_cn.md`](IM_PROTOCOL_cn.md) 为准；其中 `request_id`、`msg_type`、
`provider_message_id`、`status`、`error_code`、`error_message` 为 string，`content`
和 `content_raw` 为 JSON object，`event_ms`/`ingested_ms` 为 `TimestampMs`，`prev_seq` 为
`UInt64`。

接口说明：

- 所有写入接口均显式接收 `principal` 与 `token`
- `publish_send_request(...)` 暴露 OpenEvent `recipients` 参数，由调用方决定；未传或传入 `None` 时按空列表发布；该字段只表达 OpenEvent 定向可见性，不表达 IM 发送目标
- `publish_send_result(...)` 要求调用方传入对应 `send.request` 发起 principal；SDK 只做列表结构与 `UInt64` 范围归一化
- `publish_sync_record(...)` 透传调用方传入的 `recipients`，SDK 应做 `UInt64` 范围归一化、去重、排序
- SDK 使用 OpenEvent Message 顶层 `principal` 表达来源身份，不在 payload 中写入 `source_principal`

## 4. 错误类型

SDK 公开错误类型：

- `INVALID_KIND`
- `MALFORMED_PAYLOAD`
- `PUBLISH_FAILED`
- `UNSUPPORTED_PROTOCOL_VERSION`

OpenEvent 发布失败会以 `PUBLISH_FAILED` 暴露给调用方；非法 payload、非法字段类型或不支持的协议版本会以对应错误暴露给调用方。

## 5. 使用示例

发布 `send.request`：

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

解析 OpenEvent 消息：

```python
from openevent.im_sdk import create_client

client = create_client(openevent_client)
parsed = client.parse_message(message)
if parsed.kind == "sync.record":
    provider_message_id = parsed.data["provider_message_id"]
```

判断业务观察超时：

```python
from openevent.im_sdk import is_request_timeout

timed_out = is_request_timeout(
    request_event_ms=1710000001000,
    now_ms=1710000062000,
    timeout_ms=60000,
)
```

## 6. 集成方式

业务模块：

1. 调用 SDK `publish_send_request(...)`
2. 记录返回的 OpenEvent `seq` 和业务状态
3. 如需订阅消息，直接使用 `openevent-sdk`
4. 订阅到消息后可调用 SDK `parse_message(...)`

Sync Worker：

1. 通过 `openevent-sdk` 读取 OpenEvent 消息并筛选 `send.request`
2. 使用 SDK `parse_message(...)` 做 payload 解析
3. 发送动作完成后通过 SDK `publish_send_result(...)` 回写结果，调用方保证 recipients 对应原始 `send.request.principal`
4. 入站事件到达时通过 SDK `publish_sync_record(...)` 发布记录；若 OpenEvent 拒绝完整 payload，再按协议构造降级记录重试

## 7. 版本策略

1. SDK 与协议版本绑定：`im.v1` -> SDK `v1.x`
2. 破坏性变更走新协议：`im.v2` + SDK `v2.x`
3. 同一主版本内只做向后兼容增强
