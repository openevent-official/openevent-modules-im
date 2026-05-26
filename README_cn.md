# OpenEvent IM 协议模块

[English version](README.md)

本仓库提供接入 OpenEvent 的 IM 协议模块，当前包含：

- `im.v1` IM 协议规格
- Python IM 协议 SDK
- P2P 单聊同步 Worker
- Feishu/Lark P2P 单聊接入配置样例

本仓库不重新定义 OpenEvent gRPC API；OpenEvent API 以环境中安装的 `openevent-sdk` 及其文档为准。

## 文档

建议按下面顺序阅读：

1. [`docs/IM_PROTOCOL_cn.md`](docs/IM_PROTOCOL_cn.md)：`im.v1` 协议规格，定义 channel description、payload envelope 和消息语义。
2. [`docs/IM-PROTOCOL-SDK_cn.md`](docs/IM-PROTOCOL-SDK_cn.md)：Python SDK 的公开 API、数据模型和集成方式。
3. [`docs/IM-P2P-SYNCER_cn.md`](docs/IM-P2P-SYNCER_cn.md)：P2P 单聊同步 Worker 的使用范围、配置方式和运维检查。

开源文档只描述公开协议、公开 API、配置和运行方式。

本模块不感知具体上层应用。上层业务进程只通过 `im.v1` 协议和 OpenEvent channel 与本模块组合。

## 当前能力

- `src/openevent/im_sdk/` 提供 `im.v1` payload 编解码、解析和 OpenEvent 发布辅助。
- `src/openevent/im_p2p_syncer/` 提供 P2P 单聊同步 Worker 与 `im-p2p-syncer` 命令入口。
- `p2p_config.yaml` 提供 P2P 单聊同步 Worker 配置样例。
- `tests/` 包含基础单元测试。

当前 P2P Worker 聚焦 `session_type="p2p"` 的单聊场景，不支持群聊、频道、群成员管理或 `@` 解析。

## 环境要求

本项目要求 Python 3.10 或更高版本，并依赖环境中可安装的 `openevent-sdk>=0.3.0`。

测试使用当前 Python 环境中已经安装好的 `openevent-sdk>=0.3.0` 包，不会从
`openevent-sdk/` 子模块安装 SDK。

## 运行

使用样例配置启动 P2P 单聊同步 Worker：

```bash
im-p2p-syncer --config p2p_config.yaml
```

实际部署时请使用自己的 OpenEvent endpoint、principal、token、Provider 凭据和会话映射生成等价配置。

## 构建和测试

构建、测试和安装统一通过 `make` 执行。`build/` 保存构建依赖、测试依赖、缓存和临时文件；wheel 产物放在 `dist/`。

构建 wheel：

```bash
make build
```

构建完成后，wheel 位于：

```text
dist/openevent_modules_im-0.1.0-py3-none-any.whl
```

本项目 wheel 包内容为 `openevent.im_sdk` 和 `openevent.im_p2p_syncer`；`openevent-sdk>=0.3.0` 由安装环境按依赖解析提供。

构建并安装生成的 wheel：

```bash
make install
```

需要指定安装路径时，通过 `INSTALL_ARGS` 传递 `pip install` 参数：

```bash
make install INSTALL_ARGS="--target /opt/openevent-modules-im"
make install INSTALL_ARGS="--prefix /opt/openevent-modules-im"
```

运行测试：

```bash
make test
```

运行端到端测试时，需要显式传入 OpenEvent server 二进制：

```bash
OPENEVENT_SERVER_BIN=<openevent_server_binary> make e2e
```

如果当前 Python 环境缺少 `openevent-sdk`、`pytest` 或 `PyYAML`，e2e 脚本会在
启动 OpenEvent 前直接退出报错。

清理构建产物和临时文件：

```bash
make clean
```
