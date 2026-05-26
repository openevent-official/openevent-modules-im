# OpenEvent IM Module

[中文版](README_cn.md)

This repository provides IM protocol modules for OpenEvent. It currently
contains:

- `im.v1` IM protocol specification.
- Python IM protocol SDK.
- P2P direct-message sync worker.
- Feishu/Lark P2P direct-message configuration examples.

This repository does not redefine the OpenEvent gRPC API. OpenEvent API behavior
is defined by the installed `openevent-sdk` package and its documentation.

## Documentation

Recommended reading order:

1. [`docs/IM_PROTOCOL.md`](docs/IM_PROTOCOL.md): `im.v1` protocol
   specification, including channel description, payload envelope, and message
   semantics.
2. [`docs/IM-PROTOCOL-SDK.md`](docs/IM-PROTOCOL-SDK.md): public Python SDK
   APIs, data models, and integration guidance.
3. [`docs/IM-P2P-SYNCER.md`](docs/IM-P2P-SYNCER.md): P2P direct-message sync
   worker scope, configuration, and operations checklist.

Public documents describe only public protocols, public APIs, configuration, and
runtime behavior.

This module is independent of any specific upper-layer application. Business
processes compose with this module only through the `im.v1` protocol and
OpenEvent channels.

## Current Capabilities

- `src/openevent/im_sdk/` provides `im.v1` payload encoding, decoding, parsing,
  and OpenEvent publishing helpers.
- `src/openevent/im_p2p_syncer/` provides the P2P sync worker and
  `im-p2p-syncer` command entry point.
- `p2p_config.yaml` provides a sample P2P direct-message worker configuration.
- `tests/` contains basic unit tests.

The current P2P worker focuses on `session_type="p2p"` direct-message sessions.
It does not support group chats, channels, group member management, or mention
parsing.

## Requirements

Python 3.10 or later is required. The runtime environment must provide
`openevent-sdk>=0.3.0`.

Tests use the `openevent-sdk>=0.3.0` package already installed in the current
Python environment. They do not install SDK from the submodule.

## Run

Start the P2P sync worker with the sample config:

```bash
im-p2p-syncer --config p2p_config.yaml
```

For real deployments, generate an equivalent config with your OpenEvent
endpoint, principals, tokens, provider credentials, and session mappings.

## Build and Test

Build, test, and install tasks are wrapped by `make`. `build/` stores build
dependencies, test dependencies, caches, and temporary files. Wheel artifacts are
written to `dist/`.

Build the wheel:

```bash
make build
```

The wheel is written to:

```text
dist/openevent_modules_im-0.1.0-py3-none-any.whl
```

The wheel contains `openevent.im_sdk` and `openevent.im_p2p_syncer`.
`openevent-sdk>=0.3.0` is resolved by the install environment.

Build and install:

```bash
make install
```

Pass `pip install` options through `INSTALL_ARGS` when a custom install path is
needed:

```bash
make install INSTALL_ARGS="--target /opt/openevent-modules-im"
make install INSTALL_ARGS="--prefix /opt/openevent-modules-im"
```

Run tests:

```bash
make test
```

Run end-to-end tests with an explicit OpenEvent server binary:

```bash
OPENEVENT_SERVER_BIN=<openevent_server_binary> make e2e
```

If `openevent-sdk`, `pytest`, or `PyYAML` is missing from the current Python
environment, the e2e script exits before starting OpenEvent.

Clean build products and temporary files:

```bash
make clean
```
