from __future__ import annotations

from collections import namedtuple
from typing import Any

import grpc


class _ClientCallDetails(
    namedtuple(
        "_ClientCallDetails",
        ("method", "timeout", "metadata", "credentials", "wait_for_ready", "compression"),
    ),
    grpc.ClientCallDetails,
):
    pass


class RpcTimeoutInterceptor(grpc.UnaryUnaryClientInterceptor):
    def __init__(self, timeout_seconds: float):
        self._timeout_seconds = timeout_seconds

    def intercept_unary_unary(self, continuation, client_call_details, request):
        existing_timeout = client_call_details.timeout
        timeout = (
            self._timeout_seconds
            if existing_timeout is None
            else min(existing_timeout, self._timeout_seconds)
        )
        details = _ClientCallDetails(
            method=client_call_details.method,
            timeout=timeout,
            metadata=client_call_details.metadata,
            credentials=client_call_details.credentials,
            wait_for_ready=getattr(client_call_details, "wait_for_ready", None),
            compression=getattr(client_call_details, "compression", None),
        )
        return continuation(details, request)


def create_rpc_client(client_type: type[Any], target: str, timeout_seconds: float) -> Any:
    base_client = client_type(target)
    channel = grpc.intercept_channel(
        base_client.channel,
        RpcTimeoutInterceptor(timeout_seconds),
    )
    return client_type(target, channel=channel)
