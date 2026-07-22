from collections import namedtuple

import pytest
from grpc import RpcError, StatusCode

from openevent.im_p2p_syncer.rpc import RpcTimeoutInterceptor
from openevent.im_p2p_syncer.syncer import _is_transient_fetch_error


CallDetails = namedtuple(
    "CallDetails",
    ("method", "timeout", "metadata", "credentials", "wait_for_ready", "compression"),
)


def test_rpc_timeout_interceptor_adds_default_timeout():
    interceptor = RpcTimeoutInterceptor(10.0)
    captured = []

    interceptor.intercept_unary_unary(
        lambda details, request: captured.append((details, request)),
        CallDetails("/service/method", None, None, None, None, None),
        "request",
    )

    assert captured[0][0].timeout == 10.0
    assert captured[0][1] == "request"


def test_rpc_timeout_interceptor_preserves_shorter_timeout():
    interceptor = RpcTimeoutInterceptor(10.0)
    captured = []

    interceptor.intercept_unary_unary(
        lambda details, request: captured.append(details),
        CallDetails("/service/method", 3.0, None, None, None, None),
        "request",
    )

    assert captured[0].timeout == 3.0


class FakeRpcError(RpcError):
    def __init__(self, status_code):
        self._status_code = status_code

    def code(self):
        return self._status_code


@pytest.mark.parametrize("status_code", [StatusCode.UNAVAILABLE, StatusCode.DEADLINE_EXCEEDED])
def test_fetch_retries_only_explicit_transient_statuses(status_code):
    assert _is_transient_fetch_error(FakeRpcError(status_code))


@pytest.mark.parametrize("error", [FakeRpcError(StatusCode.PERMISSION_DENIED), RuntimeError("bad")])
def test_fetch_does_not_retry_permanent_or_unclassified_errors(error):
    assert not _is_transient_fetch_error(error)
