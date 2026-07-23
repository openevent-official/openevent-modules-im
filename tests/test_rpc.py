import pytest
from grpc import RpcError, StatusCode

from openevent.im_p2p_syncer.syncer import _is_transient_fetch_error


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
