from .normalizer import require_duration_ms, require_timestamp_ms


def is_request_timeout(request_event_ms: int, now_ms: int, timeout_ms: int) -> bool:
    request_event_ms = require_timestamp_ms(request_event_ms, "request_event_ms")
    now_ms = require_timestamp_ms(now_ms, "now_ms")
    timeout_ms = require_duration_ms(timeout_ms, "timeout_ms")
    return now_ms >= request_event_ms and now_ms - request_event_ms >= timeout_ms
