from __future__ import annotations

import hashlib


def stable_lark_openapi_uuid(request_id: str) -> str:
    digest = hashlib.sha256(request_id.encode("utf-8")).hexdigest()
    return f"oe-{digest[:47]}"
