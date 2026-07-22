class ImProtocolError(ValueError):
    """Base error for invalid IM protocol inputs."""


class InvalidKindError(ImProtocolError):
    """Raised when a payload kind is not part of im.v1."""


class MalformedPayloadError(ImProtocolError):
    """Raised when payload JSON or envelope fields are malformed."""


class PublishFailedError(RuntimeError):
    """Raised when OpenEvent publishing fails."""

    def __init__(
        self,
        message: str,
        *,
        retry_safe: bool = False,
        outcome_unknown: bool = False,
    ):
        super().__init__(message)
        self.retry_safe = retry_safe
        self.outcome_unknown = outcome_unknown
