class ImProtocolError(ValueError):
    """Base error for invalid IM protocol inputs."""


class InvalidKindError(ImProtocolError):
    """Raised when a payload kind is not part of im.v1."""


class MalformedPayloadError(ImProtocolError):
    """Raised when payload JSON or envelope fields are malformed."""


class PublishFailedError(RuntimeError):
    """Raised when OpenEvent publishing fails."""


class UnsupportedProtocolVersionError(ImProtocolError):
    """Raised when a channel description uses an unsupported IM protocol version."""
