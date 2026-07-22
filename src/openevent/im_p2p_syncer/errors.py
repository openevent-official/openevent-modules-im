class ProviderDataError(RuntimeError):
    """Provider delivered malformed or inconsistent message data."""


class PermanentProviderError(RuntimeError):
    """Provider rejected a request that must not be retried."""


class TransientProviderError(RuntimeError):
    """Provider request failed temporarily and may be retried."""


class StateConflictError(RuntimeError):
    """OpenEvent history contains conflicting protocol state."""
