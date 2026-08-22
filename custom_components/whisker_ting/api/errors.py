"""Exception hierarchy for Ting REST API operations."""


class WhiskerApiError(Exception):
    """Base exception for Whisker API errors."""


class WhiskerAuthError(WhiskerApiError):
    """Authentication error."""


class WhiskerConnectionError(WhiskerApiError):
    """Connection error."""


class WhiskerAuthorizationError(WhiskerApiError):
    """The account is not authorized for an API resource."""


class WhiskerNotFoundError(WhiskerApiError):
    """An optional or removed API resource was not found."""


class WhiskerRateLimitError(WhiskerApiError):
    """The API temporarily rejected a request due to rate limiting."""


class WhiskerServiceError(WhiskerApiError):
    """The remote service temporarily failed."""


class WhiskerInvalidResponseError(WhiskerApiError):
    """The API returned a malformed successful response."""
