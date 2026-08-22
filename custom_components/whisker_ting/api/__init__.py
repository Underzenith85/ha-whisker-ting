"""Public REST API surface for the Whisker Ting integration."""

from .client import WhiskerApiClient
from .errors import (
    WhiskerApiError,
    WhiskerAuthError,
    WhiskerAuthorizationError,
    WhiskerConnectionError,
    WhiskerInvalidResponseError,
    WhiskerNotFoundError,
    WhiskerRateLimitError,
    WhiskerServiceError,
)
from .models import (
    ConditionsSnapshot,
    DeviceConditions,
    DeviceState,
    FireHazardStatus,
    FrozenPipeData,
    FrozenPipeRecord,
    HazardStatus,
    Site,
    TingEvent,
    UserData,
    VoltageReading,
)

__all__ = [
    "ConditionsSnapshot",
    "DeviceConditions",
    "DeviceState",
    "FireHazardStatus",
    "FrozenPipeData",
    "FrozenPipeRecord",
    "HazardStatus",
    "Site",
    "TingEvent",
    "UserData",
    "VoltageReading",
    "WhiskerApiClient",
    "WhiskerApiError",
    "WhiskerAuthError",
    "WhiskerAuthorizationError",
    "WhiskerConnectionError",
    "WhiskerInvalidResponseError",
    "WhiskerNotFoundError",
    "WhiskerRateLimitError",
    "WhiskerServiceError",
]
