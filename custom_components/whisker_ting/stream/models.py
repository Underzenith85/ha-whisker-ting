"""Typed models for Ting real-time streams."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class PowerQualityCategory(StrEnum):
    """Supported categorical metrics in the Ting secondary stream."""

    FREQUENCY = "frequency"
    THD_MIN = "thdMin"
    THD_AVERAGE = "thdAvg"
    THD_MAX = "thdMax"


@dataclass(frozen=True)
class VoltageData:
    """Represent one real-time voltage reading."""

    timestamp: datetime
    voltage: float
    voltage_hi: float
    voltage_lo: float
    average_peaks_max: float


@dataclass(frozen=True)
class PowerQualityData:
    """Represent frequency in hertz or normalized THD in percentage points."""

    timestamp: datetime
    category: PowerQualityCategory
    value: float


class StreamHealth(StrEnum):
    """Health of a station's real-time data stream."""

    RECEIVING = "receiving"
    DELAYED = "delayed"
    NOT_RECEIVING = "not_receiving"
    STOPPED = "stopped"


@dataclass(frozen=True)
class StationState:
    """Connection, subscription, and liveness for one station."""

    connected: bool = False
    subscribed: bool = False
    live: bool = False
    health: StreamHealth = StreamHealth.STOPPED

    @property
    def available(self) -> bool:
        """Return whether real-time readings for the station are current."""
        return self.connected and self.subscribed and self.live


@dataclass(frozen=True)
class StationDiagnostics:
    """Bounded lifecycle diagnostics for one managed station."""

    last_sample_utc: datetime | None = None
    reconnect_count: int = 0
    last_reconnect_utc: datetime | None = None
    last_reconnect_reason: str | None = None
