"""Typed models for Ting real-time streams."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


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
    """Represent one categorical power-quality reading."""

    timestamp: datetime
    category: str
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
