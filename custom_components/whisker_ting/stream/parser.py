"""Validation boundary for Ting-specific SignalR payloads."""

from __future__ import annotations

import logging
import math
from datetime import UTC, datetime
from typing import Any

from .models import PowerQualityCategory, PowerQualityData, VoltageData
from .signalr import (
    SignalRProtocolError,
    extract_categorical_payloads,
    extract_invocation_payloads,
)

_LOGGER = logging.getLogger(__name__)

SECONDARY_DATA_ELEMENTS = tuple(category.value for category in PowerQualityCategory)


def parse_timestamp(value: Any) -> datetime:
    """Parse a Ting stream timestamp with a safe current-time fallback."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return timestamp.replace(tzinfo=timestamp.tzinfo or UTC)
        except ValueError:
            pass
    return datetime.now(UTC)


def decode_voltage_data(data: bytes) -> list[VoltageData]:
    """Decode validated voltage readings from framed hub messages."""
    try:
        payloads = extract_invocation_payloads(data, "updateComboBinaryData")
    except SignalRProtocolError as err:
        _LOGGER.debug("Error decoding SignalR voltage frame: %s", err)
        return []

    readings: list[VoltageData] = []
    for payload in payloads:
        try:
            voltage = float(payload["Voltage"])
            voltage_hi = float(payload["VoltageHi"])
            voltage_lo = float(payload["VoltageLo"])
            peaks = float(payload["AveragePeaksMax"])
            if not all(
                math.isfinite(value)
                for value in (voltage, voltage_hi, voltage_lo, peaks)
            ):
                raise ValueError("non-finite voltage value")
            if abs(voltage) < 1 or abs(voltage) > 1000:
                _LOGGER.debug("Discarding anomalous voltage reading: %.2fV", voltage)
                continue
            readings.append(
                VoltageData(
                    timestamp=parse_timestamp(payload.get("DataTimeUtc")),
                    voltage=voltage,
                    voltage_hi=voltage_hi,
                    voltage_lo=voltage_lo,
                    average_peaks_max=peaks,
                )
            )
        except (KeyError, TypeError, ValueError) as err:
            _LOGGER.debug("Discarding invalid voltage payload: %s", err)
    return readings


def decode_power_quality_data(data: bytes) -> list[PowerQualityData]:
    """Decode validated frequency and THD readings from framed messages."""
    try:
        payloads = extract_categorical_payloads(data)
    except SignalRProtocolError as err:
        _LOGGER.debug("Error decoding SignalR power-quality frame: %s", err)
        return []
    readings: list[PowerQualityData] = []
    for payload in payloads:
        try:
            category = PowerQualityCategory(payload["Category"])
            value = float(payload["Value"])
            if not math.isfinite(value):
                raise ValueError("non-finite power-quality value")
            readings.append(
                PowerQualityData(
                    timestamp=parse_timestamp(payload.get("ObsTime")),
                    category=category,
                    value=value,
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return readings
