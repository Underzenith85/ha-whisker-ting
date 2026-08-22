"""Explicit, bounded import of Ting voltage history into Recorder."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import voluptuous as vol
from homeassistant.components.recorder.models.statistics import (
    StatisticData,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import async_add_external_statistics
from homeassistant.core import (
    HomeAssistant,
    ServiceCall,
    ServiceResponse,
    SupportsResponse,
)
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.util import slugify

from .api import VoltageHistoryPoint, WhiskerApiError
from .const import (
    DOMAIN,
    MAX_VOLTAGE_HISTORY_DAYS,
    SERVICE_IMPORT_VOLTAGE_HISTORY,
    VOLTAGE_HISTORY_REQUEST_HOURS,
)

ATTR_CONFIG_ENTRY_ID = "config_entry_id"
ATTR_SERIAL_NUMBER = "serial_number"
ATTR_START = "start"
ATTR_END = "end"

SERVICE_SCHEMA = vol.Schema(
    {
        vol.Required(ATTR_CONFIG_ENTRY_ID): str,
        vol.Required(ATTR_SERIAL_NUMBER): str,
        vol.Required(ATTR_START): cv.datetime,
        vol.Required(ATTR_END): cv.datetime,
    }
)


@dataclass(frozen=True)
class VoltageAggregate:
    """A normalized aggregate suitable for Recorder statistics."""

    start: datetime
    minimum_v: float
    maximum_v: float
    average_v: float
    coverage: float | None


def aggregate_voltage_history(
    points: Iterable[VoltageHistoryPoint], period: timedelta
) -> list[VoltageAggregate]:
    """Aggregate validated points into deterministic UTC periods."""
    seconds = int(period.total_seconds())
    if seconds <= 0:
        raise ValueError("Aggregation period must be positive")
    buckets: dict[datetime, list[VoltageHistoryPoint]] = {}
    for point in points:
        timestamp = point.start.astimezone(UTC)
        bucket_timestamp = int(timestamp.timestamp()) // seconds * seconds
        bucket = datetime.fromtimestamp(bucket_timestamp, UTC)
        buckets.setdefault(bucket, []).append(point)

    aggregates: list[VoltageAggregate] = []
    for start, values in sorted(buckets.items()):
        coverages = [value.coverage for value in values if value.coverage is not None]
        aggregates.append(
            VoltageAggregate(
                start=start,
                minimum_v=min(value.minimum_v for value in values),
                maximum_v=max(value.maximum_v for value in values),
                average_v=sum(value.average_v for value in values) / len(values),
                coverage=(sum(coverages) / len(coverages) if coverages else None),
            )
        )
    return aggregates


def _validate_window(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    """Validate and normalize a user-requested history window."""
    if start.tzinfo is None or end.tzinfo is None:
        raise HomeAssistantError("History start and end must include a timezone")
    start_utc = start.astimezone(UTC)
    end_utc = end.astimezone(UTC)
    if start_utc >= end_utc:
        raise HomeAssistantError("History start must be before end")
    if end_utc - start_utc > timedelta(days=MAX_VOLTAGE_HISTORY_DAYS):
        raise HomeAssistantError(
            f"History imports are limited to {MAX_VOLTAGE_HISTORY_DAYS} days"
        )
    return start_utc, end_utc


async def _fetch_history(
    client: Any, serial_number: str, start: datetime, end: datetime
) -> list[VoltageHistoryPoint]:
    """Fetch sequential daily chunks and return nothing on interruption."""
    points: list[VoltageHistoryPoint] = []
    cursor = start
    while cursor < end:
        chunk_end = min(end, cursor + timedelta(hours=VOLTAGE_HISTORY_REQUEST_HOURS))
        points.extend(
            await client.get_voltage_history(serial_number, cursor, chunk_end)
        )
        cursor = chunk_end
    return points


def _statistics(
    serial_number: str, hourly: list[VoltageAggregate]
) -> tuple[
    StatisticMetaData, list[StatisticData], StatisticMetaData, list[StatisticData]
]:
    """Build voltage and coverage external-statistics payloads."""
    key = slugify(serial_number)
    voltage_metadata: StatisticMetaData = {
        "has_mean": True,
        "has_sum": False,
        "name": "Ting historical voltage",
        "source": DOMAIN,
        "statistic_id": f"{DOMAIN}:{key}_voltage",
        "unit_of_measurement": "V",
    }
    coverage_metadata: StatisticMetaData = {
        "has_mean": True,
        "has_sum": False,
        "name": "Ting voltage history coverage",
        "source": DOMAIN,
        "statistic_id": f"{DOMAIN}:{key}_voltage_coverage",
        "unit_of_measurement": "%",
    }
    voltage = [
        StatisticData(
            start=value.start,
            min=value.minimum_v,
            max=value.maximum_v,
            mean=value.average_v,
        )
        for value in hourly
    ]
    coverage = [
        StatisticData(
            start=value.start,
            min=value.coverage * 100,
            max=value.coverage * 100,
            mean=value.coverage * 100,
        )
        for value in hourly
        if value.coverage is not None
    ]
    return voltage_metadata, voltage, coverage_metadata, coverage


def async_register_history_service(hass: HomeAssistant) -> None:
    """Register the opt-in voltage-history import service once."""
    locks: dict[tuple[str, str], asyncio.Lock] = {}

    async def async_import(call: ServiceCall) -> ServiceResponse:
        entry_id = call.data[ATTR_CONFIG_ENTRY_ID]
        serial_number = call.data[ATTR_SERIAL_NUMBER].strip()
        entry = hass.config_entries.async_get_entry(entry_id)
        if entry is None or entry.domain != DOMAIN or not serial_number:
            raise HomeAssistantError(
                "Loaded Whisker Ting entry and device are required"
            )
        coordinator = entry.runtime_data
        if serial_number not in coordinator.data:
            raise HomeAssistantError("The requested Ting device is not in this entry")
        start, end = _validate_window(call.data[ATTR_START], call.data[ATTR_END])

        lock = locks.setdefault((entry_id, serial_number), asyncio.Lock())
        if lock.locked():
            raise HomeAssistantError(
                "A history import is already running for this device"
            )
        try:
            async with lock:
                points = await _fetch_history(
                    coordinator.client, serial_number, start, end
                )
        except WhiskerApiError as err:
            raise HomeAssistantError(f"Ting history import failed: {err}") from err

        hourly = aggregate_voltage_history(points, timedelta(hours=1))
        daily = aggregate_voltage_history(points, timedelta(days=1))
        voltage_meta, voltage, coverage_meta, coverage = _statistics(
            serial_number, hourly
        )
        if voltage:
            async_add_external_statistics(hass, voltage_meta, voltage)
        if coverage:
            async_add_external_statistics(hass, coverage_meta, coverage)
        return {
            "points_received": len(points),
            "hourly_statistics": len(voltage),
            "daily_periods": len(daily),
            "coverage_statistics": len(coverage),
        }

    hass.services.async_register(
        DOMAIN,
        SERVICE_IMPORT_VOLTAGE_HISTORY,
        async_import,
        schema=SERVICE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
