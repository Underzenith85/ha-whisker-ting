"""Tests for bounded historical-voltage import behavior."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError

from custom_components.whisker_ting.api import (
    DeviceState,
    VoltageHistoryPoint,
    WhiskerRateLimitError,
)
from custom_components.whisker_ting.api.client import WhiskerApiClient
from custom_components.whisker_ting.api.parsers import parse_voltage_history
from custom_components.whisker_ting.const import DOMAIN, SERVICE_IMPORT_VOLTAGE_HISTORY
from custom_components.whisker_ting.history import (
    _fetch_history,
    _validate_window,
    aggregate_voltage_history,
    async_register_history_service,
)

FIXTURE = Path(__file__).parent / "fixtures" / "voltage_history.json"


def test_history_parser_validates_units_shape_and_timezone() -> None:
    """A sanitized v3 fixture becomes finite, UTC-normalized typed points."""
    with FIXTURE.open(encoding="utf-8") as file:
        points = parse_voltage_history(json.load(file))

    assert len(points) == 3
    assert points[0].start == datetime(2026, 8, 21, 0, 5, tzinfo=UTC)
    assert points[0].coverage == pytest.approx(55 / 60)
    assert points[2].start == datetime(2026, 8, 21, 8, tzinfo=UTC)
    assert parse_voltage_history({"unit": "A", "data": [{}]}) == []


def test_history_parser_ignores_malformed_and_deduplicates() -> None:
    """Malformed values and duplicate timestamps cannot inflate imports."""
    valid = {
        "timestampUtc": "2026-08-21T00:00:00Z",
        "minimum": 118,
        "maximum": 122,
        "average": 120,
    }
    points = parse_voltage_history(
        {"data": [valid, valid, {**valid, "average": float("nan")}, None]}
    )

    assert len(points) == 1


@pytest.mark.asyncio
async def test_client_uses_v3_endpoint_and_bounded_utc_parameters() -> None:
    """The client uses only the observed v3 route and encodes device identity."""
    client = WhiskerApiClient(object(), "person@example.invalid")
    client._request = AsyncMock(return_value={"unit": "V", "data": []})
    pacific = timezone(timedelta(hours=-7))

    await client.get_voltage_history(
        "SERIAL/001",
        datetime(2026, 8, 1, tzinfo=pacific),
        datetime(2026, 8, 2, tzinfo=pacific),
    )

    client._request.assert_awaited_once_with(
        "GET",
        "/api/v3/Devices/SERIAL%2F001/voltage/dateRange",
        params={
            "startUtc": "2026-08-01T07:00:00+00:00",
            "endUtc": "2026-08-02T07:00:00+00:00",
        },
    )
    with pytest.raises(ValueError):
        await client.get_voltage_history(
            "SERIAL-001",
            datetime(2026, 8, 1, tzinfo=UTC),
            datetime(2026, 8, 3, tzinfo=UTC),
        )


def test_hourly_and_daily_aggregation_are_deterministic() -> None:
    """Sub-hour points group at exact UTC Recorder boundaries."""
    points = parse_voltage_history(json.loads(FIXTURE.read_text(encoding="utf-8")))
    hourly = aggregate_voltage_history(points, timedelta(hours=1))
    daily = aggregate_voltage_history(points, timedelta(days=1))

    assert len(hourly) == 2
    assert hourly[0].start.minute == 0
    assert hourly[0].minimum_v == 117.9
    assert hourly[0].maximum_v == 121.8
    assert hourly[0].average_v == 120.0
    assert len(daily) == 1


def test_history_window_is_timezone_aware_ordered_and_bounded() -> None:
    """Local offsets normalize to UTC while unsafe windows are rejected."""
    pacific = timezone(timedelta(hours=-7))
    start, end = _validate_window(
        datetime(2026, 8, 1, tzinfo=pacific),
        datetime(2026, 8, 2, tzinfo=pacific),
    )
    assert start == datetime(2026, 8, 1, 7, tzinfo=UTC)
    assert end == datetime(2026, 8, 2, 7, tzinfo=UTC)

    with pytest.raises(HomeAssistantError):
        _validate_window(datetime(2026, 8, 1), datetime(2026, 8, 2))
    with pytest.raises(HomeAssistantError):
        _validate_window(
            datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 3, 1, tzinfo=UTC)
        )
    with pytest.raises(ValueError, match="positive"):
        aggregate_voltage_history([], timedelta(0))


@pytest.mark.asyncio
async def test_fetch_chunks_sequentially_and_propagates_interruption() -> None:
    """Backfill requests stay daily and stop immediately on rate limiting."""
    client = MagicMock()
    client.get_voltage_history = AsyncMock(
        side_effect=[[], WhiskerRateLimitError("slow down")]
    )
    start = datetime(2026, 8, 1, tzinfo=UTC)

    with pytest.raises(WhiskerRateLimitError):
        await _fetch_history(client, "SERIAL-001", start, start + timedelta(days=3))

    assert client.get_voltage_history.await_args_list == [
        call("SERIAL-001", start, start + timedelta(days=1)),
        call("SERIAL-001", start + timedelta(days=1), start + timedelta(days=2)),
    ]


@pytest.mark.asyncio
async def test_service_imports_once_and_empty_history_writes_nothing(
    hass: HomeAssistant,
) -> None:
    """Explicit service imports deduplicated hourly statistics only after fetch."""
    point = VoltageHistoryPoint(
        datetime(2026, 8, 1, 0, 30, tzinfo=UTC), 118, 122, 120, 0.9
    )
    client = MagicMock(get_voltage_history=AsyncMock(return_value=[point, point]))
    coordinator = MagicMock(
        client=client,
        data={"SERIAL-001": DeviceState("SERIAL-001", "Test", "FireSensor", 1)},
    )
    entry = MagicMock(domain=DOMAIN, runtime_data=coordinator)
    async_register_history_service(hass)
    start = datetime(2026, 8, 1, tzinfo=UTC)

    with (
        patch.object(hass.config_entries, "async_get_entry", return_value=entry),
        patch(
            "custom_components.whisker_ting.history.async_add_external_statistics"
        ) as add_statistics,
    ):
        response = await hass.services.async_call(
            DOMAIN,
            SERVICE_IMPORT_VOLTAGE_HISTORY,
            {
                "config_entry_id": "entry-id",
                "serial_number": "SERIAL-001",
                "start": start.isoformat(),
                "end": (start + timedelta(hours=1)).isoformat(),
            },
            blocking=True,
            return_response=True,
        )
        assert response["hourly_statistics"] == 1
        assert add_statistics.call_count == 2
        voltage_data = add_statistics.call_args_list[0].args[2]
        assert voltage_data[0]["start"] == start

        client.get_voltage_history.return_value = []
        add_statistics.reset_mock()
        response = await hass.services.async_call(
            DOMAIN,
            SERVICE_IMPORT_VOLTAGE_HISTORY,
            {
                "config_entry_id": "entry-id",
                "serial_number": "SERIAL-001",
                "start": start,
                "end": start + timedelta(hours=1),
            },
            blocking=True,
            return_response=True,
        )
        assert response["hourly_statistics"] == 0
        add_statistics.assert_not_called()


@pytest.mark.asyncio
async def test_service_rejects_missing_entry_device_and_api_failure(
    hass: HomeAssistant,
) -> None:
    """History action failures are explicit and never write partial statistics."""
    async_register_history_service(hass)
    start = datetime(2026, 8, 1, tzinfo=UTC)
    data = {
        "config_entry_id": "entry-id",
        "serial_number": "SERIAL-001",
        "start": start,
        "end": start + timedelta(hours=1),
    }
    with (
        patch.object(hass.config_entries, "async_get_entry", return_value=None),
        pytest.raises(HomeAssistantError, match="Loaded"),
    ):
        await hass.services.async_call(
            DOMAIN,
            SERVICE_IMPORT_VOLTAGE_HISTORY,
            data,
            blocking=True,
            return_response=True,
        )

    client = MagicMock(get_voltage_history=AsyncMock())
    entry = MagicMock(domain=DOMAIN, runtime_data=MagicMock(client=client, data={}))
    with (
        patch.object(hass.config_entries, "async_get_entry", return_value=entry),
        pytest.raises(HomeAssistantError, match="not in this entry"),
    ):
        await hass.services.async_call(
            DOMAIN,
            SERVICE_IMPORT_VOLTAGE_HISTORY,
            data,
            blocking=True,
            return_response=True,
        )

    entry.runtime_data.data = {
        "SERIAL-001": DeviceState("SERIAL-001", "Test", "FireSensor", 1)
    }
    client.get_voltage_history.side_effect = WhiskerRateLimitError("limited")
    with (
        patch.object(hass.config_entries, "async_get_entry", return_value=entry),
        pytest.raises(HomeAssistantError, match="history import failed"),
    ):
        await hass.services.async_call(
            DOMAIN,
            SERVICE_IMPORT_VOLTAGE_HISTORY,
            data,
            blocking=True,
            return_response=True,
        )
