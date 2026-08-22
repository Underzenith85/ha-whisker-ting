"""Offline tests for sanitized REST response parsing."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from custom_components.whisker_ting import api

ROOT = Path(__file__).parents[1]
FIXTURES = Path(__file__).parent / "fixtures"


def _fixture(name: str) -> Any:
    """Load a sanitized REST fixture."""
    with (FIXTURES / name).open(encoding="utf-8") as file:
        value = json.load(file)
    return value


def _client() -> Any:
    """Create a parser-only client."""
    return api.WhiskerApiClient(object(), "person@example.invalid")


def test_parses_normal_learning_efh_ufh_and_multiple_records() -> None:
    """All core hazard states and multiple devices/sites parse deterministically."""
    result = _client()._parse_user_data(_fixture("user_data_hazards.json"))

    assert result.user_id == 42
    assert len(result.devices) == 4
    assert len(result.sites) == 2
    by_serial = {device.serial_number: device for device in result.devices}

    assert not by_serial["NORMAL-001"].is_fire
    assert by_serial["NORMAL-001"].fire_hazard_status.hazard_severity_level == 4
    assert by_serial["NORMAL-001"].group_name == "Home"
    assert by_serial["LEARNING-002"].fire_hazard_status.learning_mode
    assert by_serial["EFH-003"].fire_hazard_status.efh_status.level == 2
    assert by_serial["UFH-004"].fire_hazard_status.ufh_status.level == 1
    assert by_serial["UFH-004"].device_type == "FutureSensorType"
    assert [site.id for site in result.sites] == [100, 200]


def test_missing_and_null_nested_fields_use_safe_defaults(
    caplog: Any,
) -> None:
    """Null containers cannot crash parsing or leak malformed records to state."""
    caplog.set_level(logging.WARNING)
    result = _client()._parse_user_data(_fixture("user_data_missing_null.json"))

    assert [device.serial_number for device in result.devices] == [
        "NULLS-001",
        "MISSING-002",
    ]
    null_device = result.devices[0]
    assert null_device.name == "NULLS-001"
    assert null_device.device_type == "Unknown"
    assert null_device.site_id == 0
    assert null_device.group_name is None
    assert null_device.fire_hazard_status.message == "No Hazards Detected"
    assert len(result.sites) == 1
    assert result.sites[0].display_name == ""

    assert "sensitive diagnostic marker" not in caplog.text
    assert "must be skipped" not in caplog.text
    assert "Skipping device at index" in caplog.text
    assert "Skipping site at index" in caplog.text


def test_parser_does_not_retain_raw_response() -> None:
    """Device state contains modeled values only, never the response object."""
    source = _fixture("user_data_hazards.json")
    device = _client()._parse_user_data(source).devices[0]

    assert not hasattr(device, "raw_data")
    assert source["devices"][0] not in vars(device).values()


def test_malformed_top_level_collections_are_empty() -> None:
    """Non-list collection values produce deterministic empty collections."""
    result = _client()._parse_user_data({"id": 42, "devices": None, "sites": {}})

    assert result.devices == []
    assert result.sites == []


@pytest.mark.asyncio
async def test_applies_3_0_4_conditions_by_site_and_device() -> None:
    """The current Ting conditions snapshot augments its full user response."""
    client = _client()
    device = api.DeviceState(
        serial_number="SERIAL-001",
        name="Home",
        device_type="FireSensor",
        site_id=100,
    )
    client.get_user_data = AsyncMock(
        return_value=api.UserData(42, "", "", "", devices=[device])
    )
    client.get_user_conditions = AsyncMock(
        return_value={
            "devices": [
                {
                    "serialNumber": "SERIAL-001",
                    "isFire": True,
                    "hasFrozenPipe": True,
                }
            ],
            "currentTemperatures": {"100": -3.5},
            "currentOutageRisks": {
                "100": {
                    "status": "elevated",
                    "level": 2,
                    "nested": {"must": "not be retained"},
                }
            },
        }
    )

    result = (await client.get_all_device_states())["SERIAL-001"]

    assert result.is_fire
    assert result.has_frozen_pipe
    assert result.current_temperature_c == -3.5
    assert result.current_outage_risk == {"status": "elevated", "level": 2}


def test_non_finite_coordinates_are_discarded() -> None:
    """NaN and infinity cannot be retained as site coordinates."""
    result = _client()._parse_user_data(
        {
            "sites": [
                {
                    "id": 1,
                    "latitude": float("nan"),
                    "longitude": float("inf"),
                }
            ]
        }
    )

    assert result.sites[0].latitude is None
    assert result.sites[0].longitude is None


def test_parses_frozen_pipe_status_and_history_without_raw_data() -> None:
    """Known frozen-pipe fields parse while unknown fields are discarded."""
    client = _client()
    status = client._parse_frozen_pipe_record(
        _fixture("frozen_pipe_status_active.json")
    )
    history = client._parse_frozen_pipe_history(_fixture("frozen_pipe_history.json"))

    assert status is not None
    assert status.level == 55
    assert status.outdoor_temperature_c == -8.5
    assert status.detected_location_type == "UnconditionedSpace"
    assert status.timestamp_utc == "2026-01-15T03:04:05Z"
    assert status.notification_type == "TA1"
    assert not hasattr(status, "ignored_sensitive_field")
    assert len(history) == 2
    assert history[0].resolved_timestamp_utc == "2026-01-14T06:00:00Z"
    assert history[0].user_action == "ActionTurnedOnHeat"


def test_frozen_pipe_parser_handles_empty_and_malformed_responses() -> None:
    """Optional malformed responses produce safe empty models."""
    client = _client()

    assert client._parse_frozen_pipe_record(None) is None
    assert client._parse_frozen_pipe_record([]) is None
    assert client._parse_frozen_pipe_record({"unexpected": "value"}) is None
    assert client._parse_frozen_pipe_history(None) == []
    assert client._parse_frozen_pipe_history({"history": [None, "bad"]}) == []


@pytest.mark.asyncio
async def test_fetches_status_and_history_with_encoded_serial() -> None:
    """Both read-only endpoints are fetched and combined per station."""
    client = _client()
    client._get_optional_data = AsyncMock(
        side_effect=[
            _fixture("frozen_pipe_status_active.json"),
            _fixture("frozen_pipe_history.json"),
        ]
    )

    result = await client.get_frozen_pipe_data("SERIAL/001")

    assert result.status is not None
    assert result.status.level == 55
    assert len(result.history) == 2
    assert [call.args[0] for call in client._get_optional_data.await_args_list] == [
        "/api/v1/FrozenPipe/SERIAL%2F001",
        "/api/v1/FrozenPipe/SERIAL%2F001/currentHistory",
    ]


@pytest.mark.asyncio
async def test_optional_endpoint_failure_returns_empty_data() -> None:
    """Unavailable optional feature endpoints do not fail the account update."""
    client = _client()
    client._request = AsyncMock(side_effect=api.WhiskerApiError("not available"))

    result = await client.get_frozen_pipe_data("SERIAL-001")

    assert result.status is None
    assert result.history == []


def test_event_history_is_scoped_normalized_and_sorted() -> None:
    """Unknown event types survive while malformed and unscoped data is omitted."""
    events = _client()._parse_event_history(_fixture("notification_history.json"))

    assert [event.event_id for event in events] == [
        "event-new",
        "other-station",
        "event-old",
    ]
    assert events[2].event_type == "FutureEventType"
    assert events[2].timestamp_utc == "2026-08-20T00:00:00+00:00"
    assert not hasattr(events[0], "statuses")


@pytest.mark.asyncio
async def test_event_history_request_is_read_only_and_bounded() -> None:
    """History retrieval supplies a bounded date window and exclusion flags."""
    client = _client()
    client._user_id = 42
    client._get_optional_data = AsyncMock(
        return_value=_fixture("notification_history.json")
    )

    events = await client.get_event_history(days=30)

    assert len(events) == 3
    endpoint = client._get_optional_data.await_args.args[0]
    params = client._get_optional_data.await_args.kwargs["params"]
    assert endpoint == "/api/v1/Notifications/history/42"
    assert params["excludeStatuses"] == "true"
    assert params["excludeCleared"] == "false"
    assert params["sentStartUtc"] < params["sentEndUtc"]
