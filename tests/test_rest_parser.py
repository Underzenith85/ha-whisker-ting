"""Offline tests for sanitized REST response parsing."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from custom_components.whisker_ting import api

ROOT = Path(__file__).parents[1]
FIXTURES = Path(__file__).parent / "fixtures"


def _fixture(name: str) -> dict[str, Any]:
    """Load a sanitized REST fixture."""
    with (FIXTURES / name).open(encoding="utf-8") as file:
        value = json.load(file)
    assert isinstance(value, dict)
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
