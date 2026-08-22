"""Tests for explicit Ting hazard status semantics."""

from __future__ import annotations

import pytest

from custom_components.whisker_ting.api import (
    DeviceState,
    FireHazardStatus,
    HazardStatus,
    TingEvent,
)
from custom_components.whisker_ting.binary_sensor import BINARY_SENSOR_DESCRIPTIONS
from custom_components.whisker_ting.sensor import (
    SENSOR_DESCRIPTIONS,
    _get_hazard_status,
)


def _device(
    efh_status: str | None = None,
    ufh_status: str | None = None,
    *,
    efh_level: int | None = None,
    ufh_level: int | None = None,
    severity: int | None = None,
    learning: bool = False,
    is_fire: bool = False,
) -> DeviceState:
    """Build a device with independently controlled hazard values."""
    return DeviceState(
        "SERIAL-001",
        "Fixture device",
        "FireSensor",
        1,
        is_fire=is_fire,
        fire_hazard_status=FireHazardStatus(
            learning_mode=learning,
            hazard_severity_level=severity,
            efh_status=HazardStatus(status=efh_status, level=efh_level),
            ufh_status=HazardStatus(status=ufh_status, level=ufh_level),
        ),
    )


@pytest.mark.parametrize(
    ("device", "expected"),
    [
        (_device(), "no_hazards"),
        (_device(learning=True), "learning"),
        (_device(is_fire=True), "fire_hazard"),
        (_device(efh_status="PossibleFire"), "fire_hazard"),
        (_device(efh_status="HazardFound"), "fire_hazard"),
        (_device(ufh_status="PowerQualityHazard"), "power_quality_hazard"),
        (_device(efh_status="ElevatedSuspicious"), "elevated_suspicious"),
        (_device(efh_status="ReviewedNotFire"), "reviewed_not_fire"),
        (_device(efh_status="FutureStatus"), "unknown"),
    ],
)
def test_overall_hazard_status_uses_explicit_semantics(
    device: DeviceState, expected: str
) -> None:
    """Known statuses remain distinct and unknown values fail safely."""
    assert _get_hazard_status(device) == expected


def test_positive_numeric_levels_do_not_create_false_hazards() -> None:
    """Numeric levels and severity are diagnostic rather than boolean truth."""
    device = _device(efh_level=2, ufh_level=1, severity=5)
    descriptions = {item.key: item for item in BINARY_SENSOR_DESCRIPTIONS}

    assert _get_hazard_status(device) == "no_hazards"
    assert not descriptions["electrical_fire_hazard"].value_fn(device)
    assert not descriptions["unverified_fire_hazard"].value_fn(device)


def test_binary_hazards_follow_known_status_values() -> None:
    """Fire-warning and power-quality binary sensors remain distinguishable."""
    descriptions = {item.key: item for item in BINARY_SENSOR_DESCRIPTIONS}
    electrical = _device(efh_status="ElevatedSuspicious")
    power_quality = _device(ufh_status="PowerQualityHazard")

    assert descriptions["electrical_fire_hazard"].value_fn(electrical)
    assert not descriptions["unverified_fire_hazard"].value_fn(electrical)
    assert not descriptions["electrical_fire_hazard"].value_fn(power_quality)
    assert descriptions["unverified_fire_hazard"].value_fn(power_quality)


def test_event_conditions_use_only_the_newest_valid_explicit_transition() -> None:
    """Clears, malformed records, missing history, and future types fail safely."""
    descriptions = {item.key: item for item in BINARY_SENSOR_DESCRIPTIONS}
    device = _device()
    assert descriptions["power_outage"].value_fn(device) is None

    device.events = [
        TingEvent(
            "FutureEvent",
            "2026-08-22T12:00:00+00:00",
            event_kind=None,
        ),
        TingEvent(
            "PowerRestored",
            "2026-08-22T11:00:00+00:00",
            event_kind="power_restored",
        ),
        TingEvent(
            "PowerOutage",
            "malformed",
            event_kind="power_outage",
        ),
        TingEvent(
            "PowerOutage",
            "2026-08-22T10:00:00+00:00",
            event_kind="power_outage",
        ),
    ]

    assert descriptions["power_outage"].value_fn(device) is False
    assert descriptions["generator_running"].value_fn(device) is None


def test_voltage_condition_distinguishes_latest_sag_from_swell() -> None:
    """Voltage excursions remain an enum because no normal transition is known."""
    description = next(
        item for item in SENSOR_DESCRIPTIONS if item.key == "voltage_condition"
    )
    device = _device()
    assert description.value_fn(device) is None
    device.events = [
        TingEvent(
            "VoltageSag",
            "2026-08-22T10:00:00+00:00",
            event_kind="voltage_sag",
        ),
        TingEvent(
            "VoltageSwell",
            "2026-08-22T11:00:00+00:00",
            event_kind="voltage_swell",
        ),
    ]

    assert description.value_fn(device) == "swell"
