"""Complete entity-platform and value-extractor coverage."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from homeassistant.core import HomeAssistant

from custom_components.whisker_ting.api import (
    DeviceState,
    FrozenPipeData,
    FrozenPipeRecord,
    Site,
    TingEvent,
)
from custom_components.whisker_ting.binary_sensor import (
    BINARY_SENSOR_DESCRIPTIONS,
    SITE_BINARY_SENSOR_DESCRIPTIONS,
    WhiskerBinarySensor,
    WhiskerSiteBinarySensor,
    _device_event_condition,
    _event_condition,
)
from custom_components.whisker_ting.binary_sensor import (
    async_setup_entry as async_setup_binary_sensors,
)
from custom_components.whisker_ting.sensor import (
    SENSOR_DESCRIPTIONS,
    SITE_SENSOR_DESCRIPTIONS,
    WhiskerSensor,
    WhiskerSiteSensor,
    _event_attributes,
    _event_timestamp,
    _get_frozen_pipe_last_event,
    _get_outage_risk_attributes,
    _get_outage_risk_state,
    _latest_voltage_condition,
    _realtime_sample_age,
)


def rich_state() -> tuple[DeviceState, Site]:
    """Return synthetic device and site values exercising all extractors."""
    events = [
        TingEvent(
            "VoltageSwell",
            "2026-08-22T02:00:00+00:00",
            event_kind="voltage_swell",
            event_id="event",
            category="power",
            title="Synthetic",
            message="Synthetic message",
        ),
        TingEvent(
            "PowerOutage",
            "2026-08-22T01:00:00+00:00",
            event_kind="power_outage",
        ),
        TingEvent("Malformed", "bad", event_kind="power_outage"),
    ]
    device = DeviceState(
        "SERIAL-001",
        "Synthetic device",
        "FireSensor",
        100,
        is_online=True,
        current_temperature_c=20,
        current_outage_risk={"status": "low", "ignored": object()},
        events=list(events),
        last_realtime_sample_utc=datetime.now(UTC) - timedelta(seconds=2),
    )
    device.voltage = device.voltage.with_voltage(
        voltage=120, voltage_hi=121, voltage_lo=119, average_peaks_max=4
    )
    device.voltage = device.voltage.with_frequency(60).with_thd_min(1)
    device.voltage = device.voltage.with_thd_average(2).with_thd_max(3)
    device.frozen_pipe = FrozenPipeData(
        status=FrozenPipeRecord(timestamp_utc=None, level=1),
        history=[
            FrozenPipeRecord(timestamp_utc="bad"),
            FrozenPipeRecord(timestamp_utc="2026-08-21T00:00:00Z"),
        ],
    )
    site = Site(
        100,
        42,
        "Synthetic site",
        current_temperature_c=20,
        current_outage_risk={"risk": 2},
        events=list(events),
    )
    return device, site


def test_every_entity_description_evaluates_rich_and_empty_state() -> None:
    """All generated descriptions safely evaluate present and absent optional data."""
    device, site = rich_state()
    for description in SENSOR_DESCRIPTIONS:
        description.value_fn(device)
        if description.attributes_fn:
            description.attributes_fn(device)
    for description in BINARY_SENSOR_DESCRIPTIONS:
        description.value_fn(device)
    for description in SITE_SENSOR_DESCRIPTIONS:
        description.value_fn(site)
        if description.attributes_fn:
            description.attributes_fn(site)
    for description in SITE_BINARY_SENSOR_DESCRIPTIONS:
        description.value_fn(site)


def test_entity_helpers_cover_empty_malformed_and_scalar_values() -> None:
    """Entity helpers return safe unknowns at malformed service boundaries."""
    assert _get_outage_risk_state("low") == "low"
    assert _get_outage_risk_state({"level": 3}) == 3
    assert _get_outage_risk_state({"status": True}) is None
    assert _get_outage_risk_state(None) is None
    assert _get_outage_risk_attributes("low") is None
    assert _get_outage_risk_attributes({"short": 1, "x" * 65: 2}) == {"short": 1}
    assert _event_attributes([]) is None
    assert _event_timestamp([], "power_outage") is None
    assert _latest_voltage_condition([]) is None
    assert (
        _latest_voltage_condition([TingEvent("Other", "bad", event_kind="other")])
        is None
    )
    assert _realtime_sample_age(DeviceState("S", "D", "T", 1)) is None
    future = DeviceState("S", "D", "T", 1)
    future.last_realtime_sample_utc = datetime.now(UTC) + timedelta(seconds=10)
    assert _realtime_sample_age(future) == 0
    assert _get_frozen_pipe_last_event(DeviceState("S", "D", "T", 1)) is None

    events = [
        TingEvent("On", "bad", event_kind="on"),
        TingEvent("Other", "2026-01-01T00:00:00Z", event_kind="other"),
    ]
    assert _event_condition(events, frozenset({"on"}), frozenset({"off"})) is None
    device = DeviceState("S", "D", "T", 1, is_online=False, events=events)
    assert not _device_event_condition(
        device, "device_online", frozenset({"on"}), frozenset({"off"})
    )


@pytest.mark.asyncio
async def test_platform_setup_and_entity_missing_state(hass: HomeAssistant) -> None:
    """Both platforms add site/device entities and tolerate removed state."""
    device, site = rich_state()
    coordinator = MagicMock(
        data={device.serial_number: device},
        sites={site.id: site},
        last_update_success=True,
    )
    entry = MagicMock(runtime_data=coordinator)
    add = MagicMock()
    await async_setup_binary_sensors(hass, entry, add)
    assert len(add.call_args.args[0]) == len(BINARY_SENSOR_DESCRIPTIONS) + len(
        SITE_BINARY_SENSOR_DESCRIPTIONS
    )

    binary = WhiskerBinarySensor(
        coordinator, device.serial_number, BINARY_SENSOR_DESCRIPTIONS[0]
    )
    site_binary = WhiskerSiteBinarySensor(
        coordinator, site.id, SITE_BINARY_SENSOR_DESCRIPTIONS[0]
    )
    coordinator.data = {}
    coordinator.sites = {}
    assert binary.is_on is None
    assert site_binary.is_on is None

    sensor = WhiskerSensor(coordinator, device.serial_number, SENSOR_DESCRIPTIONS[0])
    site_sensor = WhiskerSiteSensor(coordinator, site.id, SITE_SENSOR_DESCRIPTIONS[0])
    assert sensor.native_value is None
    assert sensor.extra_state_attributes is None
    assert site_sensor.native_value is None
    assert site_sensor.extra_state_attributes is None
