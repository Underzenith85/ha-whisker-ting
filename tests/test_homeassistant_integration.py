"""Home Assistant fixture tests for integration lifecycle behavior."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.config_entries import SOURCE_USER
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.whisker_ting import async_unload_entry
from custom_components.whisker_ting.api import (
    DeviceState,
    FrozenPipeData,
    Site,
    TingEvent,
    UserData,
    WhiskerApiError,
)
from custom_components.whisker_ting.binary_sensor import (
    BINARY_SENSOR_DESCRIPTIONS,
    WhiskerBinarySensor,
    WhiskerBinarySensorEntityDescription,
)
from custom_components.whisker_ting.const import (
    CONF_API_KEY,
    CONF_PASSWORD,
    CONF_REFRESH_TOKEN,
    CONF_USER_ID,
    CONF_USERNAME,
    DOMAIN,
)
from custom_components.whisker_ting.coordinator import (
    WhiskerDataUpdateCoordinator,
)
from custom_components.whisker_ting.sensor import (
    SENSOR_DESCRIPTIONS,
    SITE_SENSOR_DESCRIPTIONS,
    WhiskerSensor,
    WhiskerSensorEntityDescription,
    WhiskerSiteSensor,
)
from custom_components.whisker_ting.sensor import (
    async_setup_entry as async_setup_sensor_entry,
)
from custom_components.whisker_ting.stream import (
    PowerQualityCategory,
    PowerQualityData,
    VoltageData,
)


@pytest.mark.parametrize(
    "description", [*SENSOR_DESCRIPTIONS, *BINARY_SENSOR_DESCRIPTIONS]
)
def test_entity_descriptions_use_translation_keys(
    description: WhiskerSensorEntityDescription | WhiskerBinarySensorEntityDescription,
) -> None:
    """Entity display names must come from Home Assistant translations."""
    assert description.translation_key
    assert not isinstance(description.name, str)


@pytest.mark.asyncio
@pytest.mark.usefixtures("enable_custom_integrations")
async def test_config_flow_uses_ha_fixture_and_drops_password(
    hass: HomeAssistant,
) -> None:
    """A successful HA config flow persists renewable credentials, not a password."""
    client = MagicMock()
    client.get_user_data = AsyncMock(
        return_value=UserData(
            user_id=42,
            email="person@example.invalid",
            first_name="Example",
            last_name="User",
        )
    )
    client.refresh_token = "fixture-refresh-token"
    client.user_id = 42
    client.api_key = "fixture-api-key"

    with (
        patch(
            "custom_components.whisker_ting.config_flow.WhiskerApiClient",
            return_value=client,
        ),
        patch(
            "custom_components.whisker_ting.async_setup_entry",
            AsyncMock(return_value=True),
        ),
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_USER},
            data={
                CONF_USERNAME: "person@example.invalid",
                CONF_PASSWORD: "fixture-password",
            },
        )
        result["result"].runtime_data = MagicMock(async_shutdown=AsyncMock())
        await hass.async_block_till_done()

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"] == {
        CONF_USERNAME: "person@example.invalid",
        CONF_REFRESH_TOKEN: "fixture-refresh-token",
        CONF_USER_ID: 42,
        CONF_API_KEY: "fixture-api-key",
    }
    assert CONF_PASSWORD not in result["data"]


@pytest.mark.asyncio
async def test_unload_shuts_down_coordinator_and_platforms(
    hass: HomeAssistant,
) -> None:
    """Entry unload closes streaming resources before unloading entities."""
    coordinator = MagicMock()
    coordinator.async_shutdown = AsyncMock()
    entry = MagicMock()
    entry.runtime_data = coordinator

    with patch.object(
        hass.config_entries,
        "async_unload_platforms",
        AsyncMock(return_value=True),
    ) as unload_platforms:
        assert await async_unload_entry(hass, entry)

    coordinator.async_shutdown.assert_awaited_once_with()
    unload_platforms.assert_awaited_once()


@pytest.mark.parametrize(
    ("entity_class", "description"),
    [
        (WhiskerSensor, SENSOR_DESCRIPTIONS[0]),
        (WhiskerBinarySensor, BINARY_SENSOR_DESCRIPTIONS[0]),
    ],
)
def test_entity_availability_tracks_coordinator_device_membership(
    entity_class: type,
    description: object,
) -> None:
    """Sensor and binary-sensor availability require current coordinator data."""
    device = DeviceState("SERIAL-001", "Fixture device", "FireSensor", 1)
    coordinator = MagicMock()
    coordinator.last_update_success = True
    coordinator.data = {device.serial_number: device}
    coordinator.is_realtime_available.return_value = True
    entity = entity_class(coordinator, device.serial_number, description)

    assert entity.available
    coordinator.data = {}
    assert not entity.available


def test_shared_entity_device_info_handles_present_and_missing_state() -> None:
    """All platforms retain stable registry identity when REST state disappears."""
    device = DeviceState(
        "SERIAL-001", "Fixture device", "FireSensor", 1, version="3.0.4"
    )
    coordinator = MagicMock()
    coordinator.last_update_success = True
    coordinator.data = {device.serial_number: device}
    entity = WhiskerBinarySensor(
        coordinator, device.serial_number, BINARY_SENSOR_DESCRIPTIONS[0]
    )

    assert entity.unique_id == "SERIAL-001_fire_hazard"
    assert entity.device_info["name"] == "Fixture device"
    assert entity.device_info["sw_version"] == "3.0.4"

    coordinator.data = {}
    assert entity.device_info["identifiers"] == {("whisker_ting", "SERIAL-001")}
    assert entity.device_info["name"] == "SERIAL-001"


@pytest.mark.asyncio
async def test_site_entities_are_unique_per_site_and_migrate_legacy_duplicates(
    hass: HomeAssistant,
) -> None:
    """Shared site conditions produce one entity while retaining one entity ID."""
    first = DeviceState("SERIAL-001", "First device", "FireSensor", 100)
    second = DeviceState("SERIAL-002", "Second device", "FireSensor", 100)
    site = Site(
        100,
        42,
        "Home",
        address_line1="123 Private Street",
        latitude=40.1,
        longitude=-74.2,
        current_temperature_c=20.5,
    )
    coordinator = MagicMock()
    coordinator.data = {first.serial_number: first, second.serial_number: second}
    coordinator.sites = {site.id: site}
    registry = er.async_get(hass)
    retained = registry.async_get_or_create(
        "sensor",
        DOMAIN,
        "SERIAL-001_current_outdoor_temperature",
        suggested_object_id="legacy_site_temperature",
    )
    duplicate = registry.async_get_or_create(
        "sensor",
        DOMAIN,
        "SERIAL-002_current_outdoor_temperature",
        suggested_object_id="duplicate_site_temperature",
    )
    entry = MagicMock(runtime_data=coordinator)
    add_entities = MagicMock()

    await async_setup_sensor_entry(hass, entry, add_entities)

    entities = add_entities.call_args.args[0]
    site_entities = [
        entity for entity in entities if isinstance(entity, WhiskerSiteSensor)
    ]
    assert len(site_entities) == len(SITE_SENSOR_DESCRIPTIONS)
    temperature = next(
        entity
        for entity in site_entities
        if entity.entity_description.key == "current_outdoor_temperature"
    )
    assert temperature.unique_id == "site_100_current_outdoor_temperature"
    assert temperature.native_value == 20.5
    assert temperature.device_info["name"] == "Home"
    assert "123 Private Street" not in str(temperature.device_info)
    assert "40.1" not in str(temperature.device_info)
    assert (
        registry.async_get(retained.entity_id).unique_id
        == "site_100_current_outdoor_temperature"
    )
    assert registry.async_get(duplicate.entity_id) is None


def test_site_and_device_registry_identity_survive_renames_and_missing_sites() -> None:
    """Stable IDs do not depend on a site name and missing sites are unavailable."""
    device = DeviceState("SERIAL-001", "Device", "FireSensor", 100)
    site = Site(100, 42, "Original")
    coordinator = MagicMock()
    coordinator.last_update_success = True
    coordinator.data = {device.serial_number: device}
    coordinator.sites = {site.id: site}
    entity = WhiskerSiteSensor(coordinator, site.id, SITE_SENSOR_DESCRIPTIONS[0])
    device_entity = WhiskerSensor(
        coordinator, device.serial_number, SENSOR_DESCRIPTIONS[0]
    )

    assert entity.unique_id == "site_100_current_outdoor_temperature"
    assert device_entity.device_info["via_device"] == (DOMAIN, "site_100")
    site.display_name = "Renamed"
    assert entity.unique_id == "site_100_current_outdoor_temperature"
    assert entity.device_info["name"] == "Renamed"
    coordinator.sites = {}
    assert not entity.available


def test_structured_event_entities_select_newest_matching_event() -> None:
    """Known transitions expose timestamps while future types remain generic."""
    device = DeviceState("SERIAL-001", "Device", "FireSensor", 100)
    device.events = [
        TingEvent(
            "FutureEventType",
            "2026-08-22T12:00:00+00:00",
            serial_number=device.serial_number,
        ),
        TingEvent(
            "PowerRestored",
            "2026-08-22T11:00:00+00:00",
            serial_number=device.serial_number,
            event_kind="power_restored",
        ),
        TingEvent(
            "PowerRestored",
            "malformed",
            serial_number=device.serial_number,
            event_kind="power_restored",
        ),
    ]
    coordinator = MagicMock()
    coordinator.last_update_success = True
    coordinator.data = {device.serial_number: device}
    descriptions = {description.key: description for description in SENSOR_DESCRIPTIONS}

    latest = WhiskerSensor(
        coordinator, device.serial_number, descriptions["latest_event"]
    )
    restoration = WhiskerSensor(
        coordinator, device.serial_number, descriptions["last_power_restoration"]
    )

    assert latest.native_value == "FutureEventType"
    assert restoration.native_value == datetime(2026, 8, 22, 11, tzinfo=UTC)


@pytest.mark.asyncio
async def test_site_only_events_are_not_assigned_to_sensor_devices(
    hass: HomeAssistant,
) -> None:
    """Coordinator scopes events to their explicit device or site owner."""
    device = DeviceState("SERIAL-001", "Device", "FireSensor", 100)
    site = Site(100, 42, "Home")
    client = MagicMock(api_key=None, user_id=None)
    client.sites = {site.id: site}
    client.get_all_device_states = AsyncMock(
        return_value={device.serial_number: device}
    )
    client.get_event_history = AsyncMock(
        return_value=[
            TingEvent(
                "PowerOutage",
                "2026-08-22T10:00:00+00:00",
                site_id=site.id,
                event_kind="power_outage",
            ),
            TingEvent(
                "DeviceOffline",
                "2026-08-22T09:00:00+00:00",
                serial_number=device.serial_number,
                event_kind="device_offline",
            ),
            TingEvent(
                "PowerOutage",
                "2026-08-22T08:00:00+00:00",
                site_id=999,
                event_kind="power_outage",
            ),
        ]
    )
    client.get_frozen_pipe_data = AsyncMock(return_value=FrozenPipeData())
    manager = MagicMock()
    manager.disconnect_all = AsyncMock()

    with patch(
        "custom_components.whisker_ting.coordinator.WhiskerWebSocketManager",
        return_value=manager,
    ):
        coordinator = WhiskerDataUpdateCoordinator(hass, client, MagicMock())
        await coordinator._async_update_data()
        await coordinator.async_shutdown()

    assert [event.event_type for event in device.events] == ["DeviceOffline"]
    assert [event.event_type for event in site.events] == ["PowerOutage"]


def test_only_realtime_entities_become_unavailable_on_stream_loss() -> None:
    """A frozen voltage is hidden while REST-backed device state stays available."""
    device = DeviceState("SERIAL-001", "Fixture device", "FireSensor", 1)
    device.voltage.voltage = 120.0
    coordinator = MagicMock()
    coordinator.last_update_success = True
    coordinator.data = {device.serial_number: device}
    coordinator.is_realtime_available.return_value = False

    descriptions = {item.key: item for item in SENSOR_DESCRIPTIONS}
    voltage = WhiskerSensor(coordinator, device.serial_number, descriptions["voltage"])
    hazard = WhiskerSensor(
        coordinator, device.serial_number, descriptions["hazard_status"]
    )

    assert voltage.native_value == 120.0
    assert not voltage.available
    assert hazard.available


@pytest.mark.asyncio
async def test_coordinator_connects_stream_once_and_disconnects_on_shutdown(
    hass: HomeAssistant,
) -> None:
    """Repeated REST updates do not create duplicate stream connections."""
    device = DeviceState(
        "SERIAL-001", "Fixture device", "FireSensor", 1, station_id="SERIAL-001"
    )
    client = MagicMock()
    client.api_key = "fixture-api-key"
    client.user_id = 42
    client.get_all_device_states = AsyncMock(
        side_effect=[{device.serial_number: device}, {device.serial_number: device}]
    )
    client.get_frozen_pipe_data = AsyncMock(return_value=FrozenPipeData())
    client.get_event_history = AsyncMock(return_value=[])
    manager = MagicMock()
    manager.connect_device = AsyncMock(return_value=True)
    manager.wait_for_data = AsyncMock(return_value=False)
    manager.get_voltage_data.return_value = None
    manager.disconnect_all = AsyncMock()
    manager.disconnect_all = AsyncMock()
    manager.is_station_managed.side_effect = [False, True, True, True]

    with patch(
        "custom_components.whisker_ting.coordinator.WhiskerWebSocketManager",
        return_value=manager,
    ):
        coordinator = WhiskerDataUpdateCoordinator(hass, client, MagicMock())
        await coordinator._async_update_data()
        await coordinator._async_update_data()
        await coordinator.async_shutdown()

    manager.connect_device.assert_awaited_once_with(
        api_key="fixture-api-key", user_id=42, station_id="SERIAL-001"
    )
    manager.disconnect_all.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_coordinator_throttles_repeated_api_failure_logs(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Repeated API failures raise updates but log the warning only once."""
    client = MagicMock()
    client.get_all_device_states = AsyncMock(
        side_effect=WhiskerApiError("sanitized fixture failure")
    )
    coordinator = WhiskerDataUpdateCoordinator(hass, client, MagicMock())
    caplog.set_level(logging.WARNING)

    with pytest.raises(UpdateFailed):
        await coordinator._async_update_data()
    with pytest.raises(UpdateFailed):
        await coordinator._async_update_data()

    assert caplog.text.count("Unable to connect to Whisker Ting API") == 1


@pytest.mark.asyncio
async def test_voltage_listener_updates_are_throttled_to_one_hz(
    hass: HomeAssistant,
) -> None:
    """Stream bursts retain the newest reading but notify at most once per second."""
    device = DeviceState(
        "SERIAL-001", "Fixture device", "FireSensor", 1, station_id="SERIAL-001"
    )
    coordinator = WhiskerDataUpdateCoordinator(hass, MagicMock(), MagicMock())
    coordinator.data = {device.serial_number: device}
    coordinator.async_set_updated_data = MagicMock()

    for voltage in (120.0, 121.0, 122.0, 123.0):
        coordinator._handle_voltage_update(
            "SERIAL-001",
            VoltageData(datetime.now(UTC), voltage, voltage + 1, voltage - 1, 4),
        )

    assert coordinator.async_set_updated_data.call_count == 1
    assert device.voltage.voltage == 123.0
    await asyncio.sleep(coordinator.STREAM_UPDATE_INTERVAL + 0.05)
    assert coordinator.async_set_updated_data.call_count == 2
    await coordinator.async_shutdown()


@pytest.mark.asyncio
async def test_interleaved_live_metrics_survive_rest_refresh(
    hass: HomeAssistant,
) -> None:
    """Typed voltage and power-quality samples merge across a REST snapshot."""
    first = DeviceState(
        "SERIAL-001", "Fixture device", "FireSensor", 1, station_id="SERIAL-001"
    )
    refreshed = DeviceState(
        "SERIAL-001", "Fixture device", "FireSensor", 1, station_id="SERIAL-001"
    )
    client = MagicMock(api_key="fixture-api-key", user_id=42)
    client.get_all_device_states = AsyncMock(
        side_effect=[{first.serial_number: first}, {refreshed.serial_number: refreshed}]
    )
    client.get_frozen_pipe_data = AsyncMock(return_value=FrozenPipeData())
    client.get_event_history = AsyncMock(return_value=[])
    manager = MagicMock()
    manager.is_station_managed.return_value = True
    manager.wait_for_data = AsyncMock(return_value=False)
    manager.get_voltage_data.return_value = None
    manager.disconnect_all = AsyncMock()

    with patch(
        "custom_components.whisker_ting.coordinator.WhiskerWebSocketManager",
        return_value=manager,
    ):
        coordinator = WhiskerDataUpdateCoordinator(hass, client, MagicMock())
        coordinator.data = await coordinator._async_update_data()
        coordinator._handle_voltage_update(
            "SERIAL-001", VoltageData(datetime.now(UTC), 120, 121, 119, 4)
        )
        coordinator._handle_power_quality_update(
            "SERIAL-001",
            PowerQualityData(datetime.now(UTC), PowerQualityCategory.FREQUENCY, 60.01),
        )
        coordinator._handle_power_quality_update(
            "SERIAL-001",
            PowerQualityData(datetime.now(UTC), PowerQualityCategory.THD_AVERAGE, 2.4),
        )

        result = await coordinator._async_update_data()
        await coordinator.async_shutdown()

    reading = result["SERIAL-001"].voltage
    assert reading.voltage == 120
    assert reading.frequency_hz == 60.01
    assert reading.thd_avg_percent == 2.4
