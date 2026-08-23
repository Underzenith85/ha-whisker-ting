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
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.update_coordinator import UpdateFailed
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.whisker_ting import (
    _register_site_devices,
    async_setup_entry,
    async_unload_entry,
)
from custom_components.whisker_ting.api import (
    DeviceState,
    FrozenPipeData,
    Site,
    TingEvent,
    UserData,
    WhiskerApiError,
    WhiskerAuthError,
    WhiskerConnectionError,
)
from custom_components.whisker_ting.binary_sensor import (
    BINARY_SENSOR_DESCRIPTIONS,
    SITE_BINARY_SENSOR_DESCRIPTIONS,
    WhiskerBinarySensor,
    WhiskerBinarySensorEntityDescription,
    WhiskerSiteBinarySensorEntityDescription,
)
from custom_components.whisker_ting.const import (
    CONF_API_KEY,
    CONF_PASSWORD,
    CONF_REFRESH_TOKEN,
    CONF_SCAN_INTERVAL,
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
    WhiskerSiteSensorEntityDescription,
)
from custom_components.whisker_ting.sensor import (
    async_setup_entry as async_setup_sensor_entry,
)
from custom_components.whisker_ting.stream import (
    PowerQualityCategory,
    PowerQualityData,
    StationDiagnostics,
    StreamHealth,
    VoltageData,
)


@pytest.mark.parametrize(
    "description",
    [
        *SENSOR_DESCRIPTIONS,
        *SITE_SENSOR_DESCRIPTIONS,
        *BINARY_SENSOR_DESCRIPTIONS,
        *SITE_BINARY_SENSOR_DESCRIPTIONS,
    ],
)
def test_entity_descriptions_use_translation_keys(
    description: WhiskerSensorEntityDescription
    | WhiskerSiteSensorEntityDescription
    | WhiskerBinarySensorEntityDescription
    | WhiskerSiteBinarySensorEntityDescription,
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


@pytest.mark.asyncio
async def test_site_devices_are_registered_before_children_and_reused_on_reload(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Parent sites exist before via_device use and retain identity on reload."""
    entry = MockConfigEntry(domain=DOMAIN, title="Fixture account")
    entry.add_to_hass(hass)
    sites = {
        100: Site(100, 42, "Home"),
        200: Site(200, 42, "Workshop"),
    }
    coordinator = MagicMock(sites=sites)
    registry = dr.async_get(hass)

    _register_site_devices(hass, entry, coordinator)
    site_devices = {
        site_id: registry.async_get_device(identifiers={(DOMAIN, f"site_{site_id}")})
        for site_id in sites
    }
    assert all(site_devices.values())

    caplog.clear()
    child = registry.async_get_or_create(
        config_entry_id=entry.entry_id,
        identifiers={(DOMAIN, "SERIAL-001")},
        name="Fixture sensor",
        via_device=(DOMAIN, "site_100"),
    )
    assert child.via_device_id == site_devices[100].id
    assert "non existing `via_device`" not in caplog.text

    sites[100].display_name = "Renamed home"
    _register_site_devices(hass, entry, coordinator)
    reloaded = registry.async_get_device(identifiers={(DOMAIN, "site_100")})
    assert reloaded.id == site_devices[100].id
    assert reloaded.name == "Renamed home"
    assert len(registry.devices) == 3


@pytest.mark.asyncio
async def test_entry_setup_registers_site_before_forwarding_platforms(
    hass: HomeAssistant,
) -> None:
    """Initial entity forwarding cannot race parent site registration."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        title="Fixture account",
        data={
            CONF_USERNAME: "person@example.invalid",
            CONF_REFRESH_TOKEN: "fixture-refresh-token",
            CONF_USER_ID: 42,
            CONF_API_KEY: "fixture-api-key",
        },
    )
    entry.add_to_hass(hass)
    client = MagicMock(
        test_connection=AsyncMock(return_value=True),
        refresh_token="fixture-refresh-token",
        user_id=42,
        api_key="fixture-api-key",
    )
    coordinator = MagicMock(
        sites={100: Site(100, 42, "Home")},
        data={"SERIAL-001": DeviceState("SERIAL-001", "Sensor", "FireSensor", 100)},
        async_config_entry_first_refresh=AsyncMock(),
    )

    async def assert_site_exists_before_forwarding(*args: object) -> None:
        registry = dr.async_get(hass)
        assert registry.async_get_device(identifiers={(DOMAIN, "site_100")})

    with (
        patch("custom_components.whisker_ting.WhiskerApiClient", return_value=client),
        patch(
            "custom_components.whisker_ting.WhiskerDataUpdateCoordinator",
            return_value=coordinator,
        ),
        patch.object(
            hass.config_entries,
            "async_forward_entry_setups",
            AsyncMock(side_effect=assert_site_exists_before_forwarding),
        ) as forward_setups,
    ):
        assert await async_setup_entry(hass, entry)

    forward_setups.assert_awaited_once()


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
async def test_rest_failure_is_independent_from_retained_stream_diagnostics(
    hass: HomeAssistant,
) -> None:
    """A failed REST refresh retains stream timestamps and marks only REST unhealthy."""
    device = DeviceState("SERIAL-001", "Device", "FireSensor", 100)
    sample_time = datetime(2026, 8, 22, 10, tzinfo=UTC)
    device.last_realtime_sample_utc = sample_time
    client = MagicMock(api_key=None, user_id=None, sites={})
    client.get_all_device_states = AsyncMock(
        side_effect=[
            {device.serial_number: device},
            WhiskerApiError("sanitized REST failure"),
        ]
    )
    client.get_event_history = AsyncMock(return_value=[])
    client.get_frozen_pipe_data = AsyncMock(return_value=FrozenPipeData())
    manager = MagicMock()
    manager.disconnect_all = AsyncMock()

    with patch(
        "custom_components.whisker_ting.coordinator.WhiskerWebSocketManager",
        return_value=manager,
    ):
        coordinator = WhiskerDataUpdateCoordinator(hass, client, MagicMock())
        coordinator.data = await coordinator._async_update_data()
        successful_update = device.last_rest_update_utc
        with pytest.raises(UpdateFailed):
            await coordinator._async_update_data()
        await coordinator.async_shutdown()

    assert device.rest_health == "error"
    assert device.last_rest_update_utc == successful_update
    assert device.last_realtime_sample_utc == sample_time


@pytest.mark.asyncio
async def test_stream_diagnostics_are_independent_and_listener_throttled(
    hass: HomeAssistant,
) -> None:
    """One station's diagnostics do not change another or bypass the throttle."""
    first = DeviceState(
        "SERIAL-001", "First", "FireSensor", 100, station_id="STATION-A"
    )
    second = DeviceState(
        "SERIAL-002", "Second", "FireSensor", 100, station_id="STATION-B"
    )
    coordinator = WhiskerDataUpdateCoordinator(hass, MagicMock(), MagicMock())
    coordinator.data = {first.serial_number: first, second.serial_number: second}
    coordinator.async_set_updated_data = MagicMock()
    sample_time = datetime(2026, 8, 22, 10, tzinfo=UTC)
    reconnect_time = datetime(2026, 8, 22, 10, 5, tzinfo=UTC)

    coordinator._handle_stream_diagnostics_update(
        "STATION-A",
        StationDiagnostics(
            last_sample_utc=sample_time,
            reconnect_count=2,
            last_reconnect_utc=reconnect_time,
            last_reconnect_reason="transport closed",
        ),
    )
    coordinator._handle_stream_diagnostics_update(
        "STATION-A",
        StationDiagnostics(last_sample_utc=sample_time, reconnect_count=3),
    )

    assert first.stream_reconnect_count == 3
    assert first.last_realtime_sample_utc == sample_time
    assert second.stream_reconnect_count == 0
    assert second.last_realtime_sample_utc is None
    assert coordinator.async_set_updated_data.call_count == 1
    await asyncio.sleep(coordinator.STREAM_UPDATE_INTERVAL + 0.05)
    assert coordinator.async_set_updated_data.call_count == 2
    await coordinator.async_shutdown()


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


@pytest.mark.asyncio
async def test_coordinator_handlers_cover_absent_data_and_all_quality_metrics(
    hass: HomeAssistant,
) -> None:
    """Callbacks safely ignore absent data and apply every quality category."""
    coordinator = WhiskerDataUpdateCoordinator(hass, MagicMock(), MagicMock())
    voltage = VoltageData(datetime.now(UTC), 120, 121, 119, 4)
    coordinator._schedule_stream_listener_update()
    coordinator._flush_stream_listener_update()
    coordinator._handle_voltage_update("station", voltage)
    coordinator._handle_power_quality_update(
        "station",
        PowerQualityData(datetime.now(UTC), PowerQualityCategory.THD_MIN, 1),
    )
    assert not coordinator.is_realtime_available("missing")

    device = DeviceState("SERIAL", "Device", "Type", 1, station_id="station")
    coordinator.data = {device.serial_number: device}
    coordinator.async_set_updated_data = MagicMock()
    coordinator._handle_availability_update("station", True)
    coordinator._handle_stream_health_update("station", StreamHealth.RECEIVING)
    coordinator._handle_stream_diagnostics_update(
        "station",
        StationDiagnostics(
            last_sample_utc=datetime.now(UTC),
            reconnect_count=2,
            last_reconnect_reason="synthetic",
        ),
    )
    for category, value in (
        (PowerQualityCategory.THD_MIN, 1),
        (PowerQualityCategory.THD_AVERAGE, 2),
        (PowerQualityCategory.THD_MAX, 3),
    ):
        coordinator._handle_power_quality_update(
            "station", PowerQualityData(datetime.now(UTC), category, value)
        )
    assert device.voltage.thd_min_percent == 1
    assert device.voltage.thd_max_percent == 3
    await coordinator.async_shutdown()


@pytest.mark.asyncio
async def test_coordinator_websocket_skip_empty_credentials_and_connection_error(
    hass: HomeAssistant,
) -> None:
    """Stream setup handles empty data, absent credentials, and client failure."""
    client = MagicMock(api_key=None, user_id=None)
    manager = MagicMock(
        is_station_managed=MagicMock(return_value=False),
        connect_device=AsyncMock(side_effect=RuntimeError("synthetic")),
        disconnect_all=AsyncMock(),
    )
    with patch(
        "custom_components.whisker_ting.coordinator.WhiskerWebSocketManager",
        return_value=manager,
    ):
        coordinator = WhiskerDataUpdateCoordinator(hass, client, MagicMock())
        await coordinator._connect_websocket({})
        await coordinator._connect_websocket(
            {"SERIAL": DeviceState("SERIAL", "Device", "Type", 1, station_id="s")}
        )
        client.api_key = "key"
        client.user_id = 42
        await coordinator._connect_websocket(
            {"SERIAL": DeviceState("SERIAL", "Device", "Type", 1, station_id="s")}
        )
        await coordinator.async_shutdown()
    manager.connect_device.assert_awaited_once()


@pytest.mark.asyncio
async def test_coordinator_auth_failure_and_successful_repair_evaluation(
    hass: HomeAssistant,
) -> None:
    """Account auth failures create Repairs and successful refreshes clear them."""
    repair = MagicMock()
    client = MagicMock(
        api_key=None,
        user_id=None,
        sites={},
        unauthorized_capabilities={"conditions"},
    )
    client.get_all_device_states = AsyncMock(side_effect=WhiskerAuthError("expired"))
    coordinator = WhiskerDataUpdateCoordinator(
        hass, client, MagicMock(), repair_manager=repair
    )
    coordinator.data = {"OLD": DeviceState("OLD", "Old", "Type", 1)}
    with pytest.raises(ConfigEntryAuthFailed):
        await coordinator._async_update_data()
    assert coordinator.data["OLD"].rest_health == "error"
    repair.create_authentication_issue.assert_called_once()

    device = DeviceState("SERIAL", "Device", "Type", 1)
    client.get_all_device_states = AsyncMock(return_value={"SERIAL": device})
    client.get_event_history = AsyncMock(return_value=[])
    client.get_frozen_pipe_data = AsyncMock(return_value=FrozenPipeData())
    coordinator.data = None
    result = await coordinator._async_update_data()
    assert result == {"SERIAL": device}
    repair.clear_authentication_issue.assert_called_once()
    repair.evaluate.assert_called_once()
    await coordinator.async_shutdown()


@pytest.mark.asyncio
async def test_frozen_pipe_enrichment_isolates_optional_device_failure(
    hass: HomeAssistant,
) -> None:
    """One optional enrichment failure does not discard another device's data."""
    detail = FrozenPipeData()
    client = MagicMock()
    client.get_frozen_pipe_data = AsyncMock(
        side_effect=[detail, WhiskerApiError("optional failure")]
    )
    coordinator = WhiskerDataUpdateCoordinator(hass, client, MagicMock())
    first = DeviceState("FIRST", "First", "Type", 1)
    second = DeviceState("SECOND", "Second", "Type", 1)

    await coordinator._async_enrich_frozen_pipe({"FIRST": first, "SECOND": second})

    assert first.frozen_pipe is detail
    assert second.frozen_pipe == FrozenPipeData()


@pytest.mark.asyncio
async def test_frozen_pipe_enrichment_propagates_cancellation(
    hass: HomeAssistant,
) -> None:
    """Coordinator cancellation cannot be downgraded to optional data loss."""
    client = MagicMock()
    client.get_frozen_pipe_data = AsyncMock(side_effect=asyncio.CancelledError)
    coordinator = WhiskerDataUpdateCoordinator(hass, client, MagicMock())
    device = DeviceState("SERIAL", "Device", "Type", 1)

    with pytest.raises(asyncio.CancelledError):
        await coordinator._async_enrich_frozen_pipe({"SERIAL": device})


@pytest.mark.asyncio
async def test_setup_entry_connection_failures_and_migration_options(
    hass: HomeAssistant,
) -> None:
    """Entry setup classifies probe failures and migration/options update state."""
    from custom_components.whisker_ting import (
        async_migrate_entry,
        async_options_updated,
    )

    entry = MockConfigEntry(
        domain=DOMAIN,
        version=1,
        data={CONF_USERNAME: "user", CONF_REFRESH_TOKEN: "refresh"},
        options={CONF_SCAN_INTERVAL: 300},
    )
    entry.add_to_hass(hass)
    assert await async_migrate_entry(hass, entry)
    assert entry.version == 2

    coordinator = MagicMock()
    entry.runtime_data = coordinator
    await async_options_updated(hass, entry)
    assert coordinator.update_interval.total_seconds() == 300

    for failure, expected in (
        (False, ConfigEntryNotReady),
        (WhiskerAuthError("bad"), ConfigEntryAuthFailed),
        (WhiskerConnectionError("offline"), ConfigEntryNotReady),
    ):
        client = MagicMock(
            test_connection=AsyncMock(
                side_effect=failure if isinstance(failure, Exception) else None,
                return_value=failure if isinstance(failure, bool) else None,
            )
        )
        with (
            patch(
                "custom_components.whisker_ting.WhiskerApiClient", return_value=client
            ),
            pytest.raises(expected),
        ):
            await async_setup_entry(hass, entry)
