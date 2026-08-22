"""Home Assistant fixture tests for integration lifecycle behavior."""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from homeassistant.config_entries import SOURCE_USER
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.whisker_ting import async_unload_entry
from custom_components.whisker_ting.api import (
    DeviceState,
    UserData,
    WhiskerApiError,
)
from custom_components.whisker_ting.binary_sensor import (
    BINARY_SENSOR_DESCRIPTIONS,
    WhiskerBinarySensor,
)
from custom_components.whisker_ting.const import (
    CONF_API_KEY,
    CONF_PASSWORD,
    CONF_REFRESH_TOKEN,
    CONF_USERNAME,
    CONF_USER_ID,
    DOMAIN,
)
from custom_components.whisker_ting.coordinator import (
    WhiskerDataUpdateCoordinator,
)
from custom_components.whisker_ting.sensor import SENSOR_DESCRIPTIONS, WhiskerSensor


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
        result["result"].runtime_data = MagicMock(
            async_shutdown=AsyncMock()
        )
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
    entity = entity_class(coordinator, device.serial_number, description)

    assert entity.available
    coordinator.data = {}
    assert not entity.available


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
    manager = MagicMock()
    manager.connect_device = AsyncMock(return_value=True)
    manager.wait_for_data = AsyncMock(return_value=False)
    manager.get_voltage_data.return_value = None
    manager.disconnect_all = AsyncMock()

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
