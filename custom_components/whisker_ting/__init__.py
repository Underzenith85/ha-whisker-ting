"""The Whisker Ting integration."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import WhiskerApiClient, WhiskerAuthError, WhiskerConnectionError
from .const import (
    CONF_API_KEY,
    CONF_PASSWORD,
    CONF_REFRESH_TOKEN,
    CONF_SCAN_INTERVAL,
    CONF_USER_ID,
    CONF_USERNAME,
    DEFAULT_SCAN_INTERVAL,
)
from .coordinator import WhiskerDataUpdateCoordinator
from .repairs import WhiskerRepairManager

if TYPE_CHECKING:
    from typing import TypeAlias

    WhiskerConfigEntry: TypeAlias = ConfigEntry[WhiskerDataUpdateCoordinator]
else:
    WhiskerConfigEntry = ConfigEntry

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.BINARY_SENSOR, Platform.SENSOR]


async def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Migrate legacy entries while preserving credentials needed for one login."""
    if entry.version < 2:
        hass.config_entries.async_update_entry(entry, version=2, minor_version=1)
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Whisker Ting from a config entry."""
    username = entry.data[CONF_USERNAME]
    password = entry.data.get(CONF_PASSWORD)
    scan_interval = entry.options.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)

    session = async_get_clientsession(hass)
    client = WhiskerApiClient(
        session,
        username,
        password,
        refresh_token=entry.data.get(CONF_REFRESH_TOKEN),
        user_id=entry.data.get(CONF_USER_ID),
        api_key=entry.data.get(CONF_API_KEY),
    )

    # Test the connection
    try:
        if not await client.test_connection():
            raise ConfigEntryNotReady("Failed to connect to Whisker Ting API")
    except WhiskerAuthError as err:
        raise ConfigEntryAuthFailed("Invalid authentication") from err
    except WhiskerConnectionError as err:
        raise ConfigEntryNotReady(f"Connection error: {err}") from err

    credentials = {
        CONF_USERNAME: username,
        CONF_REFRESH_TOKEN: client.refresh_token,
        CONF_USER_ID: client.user_id,
        CONF_API_KEY: client.api_key,
    }
    if entry.data != credentials:
        hass.config_entries.async_update_entry(
            entry, data=credentials, version=2, minor_version=1
        )

    # Create the coordinator
    repair_manager = WhiskerRepairManager(hass, entry.entry_id, entry.title)
    coordinator = WhiskerDataUpdateCoordinator(
        hass, client, session, scan_interval, repair_manager
    )

    # Fetch initial data
    await coordinator.async_config_entry_first_refresh()

    # Store the coordinator
    entry.runtime_data = coordinator

    # Set up platforms
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Listen for options updates
    entry.async_on_unload(entry.add_update_listener(async_options_updated))

    return True


async def async_options_updated(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle options update."""
    coordinator: WhiskerDataUpdateCoordinator = entry.runtime_data
    scan_interval = entry.options.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)
    coordinator.update_interval = timedelta(seconds=scan_interval)
    _LOGGER.debug("Updated scan interval to %s seconds", scan_interval)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    # Shutdown the coordinator (disconnects WebSocket)
    coordinator: WhiskerDataUpdateCoordinator = entry.runtime_data
    await coordinator.async_shutdown()

    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)


async def async_remove_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Remove Repairs owned by a deleted config entry."""
    WhiskerRepairManager(hass, entry.entry_id, entry.title).clear_all()
