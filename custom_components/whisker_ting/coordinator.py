"""Data coordinator for Whisker Ting."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

import aiohttp

from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import (
    DeviceState,
    WhiskerApiClient,
    WhiskerApiError,
    WhiskerAuthError,
)
from .const import DEFAULT_SCAN_INTERVAL, DOMAIN
from .stream import (
    PowerQualityCategory,
    PowerQualityData,
    StreamHealth,
    VoltageData,
    WhiskerWebSocketManager,
)

_LOGGER = logging.getLogger(__name__)


class WhiskerDataUpdateCoordinator(DataUpdateCoordinator[dict[str, DeviceState]]):
    """Class to manage fetching Whisker Ting data."""

    STREAM_UPDATE_INTERVAL = 1.0

    def __init__(
        self,
        hass: HomeAssistant,
        client: WhiskerApiClient,
        session: aiohttp.ClientSession,
        update_interval_seconds: int = DEFAULT_SCAN_INTERVAL,
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=update_interval_seconds),
        )
        self.client = client
        self._session = session
        self._last_update_success: bool | None = None
        self._ws_manager: WhiskerWebSocketManager | None = None
        self._last_stream_listener_update = 0.0
        self._stream_update_handle: asyncio.TimerHandle | None = None

    @callback
    def _schedule_stream_listener_update(self) -> None:
        """Notify listeners at most once per configured stream interval."""
        if self.data is None:
            return
        loop = asyncio.get_running_loop()
        elapsed = loop.time() - self._last_stream_listener_update
        if elapsed >= self.STREAM_UPDATE_INTERVAL:
            self._flush_stream_listener_update()
        elif self._stream_update_handle is None:
            self._stream_update_handle = loop.call_later(
                self.STREAM_UPDATE_INTERVAL - elapsed,
                self._flush_stream_listener_update,
            )

    @callback
    def _flush_stream_listener_update(self) -> None:
        """Publish the newest in-memory stream state to listeners."""
        self._stream_update_handle = None
        if self.data is None:
            return
        self._last_stream_listener_update = asyncio.get_running_loop().time()
        self.async_set_updated_data(self.data)

    @callback
    def _handle_availability_update(self, station_id: str, available: bool) -> None:
        """Publish a station availability transition through the throttle."""
        _LOGGER.debug(
            "Station %s real-time availability changed to %s",
            station_id,
            available,
        )
        self._schedule_stream_listener_update()

    @callback
    def _handle_stream_health_update(
        self, station_id: str, health: StreamHealth
    ) -> None:
        """Store and publish a station stream-health transition."""
        if self.data is not None:
            for device in self.data.values():
                if device.station_id == station_id:
                    device.stream_health = health.value
                    break
        self._schedule_stream_listener_update()

    def is_realtime_available(self, device_id: str) -> bool:
        """Return whether a device's real-time stream is subscribed and live."""
        if self.data is None or self._ws_manager is None:
            return False
        device = self.data.get(device_id)
        return bool(
            device
            and device.station_id
            and self._ws_manager.is_station_available(device.station_id)
        )

    @callback
    def _handle_voltage_update(
        self, station_id: str, voltage_data: VoltageData
    ) -> None:
        """Handle real-time voltage update from WebSocket."""
        if self.data is None:
            return

        # Find the device with this station_id
        for device_id, device_state in self.data.items():
            if device_state.station_id == station_id:
                # Update the voltage reading
                device_state.voltage = device_state.voltage.with_voltage(
                    voltage=voltage_data.voltage,
                    voltage_hi=voltage_data.voltage_hi,
                    voltage_lo=voltage_data.voltage_lo,
                    average_peaks_max=voltage_data.average_peaks_max,
                )
                self._schedule_stream_listener_update()
                break

    @callback
    def _handle_power_quality_update(
        self, station_id: str, reading: PowerQualityData
    ) -> None:
        """Store a Ting 3.0.4 secondary power-quality stream reading."""
        if self.data is None:
            return
        for device in self.data.values():
            if device.station_id == station_id:
                match reading.category:
                    case PowerQualityCategory.FREQUENCY:
                        device.voltage = device.voltage.with_frequency(reading.value)
                    case PowerQualityCategory.THD_MIN:
                        device.voltage = device.voltage.with_thd_min(reading.value)
                    case PowerQualityCategory.THD_AVERAGE:
                        device.voltage = device.voltage.with_thd_average(reading.value)
                    case PowerQualityCategory.THD_MAX:
                        device.voltage = device.voltage.with_thd_max(reading.value)
                self._schedule_stream_listener_update()
                break

    async def _connect_websocket(self, data: dict[str, DeviceState]) -> None:
        """Connect to WebSocket for real-time updates."""
        if self._ws_manager is None:
            self._ws_manager = WhiskerWebSocketManager(
                session=self._session,
                on_voltage_update=self._handle_voltage_update,
                on_power_quality_update=self._handle_power_quality_update,
                on_availability_update=self._handle_availability_update,
                on_health_update=self._handle_stream_health_update,
            )

        if not data:
            return

        # Get api_key and user_id from the client
        api_key = self.client.api_key
        user_id = self.client.user_id

        if not api_key or not user_id:
            _LOGGER.debug("No api_key or user_id, skipping WebSocket connection")
            return

        # Connect to each device's WebSocket stream
        for device_id, device_state in data.items():
            if device_state.station_id and not self._ws_manager.is_station_managed(
                device_state.station_id
            ):
                try:
                    connected = await self._ws_manager.connect_device(
                        api_key=api_key,
                        user_id=user_id,
                        station_id=device_state.station_id,
                    )
                    if connected:
                        _LOGGER.info(
                            "Connected to WebSocket for device %s (station %s)",
                            device_id,
                            device_state.station_id,
                        )
                except Exception as err:
                    _LOGGER.warning(
                        "Failed to connect WebSocket for device %s: %s",
                        device_id,
                        err,
                    )

    async def async_shutdown(self) -> None:
        """Shutdown the coordinator."""
        if self._stream_update_handle is not None:
            self._stream_update_handle.cancel()
            self._stream_update_handle = None
        if self._ws_manager:
            await self._ws_manager.disconnect_all()
        await super().async_shutdown()

    async def _async_update_data(self) -> dict[str, DeviceState]:
        """Fetch data from the API."""
        try:
            data = await self.client.get_all_device_states()

            events = await self.client.get_event_history()
            for event in events:
                device = data.get(event.serial_number)
                if device is not None:
                    device.events.append(event)

            frozen_pipe_results = await asyncio.gather(
                *(
                    self.client.get_frozen_pipe_data(device.serial_number)
                    for device in data.values()
                ),
                return_exceptions=True,
            )
            for device, result in zip(data.values(), frozen_pipe_results, strict=True):
                if isinstance(result, WhiskerAuthError):
                    raise result
                if isinstance(result, Exception):
                    _LOGGER.debug(
                        "Detailed frozen-pipe data unavailable for device %s: %s",
                        device.serial_number,
                        result,
                    )
                    continue
                device.frozen_pipe = result

            # Preserve existing voltage data from WebSocket
            if self.data:
                for device_id, device_state in data.items():
                    existing = self.data.get(device_id)
                    if existing and existing.voltage.has_live_data:
                        device_state.voltage = existing.voltage

            if self._last_update_success is False:
                _LOGGER.info("Connection to Whisker Ting API restored")
            self._last_update_success = True

            await self._connect_websocket(data)
            if self._ws_manager:
                wait_tasks = [
                    self._ws_manager.wait_for_data(device_state.station_id, timeout=5.0)
                    for device_state in data.values()
                    if device_state.station_id
                    and self._ws_manager.is_station_managed(device_state.station_id)
                ]
                if wait_tasks:
                    await asyncio.gather(*wait_tasks)
                for device_state in data.values():
                    if device_state.station_id:
                        device_state.stream_health = self._ws_manager.get_station_state(
                            device_state.station_id
                        ).health.value
                        voltage_data = self._ws_manager.get_voltage_data(
                            device_state.station_id
                        )
                        if voltage_data:
                            device_state.voltage = device_state.voltage.with_voltage(
                                voltage=voltage_data.voltage,
                                voltage_hi=voltage_data.voltage_hi,
                                voltage_lo=voltage_data.voltage_lo,
                                average_peaks_max=voltage_data.average_peaks_max,
                            )

            return data
        except WhiskerAuthError as err:
            self._last_update_success = False
            raise ConfigEntryAuthFailed(
                "Authentication failed - credentials may have changed"
            ) from err
        except WhiskerApiError as err:
            if self._last_update_success is not False:
                _LOGGER.warning("Unable to connect to Whisker Ting API: %s", err)
            self._last_update_success = False
            raise UpdateFailed(
                f"Error communicating with Whisker Ting API: {err}"
            ) from err
