"""Data coordinator for Whisker Ting."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import aiohttp
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import (
    DeviceState,
    Site,
    WhiskerApiClient,
    WhiskerApiError,
    WhiskerAuthError,
)
from .const import DEFAULT_SCAN_INTERVAL, DOMAIN
from .repairs import WhiskerRepairManager
from .stream import (
    PowerQualityCategory,
    PowerQualityData,
    StationDiagnostics,
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
        repair_manager: WhiskerRepairManager | None = None,
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
        self.sites: dict[int, Site] = {}
        self.repair_manager = repair_manager

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

    @callback
    def _handle_stream_diagnostics_update(
        self, station_id: str, diagnostics: StationDiagnostics
    ) -> None:
        """Store bounded station diagnostics and publish through the throttle."""
        if self.data is not None:
            for device in self.data.values():
                if device.station_id == station_id:
                    device.last_realtime_sample_utc = diagnostics.last_sample_utc
                    device.stream_reconnect_count = diagnostics.reconnect_count
                    device.last_stream_reconnect_utc = diagnostics.last_reconnect_utc
                    device.last_stream_reconnect_reason = (
                        diagnostics.last_reconnect_reason
                    )
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
        for device_state in self.data.values():
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
                on_diagnostics_update=self._handle_stream_diagnostics_update,
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
            data = await self._async_collect_rest_data()
            self._merge_retained_state(data)
            self._mark_update_successful()
            await self._async_sync_stream_state(data)
            self._evaluate_repairs(data)
            return data
        except WhiskerAuthError as err:
            self._last_update_success = False
            self._mark_rest_unhealthy()
            if self.repair_manager:
                self.repair_manager.create_authentication_issue()
            raise ConfigEntryAuthFailed(
                "Authentication failed - credentials may have changed"
            ) from err
        except WhiskerApiError as err:
            if self._last_update_success is not False:
                _LOGGER.warning("Unable to connect to Whisker Ting API: %s", err)
            self._last_update_success = False
            self._mark_rest_unhealthy()
            raise UpdateFailed(
                f"Error communicating with Whisker Ting API: {err}"
            ) from err

    async def _async_collect_rest_data(self) -> dict[str, DeviceState]:
        """Collect and enrich one independently validated REST snapshot."""
        data = await self.client.get_all_device_states()
        self._mark_snapshot_healthy(data)
        client_sites = getattr(self.client, "sites", None)
        self.sites = dict(client_sites) if isinstance(client_sites, dict) else {}
        await self._async_assign_events(data)
        await self._async_enrich_frozen_pipe(data)
        return data

    @staticmethod
    def _mark_snapshot_healthy(data: dict[str, DeviceState]) -> None:
        """Record successful REST observation metadata on a new snapshot."""
        observed_at = datetime.now(UTC)
        for device in data.values():
            device.rest_health = "healthy"
            device.last_rest_update_utc = observed_at
            if device.last_device_observation_utc is None:
                device.last_device_observation_utc = observed_at

    async def _async_assign_events(self, data: dict[str, DeviceState]) -> None:
        """Attach account events to their device or site owner."""
        for event in await self.client.get_event_history():
            event_device = (
                data.get(event.serial_number) if event.serial_number else None
            )
            if event_device is not None:
                event_device.events.append(event)
            elif event.site_id is not None and (site := self.sites.get(event.site_id)):
                site.events.append(event)

    async def _async_enrich_frozen_pipe(self, data: dict[str, DeviceState]) -> None:
        """Enrich devices concurrently without hiding auth or cancellation."""
        results = await asyncio.gather(
            *(
                self.client.get_frozen_pipe_data(device.serial_number)
                for device in data.values()
            ),
            return_exceptions=True,
        )
        for device, result in zip(data.values(), results, strict=True):
            if isinstance(result, (WhiskerAuthError, asyncio.CancelledError)):
                raise result
            if isinstance(result, Exception):
                _LOGGER.debug(
                    "Detailed frozen-pipe data unavailable for device %s: %s",
                    device.serial_number,
                    result,
                )
                continue
            if isinstance(result, BaseException):
                raise result
            device.frozen_pipe = result

    def _merge_retained_state(self, data: dict[str, DeviceState]) -> None:
        """Merge live state that remains valid across REST snapshots."""
        if not self.data:
            return
        for device_id, device_state in data.items():
            existing = self.data.get(device_id)
            if existing is None:
                continue
            if existing.voltage.has_live_data:
                device_state.voltage = existing.voltage
            device_state.last_realtime_sample_utc = existing.last_realtime_sample_utc
            device_state.stream_reconnect_count = existing.stream_reconnect_count
            device_state.last_stream_reconnect_utc = existing.last_stream_reconnect_utc
            device_state.last_stream_reconnect_reason = (
                existing.last_stream_reconnect_reason
            )

    def _mark_update_successful(self) -> None:
        """Record recovery after the REST snapshot and enrichment succeed."""
        if self._last_update_success is False:
            _LOGGER.info("Connection to Whisker Ting API restored")
        self._last_update_success = True

    async def _async_sync_stream_state(self, data: dict[str, DeviceState]) -> None:
        """Connect streams, await initial samples, and copy live state."""
        await self._connect_websocket(data)
        if self._ws_manager is None:
            return
        wait_tasks = [
            self._ws_manager.wait_for_data(device.station_id, timeout=5.0)
            for device in data.values()
            if device.station_id
            and self._ws_manager.is_station_managed(device.station_id)
        ]
        if wait_tasks:
            await asyncio.gather(*wait_tasks)
        for device in data.values():
            if device.station_id:
                self._apply_stream_state(device, device.station_id)

    def _apply_stream_state(self, device: DeviceState, station_id: str) -> None:
        """Copy one manager-owned stream snapshot onto a device."""
        if self._ws_manager is None:
            return
        diagnostics = self._ws_manager.get_station_diagnostics(station_id)
        device.last_realtime_sample_utc = diagnostics.last_sample_utc
        device.stream_reconnect_count = diagnostics.reconnect_count
        device.last_stream_reconnect_utc = diagnostics.last_reconnect_utc
        device.last_stream_reconnect_reason = diagnostics.last_reconnect_reason
        device.stream_health = self._ws_manager.get_station_state(
            station_id
        ).health.value
        voltage_data = self._ws_manager.get_voltage_data(station_id)
        if voltage_data:
            device.voltage = device.voltage.with_voltage(
                voltage=voltage_data.voltage,
                voltage_hi=voltage_data.voltage_hi,
                voltage_lo=voltage_data.voltage_lo,
                average_peaks_max=voltage_data.average_peaks_max,
            )

    def _evaluate_repairs(self, data: dict[str, DeviceState]) -> None:
        """Reconcile entry-scoped Repairs after a successful update."""
        if self.repair_manager is None:
            return
        self.repair_manager.clear_authentication_issue()
        capabilities: frozenset[str] = getattr(
            self.client, "unauthorized_capabilities", frozenset()
        )
        capability_failures = getattr(self.client, "optional_capability_failures", {})
        self.repair_manager.evaluate(data.values(), capabilities, capability_failures)

    def _mark_rest_unhealthy(self) -> None:
        """Retain last known device data while marking REST independently failed."""
        if self.data:
            for device in self.data.values():
                device.rest_health = "error"
