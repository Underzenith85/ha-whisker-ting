"""Multi-station management for Ting real-time streams."""

from __future__ import annotations

import asyncio
import logging
import random
import re
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime

import aiohttp

from .client import WhiskerWebSocket
from .models import (
    PowerQualityData,
    StationDiagnostics,
    StationState,
    StreamHealth,
    VoltageData,
)

_LOGGER = logging.getLogger(__name__)


class WhiskerWebSocketManager:
    """Manages WebSocket connections for multiple devices."""

    # Reconnect settings
    RECONNECT_MIN_DELAY = 5
    RECONNECT_MAX_DELAY = 300  # 5 minutes max
    RECONNECT_BACKOFF_FACTOR = 2
    RECONNECT_JITTER_FACTOR = 0.2

    def __init__(
        self,
        session: aiohttp.ClientSession,
        on_voltage_update: Callable[[str, VoltageData], None] | None = None,
        on_power_quality_update: Callable[[str, PowerQualityData], None] | None = None,
        on_availability_update: Callable[[str, bool], None] | None = None,
        on_health_update: Callable[[str, StreamHealth], None] | None = None,
        on_diagnostics_update: (
            Callable[[str, StationDiagnostics], None] | None
        ) = None,
    ) -> None:
        """Initialize the manager."""
        self._session = session
        self._on_voltage_update = on_voltage_update
        self._on_power_quality_update = on_power_quality_update
        self._on_availability_update = on_availability_update
        self._on_health_update = on_health_update
        self._on_diagnostics_update = on_diagnostics_update
        self._connections: dict[str, WhiskerWebSocket] = {}
        self._voltage_data: dict[str, VoltageData] = {}
        self._station_states: dict[str, StationState] = {}
        self._station_diagnostics: dict[str, StationDiagnostics] = {}
        self._credentials: dict[str, dict] = {}  # Store credentials for reconnect
        self._reconnect_tasks: dict[str, asyncio.Task] = {}
        self._reconnect_attempts: dict[str, int] = {}
        self._shutting_down = False

    def get_station_state(self, station_id: str) -> StationState:
        """Return the independently tracked state for a station."""
        return self._station_states.get(station_id, StationState())

    def get_station_diagnostics(self, station_id: str) -> StationDiagnostics:
        """Return bounded diagnostics retained for the manager lifecycle."""
        return self._station_diagnostics.get(station_id, StationDiagnostics())

    def _set_station_diagnostics(
        self, station_id: str, diagnostics: StationDiagnostics
    ) -> None:
        """Store station diagnostics and publish through the coordinator throttle."""
        self._station_diagnostics[station_id] = diagnostics
        if self._on_diagnostics_update is not None:
            self._on_diagnostics_update(station_id, diagnostics)

    def is_station_available(self, station_id: str) -> bool:
        """Return whether a station has a subscribed, live stream."""
        return self.get_station_state(station_id).available

    def is_station_managed(self, station_id: str) -> bool:
        """Return whether a station is connected or already reconnecting."""
        reconnect = self._reconnect_tasks.get(station_id)
        return station_id in self._connections or (
            reconnect is not None and not reconnect.done()
        )

    def _set_station_state(self, station_id: str, state: StationState) -> None:
        """Store station state and notify only when availability changes."""
        previous = self.get_station_state(station_id)
        self._station_states[station_id] = state
        if (
            previous.available != state.available
            and self._on_availability_update is not None
        ):
            self._on_availability_update(station_id, state.available)
        if previous.health != state.health and self._on_health_update is not None:
            self._on_health_update(station_id, state.health)

    def get_voltage_data(self, station_id: str) -> VoltageData | None:
        """Get the latest voltage data for a station."""
        return self._voltage_data.get(station_id)

    def _handle_voltage_update(self, station_id: str, data: VoltageData) -> None:
        """Handle voltage update from WebSocket."""
        self._voltage_data[station_id] = data
        self._set_station_diagnostics(
            station_id,
            replace(
                self.get_station_diagnostics(station_id),
                last_sample_utc=data.timestamp,
            ),
        )
        # Reset reconnect attempts on successful data
        self._reconnect_attempts[station_id] = 0
        state = self.get_station_state(station_id)
        self._set_station_state(
            station_id,
            StationState(
                connected=state.connected,
                subscribed=state.subscribed,
                live=True,
                health=StreamHealth.RECEIVING,
            ),
        )
        _LOGGER.debug(
            "Voltage update for %s: %.2fV (hi: %.2fV, lo: %.2fV)",
            station_id,
            data.voltage,
            data.voltage_hi,
            data.voltage_lo,
        )
        if self._on_voltage_update:
            self._on_voltage_update(station_id, data)

    def _handle_power_quality_update(
        self, station_id: str, data: PowerQualityData
    ) -> None:
        """Track any valid real-time sample before publishing its metric."""
        self._set_station_diagnostics(
            station_id,
            replace(
                self.get_station_diagnostics(station_id),
                last_sample_utc=data.timestamp,
            ),
        )
        if self._on_power_quality_update:
            self._on_power_quality_update(station_id, data)

    def _handle_health_update(self, station_id: str, health: StreamHealth) -> None:
        """Handle an independently calculated station health transition."""
        state = self.get_station_state(station_id)
        self._set_station_state(
            station_id,
            StationState(
                connected=state.connected,
                subscribed=state.subscribed,
                live=health in (StreamHealth.RECEIVING, StreamHealth.DELAYED)
                and state.live,
                health=health,
            ),
        )

    def _handle_disconnect(
        self, station_id: str, reason: str, allow_reconnect: bool
    ) -> None:
        """Handle WebSocket disconnect - schedule reconnection."""
        if self._shutting_down:
            return

        diagnostics = self.get_station_diagnostics(station_id)
        self._set_station_diagnostics(
            station_id,
            replace(
                diagnostics,
                last_reconnect_reason=_sanitize_reconnect_reason(reason),
            ),
        )

        # Remove old connection
        if station_id in self._connections:
            del self._connections[station_id]
        self._set_station_state(
            station_id,
            StationState(health=StreamHealth.NOT_RECEIVING),
        )

        if not allow_reconnect:
            _LOGGER.warning(
                "SignalR server disabled reconnect for station %s: %s",
                station_id,
                reason,
            )
            return

        # Schedule reconnection
        if (
            station_id not in self._reconnect_tasks
            or self._reconnect_tasks[station_id].done()
        ):
            self._reconnect_tasks[station_id] = asyncio.create_task(
                self._reconnect_with_backoff(station_id)
            )

    async def _reconnect_with_backoff(self, station_id: str) -> None:
        """Reconnect to a station with exponential backoff."""
        try:
            while not self._shutting_down:
                creds = self._credentials.get(station_id)
                if creds is None:
                    _LOGGER.error(
                        "No credentials stored for station %s, cannot reconnect",
                        station_id,
                    )
                    return

                attempts = self._reconnect_attempts.get(station_id, 0)
                base_delay = min(
                    self.RECONNECT_MIN_DELAY
                    * (self.RECONNECT_BACKOFF_FACTOR**attempts),
                    self.RECONNECT_MAX_DELAY,
                )
                jitter = random.uniform(
                    0,
                    min(
                        base_delay * self.RECONNECT_JITTER_FACTOR,
                        self.RECONNECT_MAX_DELAY - base_delay,
                    ),
                )
                delay = base_delay + jitter

                _LOGGER.info(
                    "Reconnecting to station %s in %.1f seconds (attempt %d)",
                    station_id,
                    delay,
                    attempts + 1,
                )
                await asyncio.sleep(delay)
                if self._shutting_down:
                    return

                self._reconnect_attempts[station_id] = attempts + 1
                diagnostics = self.get_station_diagnostics(station_id)
                self._set_station_diagnostics(
                    station_id,
                    replace(
                        diagnostics,
                        reconnect_count=diagnostics.reconnect_count + 1,
                        last_reconnect_utc=datetime.now(UTC),
                    ),
                )
                ws = WhiskerWebSocket(
                    session=self._session,
                    api_key=creds["api_key"],
                    user_id=creds["user_id"],
                    station_id=station_id,
                    on_voltage_update=self._handle_voltage_update,
                    on_power_quality_update=self._handle_power_quality_update,
                    on_disconnect=self._handle_disconnect,
                    on_health_update=self._handle_health_update,
                )

                if await ws.connect():
                    self._connections[station_id] = ws
                    state = self.get_station_state(station_id)
                    self._set_station_state(
                        station_id,
                        StationState(
                            connected=True,
                            subscribed=True,
                            live=state.live,
                            health=state.health,
                        ),
                    )
                    _LOGGER.info("Reconnected to station %s", station_id)
                    return
                _LOGGER.warning(
                    "Reconnection failed for station %s, will retry", station_id
                )
        finally:
            if self._reconnect_tasks.get(station_id) is asyncio.current_task():
                self._reconnect_tasks.pop(station_id, None)

    async def connect_device(
        self,
        api_key: str,
        user_id: int,
        station_id: str,
    ) -> bool:
        """Connect to a device's WebSocket stream."""
        if station_id in self._connections:
            connection = self._connections[station_id]
            if connection.connected:
                _LOGGER.debug("Already connected to station %s", station_id)
                return True
            self._connections.pop(station_id, None)

        reconnect = self._reconnect_tasks.get(station_id)
        if reconnect is not None and not reconnect.done():
            _LOGGER.debug("Already reconnecting to station %s", station_id)
            return False

        # Store credentials for reconnection
        self._credentials[station_id] = {
            "api_key": api_key,
            "user_id": user_id,
        }
        self._reconnect_attempts[station_id] = 0

        ws = WhiskerWebSocket(
            session=self._session,
            api_key=api_key,
            user_id=user_id,
            station_id=station_id,
            on_voltage_update=self._handle_voltage_update,
            on_power_quality_update=self._handle_power_quality_update,
            on_disconnect=self._handle_disconnect,
            on_health_update=self._handle_health_update,
        )

        if await ws.connect():
            self._connections[station_id] = ws
            state = self.get_station_state(station_id)
            self._set_station_state(
                station_id,
                StationState(
                    connected=True,
                    subscribed=True,
                    live=state.live,
                    health=StreamHealth.DELAYED,
                ),
            )
            return True
        self._set_station_state(station_id, StationState())
        return False

    async def disconnect_all(self) -> None:
        """Disconnect all WebSocket connections."""
        self._shutting_down = True

        # Cancel any pending reconnect tasks
        for task in self._reconnect_tasks.values():
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._reconnect_tasks.clear()

        # Disconnect all connections
        for station_id, ws in list(self._connections.items()):
            await ws.disconnect()
            del self._connections[station_id]
        self._station_states.clear()
        self._station_diagnostics.clear()
        self._voltage_data.clear()
        self._credentials.clear()
        self._reconnect_attempts.clear()

    async def disconnect_device(self, station_id: str) -> None:
        """Disconnect a specific device."""
        # Cancel any pending reconnect
        if station_id in self._reconnect_tasks:
            task = self._reconnect_tasks[station_id]
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            del self._reconnect_tasks[station_id]

        if station_id in self._connections:
            await self._connections[station_id].disconnect()
            del self._connections[station_id]
        self._station_states.pop(station_id, None)
        self._station_diagnostics.pop(station_id, None)
        self._voltage_data.pop(station_id, None)
        self._credentials.pop(station_id, None)
        self._reconnect_attempts.pop(station_id, None)

    async def wait_for_data(self, station_id: str, timeout: float = 5.0) -> bool:
        """Wait for first voltage data from a specific station.

        Returns True if data was received, False if timeout or not connected.
        """
        ws = self._connections.get(station_id)
        if ws:
            return await ws.wait_for_data(timeout=timeout)
        return False


def _sanitize_reconnect_reason(reason: str) -> str:
    """Bound and redact credential-shaped values from a reconnect reason."""
    sanitized = " ".join(reason.split())
    sanitized = re.sub(
        r"(?i)(authorization|token|api[_-]?key|password)\s*[:=]\s*\S+",
        r"\1=[redacted]",
        sanitized,
    )
    return sanitized[:160] or "unspecified"
