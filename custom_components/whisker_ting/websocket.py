"""WebSocket client for real-time Whisker Ting data."""

from __future__ import annotations

import asyncio
import logging
import math
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

import aiohttp

from .const import SIGNALR_URL
from .signalr import (
    SignalRHandshakeError,
    SignalRProtocolError,
    decode_handshake_response,
    encode_invocation,
    encode_ping,
    extract_categorical_payloads,
    extract_completions,
    extract_control_messages,
    extract_invocation_payloads,
)

_LOGGER = logging.getLogger(__name__)

# SignalR MessagePack message types
MSG_TYPE_INVOCATION = 1
MSG_TYPE_STREAM_ITEM = 2
MSG_TYPE_COMPLETION = 3
MSG_TYPE_STREAM_INVOCATION = 4
MSG_TYPE_CANCEL_INVOCATION = 5
MSG_TYPE_PING = 6
MSG_TYPE_CLOSE = 7


@dataclass
class VoltageData:
    """Real-time voltage data from WebSocket."""

    timestamp: datetime
    voltage: float
    voltage_hi: float
    voltage_lo: float
    average_peaks_max: float


@dataclass(frozen=True)
class PowerQualityData:
    """One categorical power-quality reading from the Ting stream."""

    timestamp: datetime
    category: str
    value: float


class StreamHealth(StrEnum):
    """Health of a station's real-time data stream."""

    RECEIVING = "receiving"
    DELAYED = "delayed"
    NOT_RECEIVING = "not_receiving"
    STOPPED = "stopped"


@dataclass(frozen=True)
class StationState:
    """Connection, subscription, and stream liveness for one station."""

    connected: bool = False
    subscribed: bool = False
    live: bool = False
    health: StreamHealth = StreamHealth.STOPPED

    @property
    def available(self) -> bool:
        """Return whether real-time readings for the station are current."""
        return self.connected and self.subscribed and self.live


class SignalRInvocationError(Exception):
    """Raised when a SignalR hub invocation fails."""


class WhiskerWebSocket:
    """WebSocket client for Whisker Ting SignalR hub."""

    DELAYED_DATA_THRESHOLD = 5.0
    NOT_RECEIVING_THRESHOLD = 10.0
    HEALTH_CHECK_INTERVAL = 1.0
    INVOCATION_TIMEOUT = 10.0
    UNSUBSCRIBE_TIMEOUT = 5.0
    SECONDARY_DATA_ELEMENTS = ("frequency", "thdMin", "thdAvg", "thdMax")

    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_key: str,
        user_id: int,
        station_id: str,
        on_voltage_update: Callable[[str, VoltageData], None] | None = None,
        on_power_quality_update: Callable[[str, PowerQualityData], None] | None = None,
        on_disconnect: Callable[[str, str, bool], None] | None = None,
        on_health_update: Callable[[str, StreamHealth], None] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        """Initialize the WebSocket client."""
        self._session = session
        self._api_key = api_key  # The api_key is used as the stream token
        self._user_id = user_id
        self._station_id = station_id
        self._on_voltage_update = on_voltage_update
        self._on_power_quality_update = on_power_quality_update
        self._on_disconnect = on_disconnect
        self._on_health_update = on_health_update
        self._monotonic = monotonic
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._connected = False
        self._ping_task: asyncio.Task | None = None
        self._receive_task: asyncio.Task | None = None
        self._stale_check_task: asyncio.Task | None = None
        self._message_id = 0
        self._pending_invocations: dict[str, asyncio.Future[Any]] = {}
        self._first_data_received = asyncio.Event()
        self._last_data_time: datetime | None = None
        self._last_data_monotonic: float | None = None
        self._health = StreamHealth.STOPPED
        self._subscribed = False
        self._subscribed_elements: set[str] = set()
        self._shutting_down = False
        self._disconnect_notified = False

    @property
    def connected(self) -> bool:
        """Return True if connected and subscribed to the device stream."""
        return self._connected and self._subscribed

    def _encode_invocation(self, method: str, args: list) -> tuple[str, bytes]:
        """Encode a SignalR invocation message."""
        self._message_id += 1
        invocation_id = str(self._message_id)
        return invocation_id, encode_invocation(invocation_id, method, args)

    def _encode_ping(self) -> bytes:
        """Encode a SignalR ping message."""
        return encode_ping()

    async def _invoke(
        self,
        method: str,
        args: list[Any],
        timeout: float | None = None,
    ) -> Any:
        """Invoke a hub method and wait for its matching Completion."""
        if self._ws is None or self._ws.closed:
            raise SignalRInvocationError("WebSocket is not connected")

        invocation_id, message = self._encode_invocation(method, args)
        future = asyncio.get_running_loop().create_future()
        self._pending_invocations[invocation_id] = future

        try:
            await self._ws.send_bytes(message)
            try:
                return await asyncio.wait_for(
                    future,
                    timeout=self.INVOCATION_TIMEOUT if timeout is None else timeout,
                )
            except TimeoutError as err:
                raise SignalRInvocationError(
                    f"SignalR invocation {method} timed out"
                ) from err
        finally:
            self._pending_invocations.pop(invocation_id, None)
            if not future.done():
                future.cancel()

    async def _subscribe(self, args: list[Any]) -> None:
        """Initialize the device stream and mark it subscribed on success."""
        await self._invoke("InitializeStreaming", args)
        self._subscribed = True
        self._subscribed_elements.add("ComboBinaryData")

    def _stream_args(self) -> list[Any]:
        """Return the server arguments that identify this station stream."""
        return [
            {"StationId": self._station_id, "DataElement": "ComboBinaryData"},
            self._api_key,
            str(self._user_id),
        ]

    async def _unsubscribe(self) -> None:
        """Release an initialized station stream and wait for Completion."""
        if not self._subscribed:
            return
        try:
            subscribed_elements = self._subscribed_elements or {"ComboBinaryData"}
            await asyncio.gather(
                *(
                    self._invoke(
                        "UnInitializeStreaming",
                        [
                            {
                                "StationId": self._station_id,
                                "DataElement": data_element,
                            },
                            self._api_key,
                            str(self._user_id),
                        ],
                        timeout=self.UNSUBSCRIBE_TIMEOUT,
                    )
                    for data_element in subscribed_elements
                )
            )
        finally:
            self._subscribed = False
            self._subscribed_elements.clear()

    def _handle_completions(self, data: bytes) -> None:
        """Resolve pending invocations represented in a binary message."""
        for completion in extract_completions(data):
            future = self._pending_invocations.get(completion.invocation_id)
            if future is None or future.done():
                continue
            if completion.error is not None:
                future.set_exception(
                    SignalRInvocationError("SignalR invocation rejected by server")
                )
            else:
                future.set_result(completion.result)

    def _fail_pending_invocations(self, error: SignalRInvocationError) -> None:
        """Fail every invocation still waiting for a Completion."""
        for future in self._pending_invocations.values():
            if not future.done():
                future.set_exception(error)

    def _set_health(self, health: StreamHealth) -> None:
        """Publish a stream health transition at most once."""
        if self._health == health:
            return
        self._health = health
        if self._on_health_update is not None:
            self._on_health_update(self._station_id, health)

    def _transition_disconnected(
        self, reason: str, *, allow_reconnect: bool = True
    ) -> None:
        """Transition to disconnected and notify reconnect logic at most once."""
        self._connected = False
        self._subscribed = False
        if not self._shutting_down:
            self._set_health(StreamHealth.NOT_RECEIVING)
        self._fail_pending_invocations(SignalRInvocationError("WebSocket disconnected"))
        current_task = asyncio.current_task()
        for task in (self._ping_task, self._receive_task, self._stale_check_task):
            if task is not None and task is not current_task and not task.done():
                task.cancel()
        if self._shutting_down or self._disconnect_notified:
            return
        self._disconnect_notified = True
        _LOGGER.warning(
            "SignalR disconnected for station %s: %s", self._station_id, reason
        )
        if self._on_disconnect:
            self._on_disconnect(self._station_id, reason, allow_reconnect)

    async def _perform_handshake(self) -> bytes | None:
        """Negotiate SignalR MessagePack and return an appended hub payload."""
        if self._ws is None:
            raise SignalRHandshakeError("SignalR transport is not connected")
        await self._ws.send_str('{"protocol":"messagepack","version":1}\x1e')
        try:
            message = await self._ws.receive(timeout=10)
        except TimeoutError as err:
            raise SignalRHandshakeError("SignalR handshake timed out") from err
        if message.type not in (
            aiohttp.WSMsgType.TEXT,
            aiohttp.WSMsgType.BINARY,
        ):
            raise SignalRHandshakeError("SignalR handshake response has invalid type")
        remainder = decode_handshake_response(message.data)
        if isinstance(remainder, str):
            raise SignalRHandshakeError(
                "SignalR MessagePack payload after handshake is not binary"
            )
        return remainder

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime:
        """Parse the timestamp representation used by the Ting stream."""
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return timestamp.replace(tzinfo=timestamp.tzinfo or UTC)
            except ValueError:
                pass
        return datetime.now(UTC)

    def _decode_voltage_data(self, data: bytes) -> list[VoltageData]:
        """Decode voltage readings from SignalR MessagePack messages."""
        try:
            payloads = extract_invocation_payloads(data, "updateComboBinaryData")
        except SignalRProtocolError as err:
            _LOGGER.debug("Error decoding SignalR voltage frame: %s", err)
            return []

        readings: list[VoltageData] = []
        for payload in payloads:
            try:
                voltage = float(payload["Voltage"])
                voltage_hi = float(payload["VoltageHi"])
                voltage_lo = float(payload["VoltageLo"])
                peaks = float(payload["AveragePeaksMax"])

                if not all(
                    math.isfinite(value)
                    for value in (voltage, voltage_hi, voltage_lo, peaks)
                ):
                    raise ValueError("non-finite voltage value")

                if abs(voltage) < 1 or abs(voltage) > 1000:
                    _LOGGER.debug(
                        "Discarding anomalous voltage reading: %.2fV", voltage
                    )
                    continue

                readings.append(
                    VoltageData(
                        timestamp=self._parse_timestamp(payload.get("DataTimeUtc")),
                        voltage=voltage,
                        average_peaks_max=peaks,
                        voltage_hi=voltage_hi,
                        voltage_lo=voltage_lo,
                    )
                )
            except (KeyError, TypeError, ValueError) as err:
                _LOGGER.debug("Discarding invalid voltage payload: %s", err)

        return readings

    def _decode_power_quality_data(self, data: bytes) -> list[PowerQualityData]:
        """Decode the frequency and THD streams used by Ting 3.0.4."""
        try:
            payloads = extract_categorical_payloads(data)
        except SignalRProtocolError as err:
            _LOGGER.debug("Error decoding SignalR power-quality frame: %s", err)
            return []
        readings: list[PowerQualityData] = []
        for payload in payloads:
            try:
                category = payload["Category"]
                value = float(payload["Value"])
                if category not in self.SECONDARY_DATA_ELEMENTS:
                    continue
                if not math.isfinite(value):
                    raise ValueError("non-finite power-quality value")
                readings.append(
                    PowerQualityData(
                        timestamp=self._parse_timestamp(payload.get("ObsTime")),
                        category=category,
                        value=value,
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        return readings

    async def connect(self) -> bool:
        """Connect to the SignalR hub."""
        try:
            _LOGGER.debug("Connecting to SignalR hub: %s", SIGNALR_URL)

            self._ws = await self._session.ws_connect(
                SIGNALR_URL,
                headers={
                    "Origin": "ionic://localhost",
                },
            )
            self._shutting_down = False
            self._disconnect_notified = False
            initial_payload = await self._perform_handshake()
            _LOGGER.debug("SignalR handshake completed")

            self._connected = True
            self._subscribed = False
            self._last_data_time = datetime.now(UTC)
            self._last_data_monotonic = self._monotonic()
            self._set_health(StreamHealth.DELAYED)
            self._receive_task = asyncio.create_task(self._receive_loop())

            if initial_payload is not None:
                self._handle_binary_message(initial_payload)

            # Subscribe to device stream using api_key as the token
            await self._subscribe(self._stream_args())

            async def subscribe_optional(data_element: str) -> None:
                try:
                    await self._invoke(
                        "InitializeStreaming",
                        [
                            {
                                "StationId": self._station_id,
                                "DataElement": data_element,
                            },
                            self._api_key,
                            str(self._user_id),
                        ],
                    )
                    self._subscribed_elements.add(data_element)
                except SignalRInvocationError as err:
                    _LOGGER.debug(
                        "Optional %s stream unavailable for station %s: %s",
                        data_element,
                        self._station_id,
                        err,
                    )

            await asyncio.gather(
                *(subscribe_optional(value) for value in self.SECONDARY_DATA_ELEMENTS)
            )

            # Start remaining background tasks after subscription succeeds
            self._ping_task = asyncio.create_task(self._ping_loop())
            self._stale_check_task = asyncio.create_task(self._stale_data_check_loop())

            _LOGGER.info("Connected to SignalR hub for station %s", self._station_id)
            return True

        except SignalRHandshakeError as err:
            _LOGGER.error("SignalR handshake failure: %s", err)
            await self._cleanup_failed_connect()
            return False
        except Exception as err:
            _LOGGER.error("Failed to connect to SignalR hub: %s", err)
            await self._cleanup_failed_connect()
            return False

    async def _cleanup_failed_connect(self) -> None:
        """Clean up transport and tasks after an unsuccessful connection."""
        self._connected = False
        self._subscribed = False
        self._set_health(StreamHealth.STOPPED)
        self._fail_pending_invocations(
            SignalRInvocationError("WebSocket connection failed")
        )
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
            self._receive_task = None
        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._ws = None

    async def disconnect(self) -> None:
        """Disconnect from the SignalR hub."""
        if self._shutting_down and self._ws is None:
            return
        self._shutting_down = True

        # Keep the receive loop alive until it can correlate the unsubscribe
        # Completion. Teardown failure must never prevent transport cleanup.
        for task in (self._ping_task, self._stale_check_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._ping_task = None
        self._stale_check_task = None

        if (
            self._connected
            and self._subscribed
            and self._ws is not None
            and not self._ws.closed
        ):
            try:
                await self._unsubscribe()
            except Exception as err:
                _LOGGER.debug(
                    "Unable to release SignalR stream for station %s: %s",
                    self._station_id,
                    err,
                )

        self._connected = False
        self._subscribed = False
        self._set_health(StreamHealth.STOPPED)
        self._fail_pending_invocations(SignalRInvocationError("WebSocket disconnected"))

        for task in [self._receive_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._receive_task = None

        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._ws = None

        _LOGGER.debug("Disconnected from SignalR hub")

    async def wait_for_data(self, timeout: float = 5.0) -> bool:
        """Wait for the first voltage data to be received.

        Returns True if data was received, False if timeout.
        """
        try:
            await asyncio.wait_for(self._first_data_received.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            _LOGGER.debug("Timeout waiting for first voltage data")
            return False

    async def _receive_loop(self) -> None:
        """Receive messages from the WebSocket."""
        try:
            while self._connected and self._ws and not self._ws.closed:
                try:
                    msg = await asyncio.wait_for(
                        self._ws.receive(),
                        timeout=30,
                    )

                    if msg.type == aiohttp.WSMsgType.BINARY:
                        try:
                            close_messages = self._handle_binary_message(msg.data)
                        except SignalRProtocolError as err:
                            _LOGGER.error("SignalR protocol failure: %s", err)
                            self._transition_disconnected("protocol failure")
                            break
                        if close_messages:
                            close = close_messages[-1]
                            self._transition_disconnected(
                                close.reason,
                                allow_reconnect=close.allow_reconnect,
                            )
                            break
                    elif msg.type == aiohttp.WSMsgType.TEXT:
                        _LOGGER.warning(
                            "Unexpected text message after SignalR handshake"
                        )

                    elif msg.type in (
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.ERROR,
                    ):
                        self._transition_disconnected(f"transport closed ({msg.type})")
                        break

                except asyncio.TimeoutError:
                    _LOGGER.debug("WebSocket receive timeout, continuing...")
                except asyncio.CancelledError:
                    break
                except Exception as err:
                    _LOGGER.error("Error in receive loop: %s", err)
                    self._transition_disconnected("receive failure")
                    break
        finally:
            if self._ws is not None and not self._ws.closed:
                await self._ws.close()

    def _handle_binary_message(self, data: bytes) -> list[Any]:
        """Process one binary SignalR payload and return Close messages."""
        self._handle_completions(data)
        ping_count, close_messages = extract_control_messages(data)
        if ping_count:
            _LOGGER.debug("Received %d SignalR Ping message(s)", ping_count)
        for voltage_data in self._decode_voltage_data(data):
            if self._on_voltage_update:
                self._last_data_time = datetime.now(UTC)
                self._last_data_monotonic = self._monotonic()
                self._set_health(StreamHealth.RECEIVING)
                self._on_voltage_update(self._station_id, voltage_data)
                if not self._first_data_received.is_set():
                    self._first_data_received.set()
        for reading in self._decode_power_quality_data(data):
            if self._on_power_quality_update is not None:
                self._on_power_quality_update(self._station_id, reading)
        return close_messages

    async def _stale_data_check_loop(self) -> None:
        """Check for stale data and trigger reconnect if needed."""
        while self._connected and not self._shutting_down:
            try:
                await asyncio.sleep(self.HEALTH_CHECK_INTERVAL)

                if not self._connected or self._shutting_down:
                    break

                if self._last_data_monotonic is not None:
                    time_since_update = self._monotonic() - self._last_data_monotonic
                    if time_since_update >= self.NOT_RECEIVING_THRESHOLD:
                        self._set_health(StreamHealth.NOT_RECEIVING)
                        _LOGGER.error(
                            "WebSocket data stale for station %s (no update in %.0f seconds), reconnecting",
                            self._station_id,
                            time_since_update,
                        )
                        self._transition_disconnected("voltage data became stale")
                        break
                    if time_since_update >= self.DELAYED_DATA_THRESHOLD:
                        self._set_health(StreamHealth.DELAYED)

            except asyncio.CancelledError:
                break
            except Exception as err:
                _LOGGER.error("Error in stale data check: %s", err)
                self._transition_disconnected("stale-data check failure")
                break

    async def _ping_loop(self) -> None:
        """Send periodic pings to keep the connection alive."""
        while self._connected and self._ws and not self._ws.closed:
            try:
                await asyncio.sleep(15)  # Ping every 15 seconds
                if self._connected and self._ws and not self._ws.closed:
                    ping_msg = self._encode_ping()
                    await self._ws.send_bytes(ping_msg)
                    _LOGGER.debug("Sent ping")
            except asyncio.CancelledError:
                break
            except Exception as err:
                _LOGGER.error("Error in ping loop: %s", err)
                self._transition_disconnected("ping failure")
                break


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
    ) -> None:
        """Initialize the manager."""
        self._session = session
        self._on_voltage_update = on_voltage_update
        self._on_power_quality_update = on_power_quality_update
        self._on_availability_update = on_availability_update
        self._on_health_update = on_health_update
        self._connections: dict[str, WhiskerWebSocket] = {}
        self._voltage_data: dict[str, VoltageData] = {}
        self._station_states: dict[str, StationState] = {}
        self._credentials: dict[str, dict] = {}  # Store credentials for reconnect
        self._reconnect_tasks: dict[str, asyncio.Task] = {}
        self._reconnect_attempts: dict[str, int] = {}
        self._shutting_down = False

    def get_station_state(self, station_id: str) -> StationState:
        """Return the independently tracked state for a station."""
        return self._station_states.get(station_id, StationState())

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
                ws = WhiskerWebSocket(
                    session=self._session,
                    api_key=creds["api_key"],
                    user_id=creds["user_id"],
                    station_id=station_id,
                    on_voltage_update=self._handle_voltage_update,
                    on_power_quality_update=self._on_power_quality_update,
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
            on_power_quality_update=self._on_power_quality_update,
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
