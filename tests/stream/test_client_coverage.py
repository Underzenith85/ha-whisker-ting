"""Additional lifecycle coverage for SignalR clients and managers."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.whisker_ting.stream import client as websocket
from custom_components.whisker_ting.stream import manager as stream_manager
from custom_components.whisker_ting.stream.models import (
    PowerQualityCategory,
    PowerQualityData,
    VoltageData,
)
from custom_components.whisker_ting.stream.signalr import SignalRHandshakeError


class FakeMessage:
    """Minimal websocket message."""

    def __init__(self, message_type: object, data: object) -> None:
        self.type = message_type
        self.data = data


class FakeWebSocket:
    """Minimal websocket transport."""

    def __init__(self) -> None:
        self.closed = False
        self.sent: list[bytes] = []
        self.text_sent: list[str] = []
        self.responses: list[FakeMessage | Exception] = []

    async def send_bytes(self, data: bytes) -> None:
        self.sent.append(data)

    async def send_str(self, data: str) -> None:
        self.text_sent.append(data)

    async def receive(self, timeout: float | None = None) -> FakeMessage:
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    async def close(self) -> None:
        self.closed = True


def make_client(**callbacks: object) -> websocket.WhiskerWebSocket:
    """Construct a client with synthetic credentials."""
    return websocket.WhiskerWebSocket(
        MagicMock(), "api-key", 42, "STATION-A", **callbacks
    )


@pytest.mark.asyncio
async def test_connect_success_initial_payload_and_optional_failure() -> None:
    """Connect starts all tasks and tolerates unavailable optional streams."""
    client = make_client()
    transport = FakeWebSocket()
    client._session.ws_connect = AsyncMock(return_value=transport)
    client._perform_handshake = AsyncMock(return_value=b"initial")
    client._handle_binary_message = MagicMock(return_value=[])

    async def subscribe(args: list[object]) -> None:
        client._subscribed = True

    client._subscribe = AsyncMock(side_effect=subscribe)
    client._invoke = AsyncMock(
        side_effect=[None, websocket.SignalRInvocationError("unsupported"), None, None]
    )
    client._receive_loop = AsyncMock()
    client._ping_loop = AsyncMock()
    client._stale_data_check_loop = AsyncMock()

    assert await client.connect()
    await asyncio.sleep(0)
    assert client.connected
    client._handle_binary_message.assert_called_once_with(b"initial")
    assert len(client._subscribed_elements) == 3
    await client.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [SignalRHandshakeError("bad handshake"), RuntimeError("transport failure")],
)
async def test_connect_failures_cleanup_transport(failure: Exception) -> None:
    """Handshake and generic failures close a partially opened transport."""
    client = make_client()
    transport = FakeWebSocket()
    client._session.ws_connect = AsyncMock(return_value=transport)
    client._perform_handshake = AsyncMock(side_effect=failure)
    assert not await client.connect()
    assert transport.closed
    assert client._ws is None


@pytest.mark.asyncio
async def test_cleanup_failed_connect_cancels_receive_task() -> None:
    """Failed setup cancels a receive loop and fails pending invocations."""
    client = make_client()
    transport = FakeWebSocket()
    client._ws = transport
    client._connected = True
    client._receive_task = asyncio.create_task(asyncio.sleep(60))
    pending = asyncio.get_running_loop().create_future()
    client._pending_invocations["1"] = pending
    await client._cleanup_failed_connect()
    assert transport.closed
    assert client._receive_task is None
    with pytest.raises(websocket.SignalRInvocationError):
        pending.result()


@pytest.mark.asyncio
async def test_handshake_rejects_missing_transport_and_invalid_messages() -> None:
    """Handshake validates transport, message type, and binary remainder."""
    client = make_client()
    with pytest.raises(SignalRHandshakeError, match="not connected"):
        await client._perform_handshake()

    transport = FakeWebSocket()
    client._ws = transport
    transport.responses.append(FakeMessage(websocket.aiohttp.WSMsgType.CLOSE, b""))
    with pytest.raises(SignalRHandshakeError, match="invalid type"):
        await client._perform_handshake()

    transport.responses.append(
        FakeMessage(websocket.aiohttp.WSMsgType.TEXT, "{}\x1etext remainder")
    )
    with (
        patch.object(websocket, "decode_handshake_response", return_value="text"),
        pytest.raises(SignalRHandshakeError, match="not binary"),
    ):
        await client._perform_handshake()


@pytest.mark.asyncio
async def test_wait_for_data_success_and_timeout() -> None:
    """Waiting reports both arrival and bounded timeout."""
    client = make_client()
    client._first_data_received.set()
    assert await client.wait_for_data(0.01)
    client._first_data_received.clear()
    assert not await client.wait_for_data(0.001)


def test_binary_message_updates_all_callbacks_and_health() -> None:
    """One binary payload publishes voltage, quality, and first-data state."""
    voltage_updates: list[VoltageData] = []
    quality_updates: list[PowerQualityData] = []
    health: list[websocket.StreamHealth] = []
    client = make_client(
        on_voltage_update=lambda station, data: voltage_updates.append(data),
        on_power_quality_update=lambda station, data: quality_updates.append(data),
        on_health_update=lambda station, value: health.append(value),
    )
    client._health = websocket.StreamHealth.DELAYED
    voltage = VoltageData(datetime.now(UTC), 120, 121, 119, 4)
    quality = PowerQualityData(datetime.now(UTC), PowerQualityCategory.FREQUENCY, 60)
    client._decode_voltage_data = MagicMock(return_value=[voltage])
    client._decode_power_quality_data = MagicMock(return_value=[quality])
    with (
        patch.object(websocket, "extract_completions", return_value=[]),
        patch.object(websocket, "extract_control_messages", return_value=(2, [])),
    ):
        assert client._handle_binary_message(b"frame") == []
    assert voltage_updates == [voltage]
    assert quality_updates == [quality]
    assert health == [websocket.StreamHealth.RECEIVING]
    assert client._first_data_received.is_set()


@pytest.mark.asyncio
async def test_receive_loop_handles_text_protocol_close_timeout_and_error() -> None:
    """All transport message and exception branches terminate safely."""
    scenarios: list[tuple[object, str | None]] = [
        (FakeMessage(websocket.aiohttp.WSMsgType.TEXT, "unexpected"), None),
        (FakeMessage(websocket.aiohttp.WSMsgType.CLOSED, b""), "transport closed"),
        (RuntimeError("receive failed"), "receive failure"),
    ]
    for response, expected in scenarios:
        notifications: list[str] = []
        client = make_client(
            on_disconnect=lambda station, reason, reconnect, target=notifications: (
                target.append(reason)
            )
        )
        transport = FakeWebSocket()
        transport.responses.append(response)
        if expected is None:
            transport.responses.append(
                FakeMessage(websocket.aiohttp.WSMsgType.CLOSED, b"")
            )
        client._ws = transport
        client._connected = True
        await client._receive_loop()
        assert transport.closed
        assert any((expected or "transport closed") in item for item in notifications)

    client = make_client()
    transport = FakeWebSocket()
    transport.responses.append(
        FakeMessage(websocket.aiohttp.WSMsgType.BINARY, b"protocol")
    )
    client._ws = transport
    client._connected = True
    client._handle_binary_message = MagicMock(
        side_effect=websocket.SignalRProtocolError("bad")
    )
    await client._receive_loop()
    assert not client.connected


@pytest.mark.asyncio
async def test_ping_loop_sends_and_handles_failure() -> None:
    """Ping loop sends keepalives and disconnects after write failure."""
    client = make_client()
    transport = FakeWebSocket()
    client._ws = transport
    client._connected = True

    async def stop_after_send(data: bytes) -> None:
        transport.sent.append(data)
        client._connected = False

    transport.send_bytes = stop_after_send
    with patch.object(websocket.asyncio, "sleep", AsyncMock()):
        await client._ping_loop()
    assert transport.sent

    notifications: list[str] = []
    client = make_client(
        on_disconnect=lambda station, reason, reconnect: notifications.append(reason)
    )
    transport = FakeWebSocket()
    transport.send_bytes = AsyncMock(side_effect=RuntimeError("write failed"))
    client._ws = transport
    client._connected = True
    with patch.object(websocket.asyncio, "sleep", AsyncMock()):
        await client._ping_loop()
    assert notifications == ["ping failure"]


@pytest.mark.asyncio
async def test_manager_connect_disconnect_and_missing_data_paths() -> None:
    """Manager covers duplicates, failed connects, targeted cleanup, and no client."""
    manager = stream_manager.WhiskerWebSocketManager(MagicMock())
    connected = MagicMock(connected=True)
    manager._connections["existing"] = connected
    assert await manager.connect_device("key", 1, "existing")

    stale = MagicMock(connected=False)
    replacement = MagicMock(connect=AsyncMock(return_value=False))
    manager._connections["failed"] = stale
    with patch.object(stream_manager, "WhiskerWebSocket", return_value=replacement):
        assert not await manager.connect_device("key", 1, "failed")

    reconnect = asyncio.create_task(asyncio.sleep(60))
    manager._reconnect_tasks["busy"] = reconnect
    assert not await manager.connect_device("key", 1, "busy")
    assert not await manager.wait_for_data("missing", 0.01)

    active = MagicMock(disconnect=AsyncMock())
    manager._connections["target"] = active
    manager._credentials["target"] = {"api_key": "key", "user_id": 1}
    manager._reconnect_tasks["target"] = asyncio.create_task(asyncio.sleep(60))
    await manager.disconnect_device("target")
    active.disconnect.assert_awaited_once()
    assert "target" not in manager._credentials

    reconnect.cancel()
    with pytest.raises(asyncio.CancelledError):
        await reconnect


@pytest.mark.asyncio
async def test_manager_reconnect_without_credentials_and_failed_attempt() -> None:
    """Reconnect exits without credentials and retries failed clients until shutdown."""
    manager = stream_manager.WhiskerWebSocketManager(MagicMock())
    await manager._reconnect_with_backoff("missing")

    manager._credentials["station"] = {"api_key": "key", "user_id": 1}
    replacement = MagicMock(connect=AsyncMock(side_effect=[False, True]))
    with (
        patch.object(stream_manager.asyncio, "sleep", AsyncMock()),
        patch.object(stream_manager.random, "uniform", return_value=0),
        patch.object(stream_manager, "WhiskerWebSocket", return_value=replacement),
    ):
        await manager._reconnect_with_backoff("station")
    assert replacement.connect.await_count == 2


@pytest.mark.asyncio
async def test_manager_disconnect_all_cancels_reconnects_and_active_clients() -> None:
    """Global shutdown cancels reconnect work and disconnects every station."""
    manager = stream_manager.WhiskerWebSocketManager(MagicMock())
    reconnect = asyncio.create_task(asyncio.sleep(60))
    active = MagicMock(disconnect=AsyncMock())
    manager._reconnect_tasks["station"] = reconnect
    manager._connections["station"] = active
    manager._station_states["station"] = stream_manager.StationState()
    manager._voltage_data["station"] = VoltageData(datetime.now(UTC), 120, 121, 119, 4)
    await manager.disconnect_all()
    assert reconnect.cancelled()
    active.disconnect.assert_awaited_once()
    assert manager._connections == {}


@pytest.mark.asyncio
async def test_manager_wait_for_connected_station() -> None:
    """Manager delegates first-data waiting to an active station client."""
    manager = stream_manager.WhiskerWebSocketManager(MagicMock())
    active = MagicMock(wait_for_data=AsyncMock(return_value=True))
    manager._connections["station"] = active
    assert await manager.wait_for_data("station", 2)
    active.wait_for_data.assert_awaited_once_with(timeout=2)


def test_manager_callbacks_and_sanitizer_defaults() -> None:
    """Optional callbacks and empty reconnect reasons cover their safe branches."""
    quality: list[PowerQualityData] = []
    diagnostics: list[str] = []
    manager = stream_manager.WhiskerWebSocketManager(
        MagicMock(),
        on_power_quality_update=lambda station, data: quality.append(data),
        on_diagnostics_update=lambda station, data: diagnostics.append(station),
    )
    reading = PowerQualityData(datetime.now(UTC), PowerQualityCategory.FREQUENCY, 60)
    manager._handle_power_quality_update("station", reading)
    assert quality == [reading]
    assert diagnostics == ["station"]
    assert stream_manager._sanitize_reconnect_reason("   ") == "unspecified"
