"""Offline tests for SignalR invocation correlation."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.whisker_ting import signalr, websocket


class FakeMessage:
    """Minimal aiohttp WebSocket message."""

    def __init__(self, message_type: Any, data: Any) -> None:
        self.type = message_type
        self.data = data


class FakeWebSocket:
    """Minimal WebSocket transport for invocation tests."""

    def __init__(self) -> None:
        self.closed = False
        self.sent: list[bytes] = []
        self.message_sent = asyncio.Event()
        self.text_sent: list[str] = []
        self.responses: list[FakeMessage | Exception] = []

    async def send_bytes(self, data: bytes) -> None:
        """Capture a binary message."""
        self.sent.append(data)
        self.message_sent.set()

    async def close(self) -> None:
        """Close the fake transport."""
        self.closed = True

    async def send_str(self, data: str) -> None:
        """Capture a text message."""
        self.text_sent.append(data)

    async def receive(self, timeout: float | None = None) -> FakeMessage:
        """Return or raise the next configured response."""
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _client() -> tuple[Any, FakeWebSocket]:
    """Create a client with a connected fake transport."""
    client = websocket.WhiskerWebSocket(object(), "api-key", 42, "ABC123")
    transport = FakeWebSocket()
    client._ws = transport
    client._connected = True
    return client, transport


def test_invocation_resolves_only_matching_completion() -> None:
    """An unrelated Completion cannot resolve a pending invocation."""

    async def scenario() -> None:
        client, transport = _client()
        invocation = asyncio.create_task(
            client._invoke("InitializeStreaming", [], timeout=1)
        )
        await transport.message_sent.wait()

        client._handle_completions(signalr.encode_hub_message([3, {}, "999", 2]))
        await asyncio.sleep(0)
        assert not invocation.done()

        client._handle_completions(signalr.encode_hub_message([3, {}, "1", 2]))
        assert await invocation is None
        assert client._pending_invocations == {}

    asyncio.run(scenario())


def test_invocation_surfaces_completion_error() -> None:
    """A server Completion error fails the matching invocation."""

    async def scenario() -> None:
        client, transport = _client()
        invocation = asyncio.create_task(client._invoke("method", [], timeout=1))
        await transport.message_sent.wait()
        client._handle_completions(
            signalr.encode_hub_message([3, {}, "1", 1, "not authorized"])
        )

        with pytest.raises(
            websocket.SignalRInvocationError, match="rejected by server"
        ):
            await invocation
        assert client._pending_invocations == {}

    asyncio.run(scenario())


def test_subscription_marks_client_connected_after_completion() -> None:
    """The client reports connected only after subscription is acknowledged."""

    async def scenario() -> None:
        client, transport = _client()
        subscription = asyncio.create_task(client._subscribe([]))
        await transport.message_sent.wait()
        assert not client.connected

        client._handle_completions(signalr.encode_hub_message([3, {}, "1", 2]))
        await subscription
        assert client.connected

    asyncio.run(scenario())


def test_invocation_timeout_cleans_up_pending_future() -> None:
    """A missing Completion times out and removes its pending future."""

    async def scenario() -> None:
        client, _ = _client()
        with pytest.raises(websocket.SignalRInvocationError, match="timed out"):
            await client._invoke("method", [], timeout=0.001)
        assert client._pending_invocations == {}

    asyncio.run(scenario())


def test_disconnect_fails_pending_invocation() -> None:
    """Disconnect fails and cleans up every pending invocation."""

    async def scenario() -> None:
        client, transport = _client()
        invocation = asyncio.create_task(client._invoke("method", [], timeout=1))
        await transport.message_sent.wait()
        await client.disconnect()

        with pytest.raises(websocket.SignalRInvocationError, match="disconnected"):
            await invocation
        assert client._pending_invocations == {}
        assert transport.closed

    asyncio.run(scenario())


def test_handshake_succeeds_with_terminated_empty_response() -> None:
    """The client sends and validates the MessagePack handshake."""

    async def scenario() -> None:
        client, transport = _client()
        transport.responses.append(
            FakeMessage(websocket.aiohttp.WSMsgType.TEXT, "{}\x1e")
        )
        await client._perform_handshake()
        assert transport.text_sent == ['{"protocol":"messagepack","version":1}\x1e']

    asyncio.run(scenario())


def test_handshake_timeout_is_a_protocol_failure() -> None:
    """A missing handshake response raises a distinct handshake error."""

    async def scenario() -> None:
        client, transport = _client()
        transport.responses.append(TimeoutError())
        with pytest.raises(signalr.SignalRHandshakeError, match="timed out"):
            await client._perform_handshake()

    asyncio.run(scenario())


def test_handshake_accepts_binary_response() -> None:
    """Ting sends its MessagePack handshake response as binary data."""

    async def scenario() -> None:
        client, transport = _client()
        transport.responses.append(
            FakeMessage(websocket.aiohttp.WSMsgType.BINARY, b"{}\x1e")
        )
        assert await client._perform_handshake() is None

    asyncio.run(scenario())


def test_power_quality_categorical_updates_are_modeled() -> None:
    """Frequency and THD records from the 3.0.4 stream reach their callback."""
    updates: list[tuple[str, websocket.PowerQualityData]] = []
    client, _ = _client()
    client._on_power_quality_update = lambda station, reading: updates.append(
        (station, reading)
    )
    frame = signalr.encode_hub_message(
        [
            1,
            {},
            None,
            "updateGraphMultiCategorical",
            [
                [
                    {
                        "Category": "frequency",
                        "ObsTime": "2026-08-22T00:00:00Z",
                        "Value": "60.01",
                    },
                    {
                        "Category": "thdAvg",
                        "ObsTime": "2026-08-22T00:00:00Z",
                        "Value": "2.4",
                    },
                ]
            ],
        ]
    )

    assert client._handle_binary_message(frame) == []
    assert [
        (station, reading.category, reading.value) for station, reading in updates
    ] == [
        ("ABC123", "frequency", 60.01),
        ("ABC123", "thdAvg", 2.4),
    ]


def test_close_transitions_and_notifies_exactly_once() -> None:
    """A Close message propagates one sanitized reconnect decision."""

    async def scenario() -> None:
        notifications: list[tuple[str, str, bool]] = []
        client, transport = _client()
        client._on_disconnect = lambda station, reason, reconnect: notifications.append(
            (station, reason, reconnect)
        )
        transport.responses.append(
            FakeMessage(
                websocket.aiohttp.WSMsgType.BINARY,
                signalr.encode_hub_message([7, "api_key=secret-value", True]),
            )
        )

        await client._receive_loop()
        client._transition_disconnected("second transition")

        assert not client.connected
        assert notifications == [("ABC123", "api_key=[redacted]", True)]

    asyncio.run(scenario())


def test_server_close_can_disable_manager_reconnect() -> None:
    """The manager honors the structured Close allow-reconnect flag."""
    manager = websocket.WhiskerWebSocketManager(object())
    manager._connections["ABC123"] = object()

    manager._handle_disconnect("ABC123", "server closed the SignalR connection", False)

    assert "ABC123" not in manager._connections
    assert manager._reconnect_tasks == {}


def test_manager_reconnect_uses_exponential_backoff() -> None:
    """Reconnect attempts are throttled and replace the disconnected client."""

    async def scenario() -> None:
        manager = websocket.WhiskerWebSocketManager(object())
        manager._credentials["ABC123"] = {"api_key": "fixture-key", "user_id": 42}
        manager._reconnect_attempts["ABC123"] = 2
        replacement = MagicMock()
        replacement.connect = AsyncMock(return_value=True)
        sleep = AsyncMock()

        with (
            patch.object(websocket.asyncio, "sleep", sleep),
            patch.object(websocket.random, "uniform", return_value=2),
            patch.object(websocket, "WhiskerWebSocket", return_value=replacement),
        ):
            manager._handle_disconnect("ABC123", "transport closed", True)
            await manager._reconnect_tasks["ABC123"]

        sleep.assert_awaited_once_with(22)
        assert manager._reconnect_attempts["ABC123"] == 3
        assert manager._connections["ABC123"] is replacement

    asyncio.run(scenario())


def test_multi_device_station_state_is_independent() -> None:
    """One station can lose liveness without affecting another station."""

    async def scenario() -> None:
        availability: list[tuple[str, bool]] = []
        manager = websocket.WhiskerWebSocketManager(
            object(),
            on_availability_update=lambda station, live: availability.append(
                (station, live)
            ),
        )
        clients = [MagicMock(connected=True), MagicMock(connected=True)]
        for client in clients:
            client.connect = AsyncMock(return_value=True)

        with patch.object(websocket, "WhiskerWebSocket", side_effect=clients):
            assert await manager.connect_device("key", 42, "STATION-A")
            assert await manager.connect_device("key", 42, "STATION-B")

        reading = websocket.VoltageData(datetime.now(UTC), 120, 121, 119, 4)
        manager._handle_voltage_update("STATION-A", reading)
        assert manager.is_station_available("STATION-A")
        assert not manager.is_station_available("STATION-B")

        manager._handle_voltage_update("STATION-B", reading)
        manager._handle_disconnect("STATION-A", "stale", False)
        assert not manager.is_station_available("STATION-A")
        assert manager.is_station_available("STATION-B")
        assert availability == [
            ("STATION-A", True),
            ("STATION-B", True),
            ("STATION-A", False),
        ]

    asyncio.run(scenario())


def test_duplicate_disconnect_schedules_one_reconnect_task() -> None:
    """Competing stale and receive-loop paths cannot create duplicate retries."""

    async def scenario() -> None:
        manager = websocket.WhiskerWebSocketManager(object())
        reconnect = AsyncMock()
        manager._reconnect_with_backoff = reconnect

        manager._handle_disconnect("ABC123", "stale", True)
        task = manager._reconnect_tasks["ABC123"]
        manager._handle_disconnect("ABC123", "transport", True)

        assert manager._reconnect_tasks["ABC123"] is task
        await task
        reconnect.assert_awaited_once_with("ABC123")

    asyncio.run(scenario())


def test_disconnect_cancels_tasks_and_closes_socket() -> None:
    """Client shutdown leaves no background task or transport running."""

    async def scenario() -> None:
        client, transport = _client()
        client._ping_task = asyncio.create_task(asyncio.sleep(60))
        client._receive_task = asyncio.create_task(asyncio.sleep(60))
        client._stale_check_task = asyncio.create_task(asyncio.sleep(60))
        tasks = [
            client._ping_task,
            client._receive_task,
            client._stale_check_task,
        ]

        await client.disconnect()

        assert transport.closed
        assert all(task.done() for task in tasks)
        assert client._ws is None
        assert client._ping_task is None
        assert client._receive_task is None
        assert client._stale_check_task is None

    asyncio.run(scenario())


def test_disconnect_unsubscribes_initialized_stream_before_close() -> None:
    """Intentional shutdown releases the station subscription first."""

    async def scenario() -> None:
        client, transport = _client()
        client._subscribed = True
        client._invoke = AsyncMock(return_value=None)
        client._receive_task = asyncio.create_task(asyncio.sleep(60))

        await client.disconnect()

        client._invoke.assert_awaited_once_with(
            "UnInitializeStreaming",
            [
                {"StationId": "ABC123", "DataElement": "ComboBinaryData"},
                "api-key",
                "42",
            ],
            timeout=client.UNSUBSCRIBE_TIMEOUT,
        )
        assert transport.closed
        assert not client._subscribed

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "error",
    [
        websocket.SignalRInvocationError("rejected by server"),
        websocket.SignalRInvocationError("timed out"),
    ],
)
def test_disconnect_closes_transport_when_unsubscribe_fails(error: Exception) -> None:
    """Server errors and timeouts cannot block transport cleanup."""

    async def scenario() -> None:
        client, transport = _client()
        client._subscribed = True
        client._invoke = AsyncMock(side_effect=error)

        await client.disconnect()

        assert transport.closed
        assert client._ws is None
        assert not client._subscribed

    asyncio.run(scenario())


def test_disconnect_without_subscription_is_idempotent() -> None:
    """Uninitialized and repeated shutdowns never invoke server teardown."""

    async def scenario() -> None:
        client, transport = _client()
        client._invoke = AsyncMock()

        await client.disconnect()
        await client.disconnect()

        client._invoke.assert_not_awaited()
        assert transport.closed

    asyncio.run(scenario())


def test_stream_health_uses_staged_thresholds_with_controllable_clock() -> None:
    """Five and ten second thresholds produce delayed then unavailable states."""

    async def scenario() -> None:
        health: list[websocket.StreamHealth] = []
        clock_values = iter([6.0, 10.0])
        client, _ = _client()
        client._health = websocket.StreamHealth.RECEIVING
        client._last_data_monotonic = 0.0
        client._monotonic = lambda: next(clock_values)
        client._on_health_update = lambda station, value: health.append(value)

        with patch.object(websocket.asyncio, "sleep", AsyncMock()):
            await client._stale_data_check_loop()

        assert health == [
            websocket.StreamHealth.DELAYED,
            websocket.StreamHealth.NOT_RECEIVING,
        ]
        assert not client.connected

    asyncio.run(scenario())


def test_delayed_stream_retains_last_reading_and_availability() -> None:
    """Delayed data remains usable until the not-receiving threshold."""
    availability: list[bool] = []
    health: list[websocket.StreamHealth] = []
    manager = websocket.WhiskerWebSocketManager(
        object(),
        on_availability_update=lambda station, value: availability.append(value),
        on_health_update=lambda station, value: health.append(value),
    )
    manager._station_states["ABC123"] = websocket.StationState(
        connected=True,
        subscribed=True,
        live=True,
        health=websocket.StreamHealth.RECEIVING,
    )
    reading = websocket.VoltageData(datetime.now(UTC), 120, 121, 119, 4)
    manager._voltage_data["ABC123"] = reading

    manager._handle_health_update("ABC123", websocket.StreamHealth.DELAYED)

    assert manager.is_station_available("ABC123")
    assert manager.get_voltage_data("ABC123") is reading
    assert availability == []
    assert health == [websocket.StreamHealth.DELAYED]

    manager._handle_health_update("ABC123", websocket.StreamHealth.NOT_RECEIVING)

    assert not manager.is_station_available("ABC123")
    assert manager.get_voltage_data("ABC123") is reading
    assert availability == [False]
    assert health[-1] is websocket.StreamHealth.NOT_RECEIVING
