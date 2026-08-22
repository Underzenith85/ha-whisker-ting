"""Offline tests for SignalR invocation correlation."""

from __future__ import annotations

import asyncio
import importlib
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

ROOT = Path(__file__).parents[1]
PACKAGE_PATH = ROOT / "custom_components" / "whisker_ting"


def _load_websocket_module() -> ModuleType:
    """Load websocket.py with a minimal aiohttp stub and no Home Assistant import."""
    aiohttp = ModuleType("aiohttp")
    aiohttp.ClientSession = object
    aiohttp.ClientWebSocketResponse = object
    aiohttp.WSMsgType = object
    sys.modules.setdefault("aiohttp", aiohttp)

    package = ModuleType("custom_components.whisker_ting")
    package.__path__ = [str(PACKAGE_PATH)]
    sys.modules.setdefault("custom_components.whisker_ting", package)
    return importlib.import_module("custom_components.whisker_ting.websocket")


websocket = _load_websocket_module()
signalr = importlib.import_module("custom_components.whisker_ting.signalr")
websocket.aiohttp.WSMsgType = SimpleNamespace(TEXT=1, BINARY=2, CLOSED=3, ERROR=4)


class FakeMessage:
    """Minimal aiohttp WebSocket message."""

    def __init__(self, message_type: int, data: Any) -> None:
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
        assert transport.text_sent == [
            '{"protocol":"messagepack","version":1}\x1e'
        ]

    asyncio.run(scenario())


def test_handshake_timeout_is_a_protocol_failure() -> None:
    """A missing handshake response raises a distinct handshake error."""

    async def scenario() -> None:
        client, transport = _client()
        transport.responses.append(TimeoutError())
        with pytest.raises(signalr.SignalRHandshakeError, match="timed out"):
            await client._perform_handshake()

    asyncio.run(scenario())


def test_handshake_rejects_unexpected_websocket_type() -> None:
    """Binary and control messages cannot masquerade as a handshake response."""

    async def scenario() -> None:
        client, transport = _client()
        transport.responses.append(
            FakeMessage(websocket.aiohttp.WSMsgType.BINARY, b"{}\x1e")
        )
        with pytest.raises(signalr.SignalRHandshakeError, match="not text"):
            await client._perform_handshake()

    asyncio.run(scenario())


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
                signalr.encode_hub_message(
                    [7, "api_key=secret-value", True]
                ),
            )
        )

        await client._receive_loop()
        client._transition_disconnected("second transition")

        assert not client.connected
        assert notifications == [
            ("ABC123", "api_key=[redacted]", True)
        ]

    asyncio.run(scenario())


def test_server_close_can_disable_manager_reconnect() -> None:
    """The manager honors the structured Close allow-reconnect flag."""
    manager = websocket.WhiskerWebSocketManager(object())
    manager._connections["ABC123"] = object()

    manager._handle_disconnect(
        "ABC123", "server closed the SignalR connection", False
    )

    assert "ABC123" not in manager._connections
    assert manager._reconnect_tasks == {}
