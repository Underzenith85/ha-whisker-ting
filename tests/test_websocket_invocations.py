"""Offline tests for SignalR invocation correlation."""

from __future__ import annotations

import asyncio
import importlib
import sys
from pathlib import Path
from types import ModuleType
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


class FakeWebSocket:
    """Minimal WebSocket transport for invocation tests."""

    def __init__(self) -> None:
        self.closed = False
        self.sent: list[bytes] = []
        self.message_sent = asyncio.Event()

    async def send_bytes(self, data: bytes) -> None:
        """Capture a binary message."""
        self.sent.append(data)
        self.message_sent.set()

    async def close(self) -> None:
        """Close the fake transport."""
        self.closed = True


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
