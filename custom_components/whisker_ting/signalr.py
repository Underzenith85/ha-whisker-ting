"""Minimal ASP.NET Core SignalR MessagePack encoding helpers."""

from __future__ import annotations

from typing import Any

import msgpack

MAX_VARINT_VALUE = 0x7FFFFFFF

MSG_TYPE_INVOCATION = 1
MSG_TYPE_PING = 6


def encode_varint(value: int) -> bytes:
    """Encode a non-negative 31-bit integer as a SignalR VarInt."""
    if not 0 <= value <= MAX_VARINT_VALUE:
        raise ValueError(f"SignalR frame length out of range: {value}")

    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def frame_message(payload: bytes) -> bytes:
    """Prefix a binary SignalR hub message with its VarInt length."""
    return encode_varint(len(payload)) + payload


def encode_hub_message(message: list[Any]) -> bytes:
    """Encode and frame a SignalR MessagePack hub message array."""
    payload = msgpack.packb(message, use_bin_type=True)
    return frame_message(payload)


def encode_invocation(
    invocation_id: str,
    target: str,
    arguments: list[Any],
) -> bytes:
    """Encode a non-streaming SignalR Invocation message."""
    return encode_hub_message(
        [
            MSG_TYPE_INVOCATION,
            {},
            invocation_id,
            target,
            arguments,
        ]
    )


def encode_ping() -> bytes:
    """Encode a SignalR Ping message."""
    return encode_hub_message([MSG_TYPE_PING])
