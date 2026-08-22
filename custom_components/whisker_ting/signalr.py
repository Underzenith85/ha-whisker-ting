"""Minimal ASP.NET Core SignalR MessagePack encoding helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import msgpack

MAX_VARINT_VALUE = 0x7FFFFFFF
MAX_VARINT_BYTES = 5

MSG_TYPE_INVOCATION = 1
MSG_TYPE_COMPLETION = 3
MSG_TYPE_PING = 6

COMPLETION_ERROR = 1
COMPLETION_VOID = 2
COMPLETION_RESULT = 3


class SignalRProtocolError(ValueError):
    """Raised when a SignalR binary message is malformed or incomplete."""


@dataclass(frozen=True)
class CompletionMessage:
    """A decoded SignalR Completion message."""

    invocation_id: str
    error: str | None = None
    result: Any = None


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


def decode_varint(data: bytes, offset: int = 0) -> tuple[int, int]:
    """Decode a SignalR VarInt and return its value and next offset."""
    value = 0
    for index in range(MAX_VARINT_BYTES):
        position = offset + index
        if position >= len(data):
            raise SignalRProtocolError("Incomplete SignalR frame length")

        byte = data[position]
        if index == MAX_VARINT_BYTES - 1 and byte & 0xF8:
            raise SignalRProtocolError("SignalR frame length exceeds 31 bits")

        value |= (byte & 0x7F) << (index * 7)
        if byte & 0x80 == 0:
            return value, position + 1

    raise SignalRProtocolError("SignalR frame length VarInt is too long")


def frame_message(payload: bytes) -> bytes:
    """Prefix a binary SignalR hub message with its VarInt length."""
    return encode_varint(len(payload)) + payload


def decode_hub_messages(data: bytes) -> list[list[Any]]:
    """Decode all length-prefixed MessagePack hub messages in a payload."""
    messages: list[list[Any]] = []
    offset = 0

    while offset < len(data):
        length, payload_offset = decode_varint(data, offset)
        if length == 0:
            raise SignalRProtocolError("SignalR hub message cannot be empty")

        payload_end = payload_offset + length
        if payload_end > len(data):
            raise SignalRProtocolError("Incomplete SignalR hub message")

        try:
            message = msgpack.unpackb(
                data[payload_offset:payload_end],
                raw=False,
                strict_map_key=False,
                timestamp=3,
            )
        except (msgpack.UnpackException, ValueError) as err:
            raise SignalRProtocolError("Invalid MessagePack hub message") from err

        if not isinstance(message, list) or not message:
            raise SignalRProtocolError("SignalR hub message must be a non-empty array")

        messages.append(message)
        offset = payload_end

    return messages


def extract_invocation_payloads(data: bytes, target: str) -> list[dict[str, Any]]:
    """Return object payloads from Invocation messages matching a target."""
    payloads: list[dict[str, Any]] = []

    for message in decode_hub_messages(data):
        if message[0] != MSG_TYPE_INVOCATION:
            continue
        if len(message) < 5:
            raise SignalRProtocolError("SignalR Invocation message is too short")
        if message[3] != target:
            continue

        arguments = message[4]
        if not isinstance(arguments, list) or not arguments:
            raise SignalRProtocolError("SignalR Invocation has no arguments")

        payload = arguments[0]
        if isinstance(payload, bytes):
            try:
                payload = msgpack.unpackb(
                    payload,
                    raw=False,
                    strict_map_key=False,
                    timestamp=3,
                )
            except (msgpack.UnpackException, ValueError) as err:
                raise SignalRProtocolError(
                    "Invalid nested MessagePack invocation payload"
                ) from err

        if not isinstance(payload, dict):
            raise SignalRProtocolError("SignalR Invocation payload must be an object")

        payloads.append(payload)

    return payloads


def extract_completions(data: bytes) -> list[CompletionMessage]:
    """Return validated Completion messages from framed hub data."""
    completions: list[CompletionMessage] = []

    for message in decode_hub_messages(data):
        if message[0] != MSG_TYPE_COMPLETION:
            continue
        if len(message) < 4:
            raise SignalRProtocolError("SignalR Completion message is too short")

        invocation_id = message[2]
        if not isinstance(invocation_id, str) or not invocation_id:
            raise SignalRProtocolError("SignalR Completion has no invocation ID")

        result_kind = message[3]
        if result_kind == COMPLETION_ERROR:
            if len(message) < 5 or not isinstance(message[4], str):
                raise SignalRProtocolError("SignalR error Completion has no error")
            completions.append(
                CompletionMessage(invocation_id=invocation_id, error=message[4])
            )
        elif result_kind == COMPLETION_VOID:
            completions.append(CompletionMessage(invocation_id=invocation_id))
        elif result_kind == COMPLETION_RESULT:
            if len(message) < 5:
                raise SignalRProtocolError("SignalR result Completion has no result")
            completions.append(
                CompletionMessage(invocation_id=invocation_id, result=message[4])
            )
        else:
            raise SignalRProtocolError(
                f"Unknown SignalR Completion result kind: {result_kind}"
            )

    return completions


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
