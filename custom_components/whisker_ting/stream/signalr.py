"""Generic ASP.NET Core SignalR MessagePack framing and decoding."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

import msgpack

MAX_VARINT_VALUE = 0x7FFFFFFF
MAX_VARINT_BYTES = 5

MSG_TYPE_INVOCATION = 1
MSG_TYPE_COMPLETION = 3
MSG_TYPE_PING = 6
MSG_TYPE_CLOSE = 7

RECORD_SEPARATOR = "\x1e"

COMPLETION_ERROR = 1
COMPLETION_VOID = 2
COMPLETION_RESULT = 3


class SignalRProtocolError(ValueError):
    """Raised when a SignalR binary message is malformed or incomplete."""


class SignalRHandshakeError(SignalRProtocolError):
    """Raised when the SignalR JSON handshake response is invalid or rejected."""


@dataclass(frozen=True)
class CompletionMessage:
    """A decoded SignalR Completion message."""

    invocation_id: str
    error: str | None = None
    result: Any = None


@dataclass(frozen=True)
class CloseMessage:
    """A decoded SignalR Close message."""

    reason: str
    allow_reconnect: bool = False


def decode_handshake_response(data: str | bytes) -> str | bytes | None:
    """Validate a SignalR handshake and return any coalesced hub payload.

    SignalR permits the JSON handshake response to arrive in either a text or
    binary WebSocket message. With the MessagePack protocol the official client
    requests a binary transfer format, and Ting responds with binary ``{}\x1e``.
    A server may append the first hub message after the record separator.
    """
    if isinstance(data, bytes):
        separator_index = data.find(RECORD_SEPARATOR.encode())
    elif isinstance(data, str):
        separator_index = data.find(RECORD_SEPARATOR)
    else:
        raise SignalRHandshakeError("SignalR handshake has an invalid type")
    if separator_index < 0:
        raise SignalRHandshakeError(
            "SignalR handshake is not record-separator terminated"
        )

    encoded_response = data[:separator_index]
    remainder = data[separator_index + 1 :]
    if isinstance(encoded_response, bytes):
        try:
            encoded_response = encoded_response.decode("utf-8")
        except UnicodeError as err:
            raise SignalRHandshakeError("SignalR handshake is not valid UTF-8") from err

    try:
        response = json.loads(encoded_response)
    except (json.JSONDecodeError, UnicodeError) as err:
        raise SignalRHandshakeError("SignalR handshake is not valid JSON") from err

    if not isinstance(response, dict):
        raise SignalRHandshakeError("SignalR handshake response must be an object")
    if response.get("error") is not None:
        raise SignalRHandshakeError("SignalR handshake rejected by server")
    if response:
        raise SignalRHandshakeError("SignalR handshake response has unknown fields")

    return remainder or None


def _sanitize_close_reason(value: Any) -> str:
    """Return a bounded Close reason with credential-shaped values redacted."""
    if not isinstance(value, str) or not value.strip():
        return "server closed the SignalR connection"
    reason = " ".join(value.split())
    reason = re.sub(
        r"(?i)\b(token|password|api[ _-]?key|authorization|bearer)\b\s*[:=]?\s*\S+",
        r"\1=[redacted]",
        reason,
    )
    return reason[:160]


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


def extract_categorical_payloads(data: bytes) -> list[dict[str, Any]]:
    """Return Ting 3.0.4 secondary power-quality stream records."""
    payloads: list[dict[str, Any]] = []
    for message in decode_hub_messages(data):
        if message[0] != MSG_TYPE_INVOCATION or len(message) < 5:
            continue
        if message[3] != "updateGraphMultiCategorical":
            continue
        arguments = message[4]
        if not isinstance(arguments, list) or not arguments:
            raise SignalRProtocolError("SignalR Invocation has no arguments")
        records = arguments[0]
        if not isinstance(records, list):
            raise SignalRProtocolError(
                "Categorical SignalR payload must be a collection"
            )
        for record in records:
            if not isinstance(record, dict):
                raise SignalRProtocolError(
                    "Categorical SignalR record must be an object"
                )
            payloads.append(record)
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


def extract_control_messages(data: bytes) -> tuple[int, list[CloseMessage]]:
    """Return validated Ping and Close messages from framed hub data."""
    ping_count = 0
    closes: list[CloseMessage] = []

    for message in decode_hub_messages(data):
        if message[0] == MSG_TYPE_PING:
            if len(message) != 1:
                raise SignalRProtocolError("SignalR Ping message has unexpected fields")
            ping_count += 1
        elif message[0] == MSG_TYPE_CLOSE:
            if len(message) > 3:
                raise SignalRProtocolError(
                    "SignalR Close message has unexpected fields"
                )
            error = message[1] if len(message) >= 2 else None
            allow_reconnect = message[2] if len(message) >= 3 else False
            if error is not None and not isinstance(error, str):
                raise SignalRProtocolError("SignalR Close reason must be a string")
            if not isinstance(allow_reconnect, bool):
                raise SignalRProtocolError(
                    "SignalR Close allow-reconnect flag must be a boolean"
                )
            closes.append(
                CloseMessage(
                    reason=_sanitize_close_reason(error),
                    allow_reconnect=allow_reconnect,
                )
            )

    return ping_count, closes


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
