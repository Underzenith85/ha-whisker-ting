"""Tests for SignalR MessagePack encoding helpers."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import msgpack
import pytest

SIGNALR_PATH = (
    Path(__file__).parents[1] / "custom_components" / "whisker_ting" / "signalr.py"
)


def _load_signalr_module() -> ModuleType:
    """Load the helper without importing Home Assistant integration modules."""
    spec = importlib.util.spec_from_file_location("whisker_ting_signalr", SIGNALR_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


signalr = _load_signalr_module()


def _split_frame(frame: bytes) -> tuple[int, bytes]:
    """Decode the test frame's VarInt length and return its payload."""
    length = 0
    shift = 0
    for index, byte in enumerate(frame):
        length |= (byte & 0x7F) << shift
        if byte & 0x80 == 0:
            return length, frame[index + 1 :]
        shift += 7
    raise AssertionError("unterminated VarInt")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, b"\x00"),
        (127, b"\x7f"),
        (128, b"\x80\x01"),
        (16_383, b"\xff\x7f"),
        (16_384, b"\x80\x80\x01"),
        (signalr.MAX_VARINT_VALUE, b"\xff\xff\xff\xff\x07"),
    ],
)
def test_encode_varint(value: int, expected: bytes) -> None:
    """SignalR frame lengths use the specified little-endian VarInt format."""
    assert signalr.encode_varint(value) == expected


@pytest.mark.parametrize("value", [-1, signalr.MAX_VARINT_VALUE + 1])
def test_encode_varint_rejects_out_of_range_values(value: int) -> None:
    """SignalR permits frame lengths from zero through 0x7fffffff."""
    with pytest.raises(ValueError):
        signalr.encode_varint(value)


def test_encode_invocation() -> None:
    """Invocation is a framed MessagePack array matching the Ting app call."""
    frame = signalr.encode_invocation(
        "1",
        "InitializeStreaming",
        [
            {"StationId": "ABC123", "DataElement": "ComboBinaryData"},
            "api-key",
            "42",
        ],
    )

    length, payload = _split_frame(frame)
    assert length == len(payload)
    assert msgpack.unpackb(payload, raw=False) == [
        1,
        {},
        "1",
        "InitializeStreaming",
        [
            {"StationId": "ABC123", "DataElement": "ComboBinaryData"},
            "api-key",
            "42",
        ],
    ]


def test_encode_ping() -> None:
    """Ping is a length-prefixed single-element MessagePack array."""
    frame = signalr.encode_ping()

    length, payload = _split_frame(frame)
    assert length == len(payload)
    assert msgpack.unpackb(payload, raw=False) == [6]


def test_frame_message_uses_multibyte_length() -> None:
    """Payloads longer than 127 bytes receive a multi-byte prefix."""
    payload = b"x" * 128
    frame = signalr.frame_message(payload)

    assert frame[:2] == b"\x80\x01"
    assert frame[2:] == payload


def _voltage_invocation(**overrides: object) -> list[object]:
    """Build a Ting voltage Invocation message."""
    payload = {
        "DataTimeUtc": "2026-08-21T20:30:00Z",
        "Voltage": 121.25,
        "VoltageHi": 122.5,
        "VoltageLo": 119.75,
        "AveragePeaksMax": 4.5,
    }
    payload.update(overrides)
    return [1, {}, None, "updateComboBinaryData", [payload]]


def test_decode_hub_messages_handles_coalesced_frames() -> None:
    """One WebSocket payload may contain more than one SignalR frame."""
    ping = signalr.encode_hub_message([6])
    invocation = signalr.encode_hub_message(_voltage_invocation())

    assert signalr.decode_hub_messages(ping + invocation) == [
        [6],
        _voltage_invocation(),
    ]


@pytest.mark.parametrize(
    "data",
    [
        b"\x80",
        b"\x05\x91\x06",
        b"\x01\xc1",
        b"\x01\x80",
        b"\xff\xff\xff\xff\x08",
    ],
)
def test_decode_hub_messages_rejects_malformed_frames(data: bytes) -> None:
    """Malformed and truncated frames fail without leaking decoder errors."""
    with pytest.raises(signalr.SignalRProtocolError):
        signalr.decode_hub_messages(data)


def test_extract_invocation_payload_uses_named_fields() -> None:
    """Payload objects retain their field names regardless of map ordering."""
    message = _voltage_invocation(
        AveragePeaksMax=5.25,
        VoltageLo=118.0,
        Voltage=120.5,
        VoltageHi=123.0,
    )
    frame = signalr.encode_hub_message(message)

    assert signalr.extract_invocation_payloads(frame, "updateComboBinaryData") == [
        message[4][0]
    ]


def test_extract_invocation_payload_ignores_unknown_messages() -> None:
    """Unrelated message types and Invocation targets are ignored."""
    frames = b"".join(
        [
            signalr.encode_hub_message([6]),
            signalr.encode_hub_message([1, {}, None, "otherTarget", [{}]]),
        ]
    )

    assert signalr.extract_invocation_payloads(frames, "updateComboBinaryData") == []


def test_extract_invocation_payload_decodes_nested_messagepack() -> None:
    """Binary invocation arguments are decoded as nested MessagePack objects."""
    payload = _voltage_invocation()[4][0]
    nested_payload = msgpack.packb(payload, use_bin_type=True)
    frame = signalr.encode_hub_message(
        [1, {}, None, "updateComboBinaryData", [nested_payload]]
    )

    assert signalr.extract_invocation_payloads(frame, "updateComboBinaryData") == [
        payload
    ]
