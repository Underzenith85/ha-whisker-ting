"""Tests for SignalR MessagePack encoding helpers."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import msgpack
import pytest

SIGNALR_PATH = (
    Path(__file__).parents[1]
    / "custom_components"
    / "whisker_ting"
    / "signalr.py"
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
