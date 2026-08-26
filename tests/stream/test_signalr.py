"""Tests for SignalR MessagePack encoding helpers."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import msgpack
import pytest

SIGNALR_PATH = (
    Path(__file__).parents[2]
    / "custom_components"
    / "whisker_ting"
    / "stream"
    / "signalr.py"
)


def _load_signalr_module() -> ModuleType:
    """Load the helper without importing Home Assistant integration modules."""
    spec = importlib.util.spec_from_file_location("whisker_ting_signalr", SIGNALR_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
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


def test_extract_void_completion() -> None:
    """A void Completion resolves its invocation without a result."""
    frame = signalr.encode_hub_message([3, {}, "7", 2])

    assert signalr.extract_completions(frame) == [
        signalr.CompletionMessage(invocation_id="7")
    ]


def test_extract_error_and_result_completions() -> None:
    """Error and result Completion payloads retain their values."""
    frames = b"".join(
        [
            signalr.encode_hub_message([3, {}, "8", 1, "not authorized"]),
            signalr.encode_hub_message([3, {}, "9", 3, {"ready": True}]),
        ]
    )

    assert signalr.extract_completions(frames) == [
        signalr.CompletionMessage(invocation_id="8", error="not authorized"),
        signalr.CompletionMessage(invocation_id="9", result={"ready": True}),
    ]


@pytest.mark.parametrize(
    "message",
    [
        [3, {}, "1"],
        [3, {}, None, 2],
        [3, {}, "1", 1],
        [3, {}, "1", 3],
        [3, {}, "1", 99],
    ],
)
def test_extract_completion_rejects_malformed_messages(
    message: list[object],
) -> None:
    """Malformed Completion messages raise a protocol error."""
    with pytest.raises(signalr.SignalRProtocolError):
        signalr.extract_completions(signalr.encode_hub_message(message))


def test_decode_successful_handshake() -> None:
    """A successful handshake is an empty JSON object terminated by RS."""
    assert signalr.decode_handshake_response("{}\x1e") is None


def test_decode_binary_handshake_and_return_coalesced_payload() -> None:
    """MessagePack handshakes may be binary and include the first hub frame."""
    payload = signalr.encode_hub_message([signalr.MSG_TYPE_PING])
    assert signalr.decode_handshake_response(b"{}\x1e" + payload) == payload


def test_extract_categorical_power_quality_payloads() -> None:
    """Ting secondary-stream arrays are extracted from their hub invocation."""
    records = [
        {"Category": "frequency", "ObsTime": "2026-08-22T00:00:00Z", "Value": "60.01"},
        {"Category": "thdAvg", "ObsTime": "2026-08-22T00:00:00Z", "Value": "0.024"},
    ]
    frame = signalr.encode_hub_message(
        [1, {}, None, "updateGraphMultiCategorical", [records]]
    )

    assert signalr.extract_categorical_payloads(frame) == records


@pytest.mark.parametrize(
    "response",
    [
        "{}",
        "not-json\x1e",
        "[]\x1e",
        '{"unexpected":true}\x1e',
    ],
)
def test_decode_handshake_rejects_malformed_responses(response: str) -> None:
    """Malformed, unterminated, and unexpected handshakes fail structurally."""
    with pytest.raises(signalr.SignalRHandshakeError):
        signalr.decode_handshake_response(response)


def test_decode_handshake_rejects_server_error_without_exposing_it() -> None:
    """Handshake errors fail without retaining the server-provided value."""
    secret = "authorization=secret-value"
    with pytest.raises(signalr.SignalRHandshakeError) as raised:
        signalr.decode_handshake_response(f'{{"error":"{secret}"}}\x1e')
    assert secret not in str(raised.value)


def test_additional_protocol_shape_failures() -> None:
    """Handshake and invocation helpers reject remaining malformed shapes."""
    with pytest.raises(signalr.SignalRHandshakeError, match="invalid type"):
        signalr.decode_handshake_response(object())
    with pytest.raises(signalr.SignalRHandshakeError, match="UTF-8"):
        signalr.decode_handshake_response(b"\xff\x1e")
    with pytest.raises(signalr.SignalRProtocolError, match="cannot be empty"):
        signalr.decode_hub_messages(b"\x00")
    short = signalr.encode_hub_message([1])
    with pytest.raises(signalr.SignalRProtocolError, match="too short"):
        signalr.extract_invocation_payloads(short, "target")
    no_arguments = signalr.encode_hub_message([1, {}, None, "target", []])
    with pytest.raises(signalr.SignalRProtocolError, match="no arguments"):
        signalr.extract_invocation_payloads(no_arguments, "target")
    categorical = signalr.encode_hub_message(
        [1, {}, None, "updateGraphMultiCategorical", [["invalid"]]]
    )
    with pytest.raises(signalr.SignalRProtocolError, match="must be an object"):
        signalr.extract_categorical_payloads(categorical)


def test_extract_ping_structurally() -> None:
    """Ping recognition uses the decoded hub type, not raw byte equality."""
    ping = signalr.encode_hub_message([signalr.MSG_TYPE_PING])
    ping_count, closes = signalr.extract_control_messages(ping + ping)
    assert ping_count == 2
    assert closes == []


def test_extract_close_sanitizes_reason_and_reconnect_flag() -> None:
    """Close parsing retains reconnect intent while redacting credentials."""
    frame = signalr.encode_hub_message(
        [signalr.MSG_TYPE_CLOSE, "authorization: secret-value", True]
    )
    ping_count, closes = signalr.extract_control_messages(frame)

    assert ping_count == 0
    assert closes == [
        signalr.CloseMessage(reason="authorization=[redacted]", allow_reconnect=True)
    ]


@pytest.mark.parametrize(
    "message",
    [
        [6, "unexpected"],
        [7, {"bad": "reason"}],
        [7, None, "yes"],
        [7, None, True, "extra"],
    ],
)
def test_extract_control_rejects_malformed_messages(
    message: list[object],
) -> None:
    """Malformed Ping and Close messages are protocol failures."""
    with pytest.raises(signalr.SignalRProtocolError):
        signalr.extract_control_messages(signalr.encode_hub_message(message))
