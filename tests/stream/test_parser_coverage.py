"""Coverage for Ting-specific stream validation boundaries."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

from custom_components.whisker_ting.stream.models import PowerQualityCategory
from custom_components.whisker_ting.stream.parser import (
    decode_power_quality_data,
    decode_voltage_data,
    parse_timestamp,
)
from custom_components.whisker_ting.stream.signalr import SignalRProtocolError


def test_parse_timestamp_accepts_datetime_iso_and_falls_back() -> None:
    """Timestamp parsing normalizes naive values and safely handles junk."""
    aware = datetime(2026, 8, 22, tzinfo=UTC)
    assert parse_timestamp(aware) is aware
    assert parse_timestamp("2026-08-22T01:02:03Z") == datetime(
        2026, 8, 22, 1, 2, 3, tzinfo=UTC
    )
    assert parse_timestamp("2026-08-22T01:02:03").tzinfo is UTC
    assert parse_timestamp("bad").tzinfo is UTC
    assert parse_timestamp(None).tzinfo is UTC


def test_voltage_decoder_validates_every_payload_boundary() -> None:
    """Valid voltage is modeled while malformed and anomalous values are skipped."""
    payloads = [
        {
            "Voltage": "120.5",
            "VoltageHi": 121,
            "VoltageLo": 119,
            "AveragePeaksMax": 4,
            "DataTimeUtc": "2026-08-22T00:00:00Z",
        },
        {"Voltage": 0, "VoltageHi": 1, "VoltageLo": 0, "AveragePeaksMax": 1},
        {"Voltage": float("inf"), "VoltageHi": 1, "VoltageLo": 1, "AveragePeaksMax": 1},
        {"Voltage": "bad"},
        {},
    ]
    with patch(
        "custom_components.whisker_ting.stream.parser.extract_invocation_payloads",
        return_value=payloads,
    ):
        readings = decode_voltage_data(b"frame")
    assert len(readings) == 1
    assert readings[0].voltage == 120.5

    with patch(
        "custom_components.whisker_ting.stream.parser.extract_invocation_payloads",
        side_effect=SignalRProtocolError("bad frame"),
    ):
        assert decode_voltage_data(b"bad") == []


def test_power_quality_decoder_validates_categories_and_values() -> None:
    """Only finite readings from recognized categories are retained."""
    payloads = [
        {"Category": "frequency", "Value": "60.1", "ObsTime": "2026-08-22T00:00:00Z"},
        {"Category": "FutureMetric", "Value": 1},
        {"Category": "thdAvg", "Value": float("nan")},
        {"Category": "thdMax", "Value": "bad"},
        {},
    ]
    with patch(
        "custom_components.whisker_ting.stream.parser.extract_categorical_payloads",
        return_value=payloads,
    ):
        readings = decode_power_quality_data(b"frame")
    assert [(item.category, item.value) for item in readings] == [
        (PowerQualityCategory.FREQUENCY, 60.1)
    ]

    with patch(
        "custom_components.whisker_ting.stream.parser.extract_categorical_payloads",
        side_effect=SignalRProtocolError("bad frame"),
    ):
        assert decode_power_quality_data(b"bad") == []
