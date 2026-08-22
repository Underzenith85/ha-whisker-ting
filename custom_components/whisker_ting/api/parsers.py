"""Validation boundary for untrusted Ting REST API responses."""

from __future__ import annotations

import logging
import math
from datetime import UTC, datetime
from typing import Any

from .models import (
    ConditionsSnapshot,
    DeviceConditions,
    DeviceState,
    FireHazardStatus,
    FrozenPipeRecord,
    HazardStatus,
    Site,
    TingEvent,
    UserData,
    VoltageHistoryPoint,
)

_LOGGER = logging.getLogger(__name__)


def mapping(value: Any) -> dict[str, Any]:
    """Return a mapping or an empty mapping for malformed values."""
    return value if isinstance(value, dict) else {}


def collection(value: Any) -> list[Any]:
    """Return an API list or an empty list for malformed values."""
    return value if isinstance(value, list) else []


def optional_string(value: Any) -> str | None:
    """Return a non-empty string or None."""
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def string(value: Any, default: str = "") -> str:
    """Return a non-empty string or a deterministic default."""
    return optional_string(value) or default


def integer(value: Any, default: int = 0) -> int:
    """Return an integer without accepting booleans or coercion."""
    return value if isinstance(value, int) and not isinstance(value, bool) else default


def optional_integer(value: Any) -> int | None:
    """Return an integer or None for malformed values."""
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def identifier(value: Any) -> int | None:
    """Return a positive integer identifier or None."""
    parsed = optional_integer(value)
    return parsed if parsed is not None and parsed > 0 else None


def optional_identifier_string(value: Any) -> str | None:
    """Return a string or integer identifier as a string."""
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    return optional_string(value)


def optional_number(value: Any) -> float | None:
    """Return a finite number without accepting booleans."""
    if (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    ):
        return float(value)
    return None


def boolean(value: Any) -> bool:
    """Return only an explicit API boolean as a boolean."""
    return value if isinstance(value, bool) else False


def optional_boolean(value: Any) -> bool | None:
    """Return an explicit API boolean or None."""
    return value if isinstance(value, bool) else None


def bounded_scalar_mapping(
    value: Any,
) -> dict[str, str | int | float | bool] | None:
    """Copy a bounded scalar-only API object."""
    if not isinstance(value, dict):
        return None
    result: dict[str, str | int | float | bool] = {}
    for key, item in value.items():
        if not isinstance(key, str) or len(key) > 64 or len(result) >= 32:
            continue
        if isinstance(item, bool):
            result[key] = item
        elif isinstance(item, (int, float)) and math.isfinite(item):
            result[key] = item
        elif isinstance(item, str) and len(item) <= 256:
            result[key] = item
    return result or None


def first_optional_string(data: dict[str, Any], *keys: str) -> str | None:
    """Return the first valid string stored under a supplied key."""
    return next(
        (
            value
            for key in keys
            if (value := optional_string(data.get(key))) is not None
        ),
        None,
    )


def parse_datetime(value: Any) -> datetime | None:
    """Parse an API timestamp and normalize it to UTC."""
    parsed_value = optional_string(value)
    if parsed_value is None:
        return None
    try:
        parsed = datetime.fromisoformat(parsed_value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=parsed.tzinfo or UTC).astimezone(UTC)


def parse_voltage_history(
    data: Any, *, limit: int = 10_000
) -> list[VoltageHistoryPoint]:
    """Parse a bounded v3 voltage-history response into UTC aggregates."""
    root = mapping(data)
    unit = first_optional_string(root, "unit", "unitOfMeasure", "unitOfMeasurement")
    if unit is not None and unit.lower() not in {"v", "volt", "volts"}:
        return []

    if isinstance(data, list):
        values = data
    else:
        values = next(
            (
                candidate
                for key in ("data", "items", "readings", "values")
                if isinstance((candidate := root.get(key)), list)
            ),
            [],
        )

    points: dict[datetime, VoltageHistoryPoint] = {}
    for value in values[:limit]:
        item = mapping(value)
        start = parse_datetime(
            first_optional_string(
                item, "timestampUtc", "startUtc", "timestamp", "start"
            )
        )
        minimum = next(
            (
                parsed
                for key in ("minimum", "min", "voltageMin")
                if (parsed := optional_number(item.get(key))) is not None
            ),
            None,
        )
        maximum = next(
            (
                parsed
                for key in ("maximum", "max", "voltageMax")
                if (parsed := optional_number(item.get(key))) is not None
            ),
            None,
        )
        average = next(
            (
                parsed
                for key in ("average", "avg", "mean", "voltageAverage")
                if (parsed := optional_number(item.get(key))) is not None
            ),
            None,
        )
        if (
            start is None
            or minimum is None
            or maximum is None
            or average is None
            or not minimum <= average <= maximum
        ):
            continue

        coverage = optional_number(item.get("coverage"))
        if coverage is None:
            samples = optional_number(item.get("sampleCount"))
            expected = optional_number(item.get("expectedSampleCount"))
            if samples is not None and expected is not None and expected > 0:
                coverage = samples / expected
        if coverage is not None:
            coverage = min(max(coverage, 0.0), 1.0)
        points[start] = VoltageHistoryPoint(start, minimum, maximum, average, coverage)
    return [points[start] for start in sorted(points)]


def parse_frozen_pipe_record(data: Any) -> FrozenPipeRecord | None:
    """Parse a frozen-pipe record and discard unknown fields."""
    if not isinstance(data, dict) or not data:
        return None
    record = FrozenPipeRecord(
        level=optional_integer(data.get("level")),
        outdoor_temperature_c=optional_number(data.get("outdoorTemperatureC")),
        detected_location_type=optional_string(data.get("detectedLocationType")),
        timestamp_utc=first_optional_string(
            data, "timestampUtc", "detectedTimestampUtc", "createdUtc"
        ),
        resolved_timestamp_utc=first_optional_string(
            data, "resolvedTimestampUtc", "resolvedUtc"
        ),
        user_action=optional_string(data.get("userAction")),
        notification_type=optional_string(data.get("notificationType")),
        notification_delivery_mode=optional_string(
            data.get("notificationDeliveryMode")
        ),
    )
    return record if any(value is not None for value in vars(record).values()) else None


def parse_frozen_pipe_history(data: Any) -> list[FrozenPipeRecord]:
    """Parse known single-record and collection history shapes."""
    if isinstance(data, list):
        values = data
    elif isinstance(data, dict):
        found = next(
            (
                data.get(key)
                for key in ("history", "items", "records", "data")
                if isinstance(data.get(key), list)
            ),
            None,
        )
        values = found if found is not None else [data]
    else:
        values = []
    return [
        record
        for value in values
        if (record := parse_frozen_pipe_record(value)) is not None
    ]


def parse_event_history(data: Any) -> list[TingEvent]:
    """Normalize, scope, and deterministically order notification history."""
    if not isinstance(data, list):
        return []
    events: list[TingEvent] = []
    seen: set[tuple[object, ...]] = set()
    for value in data:
        if not isinstance(value, dict):
            continue
        event_type = optional_string(value.get("eventType"))
        serial_number = optional_string(value.get("serialNumber"))
        site_id = identifier(value.get("siteId"))
        timestamp = parse_datetime(
            first_optional_string(
                value,
                "eventTimestampUtc",
                "sentTimestampUtc",
                "sentUtc",
                "eventTimestampLocal",
            )
        )
        if (
            event_type is None
            or (serial_number is None and site_id is None)
            or timestamp is None
        ):
            continue
        event_id = optional_identifier_string(value.get("id"))
        identity = (
            ("id", event_id)
            if event_id is not None
            else (
                "content",
                event_type,
                timestamp.isoformat(),
                serial_number,
                site_id,
            )
        )
        if identity in seen:
            continue
        seen.add(identity)
        events.append(
            TingEvent(
                event_type=event_type,
                timestamp_utc=timestamp.isoformat(),
                serial_number=serial_number,
                site_id=site_id,
                event_kind=classify_event_type(event_type),
                event_id=event_id,
                category=optional_string(value.get("eventCategory")),
                title=optional_string(value.get("title")),
                message=optional_string(value.get("message")),
            )
        )
    return sorted(
        events,
        key=lambda event: (event.timestamp_utc, event.event_id or ""),
        reverse=True,
    )


_EVENT_TYPE_KINDS = {
    "poweroutage": "power_outage",
    "outage": "power_outage",
    "powerrestored": "power_restored",
    "powerrestoration": "power_restored",
    "communityoutage": "power_outage",
    "communitypoweroutage": "power_outage",
    "communitypowerrestored": "power_restored",
    "generatoron": "generator_on",
    "generatoroff": "generator_off",
    "voltagesag": "voltage_sag",
    "voltageswell": "voltage_swell",
    "brownout": "voltage_sag",
    "surge": "voltage_swell",
    "nogrounding": "no_grounding",
    "noground": "no_grounding",
    "groundingrestored": "grounding_restored",
    "recurringpowerqualityproblem": "power_quality_problem",
    "powerqualityproblem": "power_quality_problem",
    "recurringpowerqualityproblemcleared": "power_quality_restored",
    "powerqualityproblemcleared": "power_quality_restored",
    "hightemperature": "high_temperature",
    "lowtemperature": "low_temperature",
    "fire": "fire_event",
    "fireevent": "fire_event",
    "utilityfire": "utility_fire_event",
    "utilityfireevent": "utility_fire_event",
    "deviceonline": "device_online",
    "deviceoffline": "device_offline",
}


def classify_event_type(event_type: str) -> str | None:
    """Map an explicit known Ting event type to an automation-safe kind."""
    normalized = "".join(
        character for character in event_type.lower() if character.isalnum()
    )
    return _EVENT_TYPE_KINDS.get(normalized)


def parse_user_data(data: Any) -> UserData:
    """Parse a complete user response into validated domain models."""
    root = mapping(data)
    devices: list[DeviceState] = []
    for index, device_data in enumerate(collection(root.get("devices"))):
        if not isinstance(device_data, dict):
            _LOGGER.warning("Skipping malformed device at index %d", index)
            continue
        serial_number = optional_string(device_data.get("serialNumber"))
        if serial_number is None:
            _LOGGER.warning(
                "Skipping device at index %d without a valid serial number", index
            )
            continue
        devices.append(parse_device(device_data, serial_number))

    sites: list[Site] = []
    for index, site_data in enumerate(collection(root.get("sites"))):
        if not isinstance(site_data, dict):
            _LOGGER.warning("Skipping malformed site at index %d", index)
            continue
        site_id = identifier(site_data.get("id"))
        if site_id is None:
            _LOGGER.warning("Skipping site at index %d without a valid ID", index)
            continue
        sites.append(
            Site(
                id=site_id,
                user_id=integer(site_data.get("userId")),
                display_name=string(site_data.get("displayName")),
                address_line1=optional_string(site_data.get("addressLine1")),
                city=optional_string(site_data.get("city")),
                state_province=optional_string(site_data.get("stateProvince")),
                postal_code=optional_string(site_data.get("postalCode")),
                country=optional_string(site_data.get("country")),
                latitude=optional_number(site_data.get("latitude")),
                longitude=optional_number(site_data.get("longitude")),
            )
        )

    return UserData(
        user_id=integer(root.get("id")),
        email=string(root.get("email")),
        first_name=string(root.get("firstName")),
        last_name=string(root.get("lastName")),
        phone_number=optional_string(root.get("phoneNumber")),
        devices=devices,
        sites=sites,
    )


def parse_device(data: dict[str, Any], serial_number: str | None = None) -> DeviceState:
    """Parse a device response into a validated domain model."""
    serial_number = serial_number or optional_string(data.get("serialNumber"))
    if serial_number is None:
        raise ValueError("Device has no valid serial number")

    hazard_data = mapping(data.get("fireHazardStatus"))
    efh_data = mapping(hazard_data.get("efhStatus"))
    ufh_data = mapping(hazard_data.get("ufhStatus"))
    hex_colors = mapping(hazard_data.get("hexColor"))
    fire_hazard_status = FireHazardStatus(
        learning_mode=boolean(hazard_data.get("learningMode")),
        hazard_severity_level=optional_integer(hazard_data.get("hazardSeverityLevel")),
        message=optional_string(hazard_data.get("message")),
        efh_status=HazardStatus(
            status=optional_string(efh_data.get("status")),
            timestamp_utc=optional_string(efh_data.get("timestampUtc")),
            level=optional_integer(efh_data.get("level")),
            message=optional_string(efh_data.get("message")),
            hex_color=string(efh_data.get("hexColor"), "#00FF00"),
        ),
        ufh_status=HazardStatus(
            status=optional_string(ufh_data.get("status")),
            timestamp_utc=optional_string(ufh_data.get("timestampUtc")),
            level=optional_integer(ufh_data.get("level")),
            message=optional_string(ufh_data.get("message")),
            hex_color=string(ufh_data.get("hexColor"), "#00FF00"),
        ),
        hex_color_light=string(hex_colors.get("light"), "#00FF00"),
        hex_color_medium=string(hex_colors.get("medium"), "#358C15"),
        hex_color_dark=string(hex_colors.get("dark"), "#233016"),
    )
    group_data = mapping(data.get("group"))
    return DeviceState(
        serial_number=serial_number,
        name=string(data.get("name"), serial_number),
        device_type=string(data.get("type"), "Unknown"),
        site_id=integer(data.get("siteId")),
        version=optional_string(data.get("version")),
        wifi_mac_address=optional_string(data.get("wifiMacAddress")),
        bluetooth_mac_address=optional_string(data.get("bluetoothMacAddress")),
        soc_serial_number=optional_string(data.get("socSerialNumber")),
        station_id=serial_number,
        is_fire=boolean(data.get("isFire")),
        is_hvac_verified=boolean(data.get("isHvacVerified")),
        has_frozen_pipe=boolean(data.get("hasFrozenPipe")),
        is_owner=boolean(data.get("isOwner")),
        is_online=optional_boolean(data.get("isOnline")),
        last_device_observation_utc=parse_datetime(
            first_optional_string(
                data, "lastSeenUtc", "lastObservedUtc", "lastUpdatedUtc"
            )
        ),
        fire_hazard_status=fire_hazard_status,
        group_name=optional_string(group_data.get("name")),
        group_id=optional_integer(group_data.get("id")),
    )


def parse_conditions(data: Any) -> ConditionsSnapshot | None:
    """Parse the current-conditions response without retaining raw JSON."""
    if not isinstance(data, dict):
        return None

    temperatures: dict[int, float] = {}
    for site_key, value in mapping(data.get("currentTemperatures")).items():
        try:
            site_id = int(site_key)
        except (TypeError, ValueError):
            continue
        if (temperature := optional_number(value)) is not None:
            temperatures[site_id] = temperature

    outage_risks: dict[
        int, str | int | float | dict[str, str | int | float | bool]
    ] = {}
    for site_key, value in mapping(data.get("currentOutageRisks")).items():
        try:
            site_id = int(site_key)
        except (TypeError, ValueError):
            continue
        risk = (
            value
            if isinstance(value, (str, int, float)) and not isinstance(value, bool)
            else bounded_scalar_mapping(value)
        )
        if risk is not None:
            outage_risks[site_id] = risk

    devices: list[DeviceConditions] = []
    for value in collection(data.get("devices")):
        if not isinstance(value, dict):
            continue
        serial_number = optional_string(value.get("serialNumber"))
        if serial_number is None:
            continue
        hazard_status = None
        if isinstance(value.get("fireHazardStatus"), dict):
            hazard_status = parse_device(value, serial_number).fire_hazard_status
        devices.append(
            DeviceConditions(
                serial_number=serial_number,
                is_fire=boolean(value.get("isFire")) if "isFire" in value else None,
                is_hvac_verified=(
                    boolean(value.get("isHvacVerified"))
                    if "isHvacVerified" in value
                    else None
                ),
                has_frozen_pipe=(
                    boolean(value.get("hasFrozenPipe"))
                    if "hasFrozenPipe" in value
                    else None
                ),
                fire_hazard_status=hazard_status,
            )
        )
    return ConditionsSnapshot(temperatures, outage_risks, devices)
