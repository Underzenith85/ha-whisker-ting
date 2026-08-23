"""Pure state interpretation shared by Whisker Ting entity platforms."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .api import DeviceState, TingEvent


def event_attributes(events: list[TingEvent]) -> dict[str, Any] | None:
    """Return normalized attributes for the newest station event."""
    if not events:
        return None
    event = events[0]
    return {
        "event_id": event.event_id,
        "category": event.category,
        "timestamp": event.timestamp_utc,
        "title": event.title,
        "message": event.message,
        "history_count": len(events),
    }


def event_timestamp(events: list[TingEvent], event_kind: str) -> datetime | None:
    """Return the newest valid timestamp for an explicitly classified event."""
    timestamps = [
        timestamp
        for event in events
        if event.event_kind == event_kind
        and (timestamp := _parse_event_timestamp(event)) is not None
    ]
    return max(timestamps, default=None)


def latest_voltage_condition(events: list[TingEvent]) -> str | None:
    """Return sag or swell from the newest explicitly classified voltage event."""
    values = {"voltage_sag": "sag", "voltage_swell": "swell"}
    matches = [
        (timestamp, values[event.event_kind])
        for event in events
        if event.event_kind in values
        and (timestamp := _parse_event_timestamp(event)) is not None
    ]
    return max(matches, key=lambda item: item[0])[1] if matches else None


def event_condition(
    events: list[TingEvent], on_kinds: frozenset[str], off_kinds: frozenset[str]
) -> bool | None:
    """Return state from the newest valid explicit transition."""
    matches = [
        (timestamp, event.event_kind in on_kinds)
        for event in events
        if event.event_kind in on_kinds | off_kinds
        and (timestamp := _parse_event_timestamp(event)) is not None
    ]
    return max(matches, key=lambda item: item[0])[1] if matches else None


def device_event_condition(
    state: DeviceState,
    key: str,
    on_kinds: frozenset[str],
    off_kinds: frozenset[str],
) -> bool | None:
    """Prefer an explicit REST connectivity value, then event transitions."""
    if key == "device_online" and state.is_online is not None:
        return state.is_online
    return event_condition(state.events, on_kinds, off_kinds)


def outage_risk_state(
    risk: str | int | float | dict[str, str | int | float | bool] | None,
) -> str | int | float | None:
    """Return a stable state from Ting's opaque per-site outage-risk value."""
    if isinstance(risk, (str, int, float)) and not isinstance(risk, bool):
        return risk
    if isinstance(risk, dict):
        for key in ("status", "risk", "level"):
            value = risk.get(key)
            if isinstance(value, (str, int, float)) and not isinstance(value, bool):
                return value
    return None


def outage_risk_attributes(
    risk: str | int | float | dict[str, str | int | float | bool] | None,
) -> dict[str, Any] | None:
    """Expose the documented-shape outage-risk fields as attributes."""
    if not isinstance(risk, dict):
        return None
    return {
        key: value
        for key, value in risk.items()
        if isinstance(key, str)
        and isinstance(value, (str, int, float, bool))
        and len(key) <= 64
    }


def frozen_pipe_last_event(state: DeviceState) -> datetime | None:
    """Return the newest modeled frozen-pipe timestamp."""
    records = ([state.frozen_pipe.status] if state.frozen_pipe.status else []) + list(
        state.frozen_pipe.history
    )
    timestamps: list[datetime] = []
    for record in records:
        if record.timestamp_utc is None:
            continue
        try:
            timestamps.append(
                datetime.fromisoformat(record.timestamp_utc.replace("Z", "+00:00"))
            )
        except ValueError:
            continue
    return max(timestamps, default=None)


def realtime_sample_age(state: DeviceState) -> float | None:
    """Return non-negative seconds since the newest valid real-time sample."""
    timestamp = state.last_realtime_sample_utc
    if timestamp is None:
        return None
    return max((datetime.now(timestamp.tzinfo) - timestamp).total_seconds(), 0)


def hazard_status(state: DeviceState) -> str:
    """Return the modeled overall hazard status."""
    if state.fire_hazard_status.learning_mode:
        return "learning"
    if state.is_fire:
        return "fire_hazard"
    efh = state.fire_hazard_status.efh_status
    ufh = state.fire_hazard_status.ufh_status
    if efh.status in {"PossibleFire", "HazardFound"}:
        return "fire_hazard"
    if ufh.status == "PowerQualityHazard":
        return "power_quality_hazard"
    if efh.status == "ElevatedSuspicious":
        return "elevated_suspicious"
    if efh.status == "ReviewedNotFire":
        return "reviewed_not_fire"
    known_normal = {None, "", "None", "NoHazard", "NoHazards", "Normal"}
    if efh.status in known_normal and ufh.status in known_normal:
        return "no_hazards"
    return "unknown"


def hazard_attributes(state: DeviceState) -> dict[str, Any]:
    """Return modeled status details without retaining the raw response."""
    hazard = state.fire_hazard_status
    return {
        "severity_level": hazard.hazard_severity_level,
        "efh_status": hazard.efh_status.status,
        "efh_level": hazard.efh_status.level,
        "efh_timestamp": hazard.efh_status.timestamp_utc,
        "ufh_status": hazard.ufh_status.status,
        "ufh_level": hazard.ufh_status.level,
        "ufh_timestamp": hazard.ufh_status.timestamp_utc,
    }


def _parse_event_timestamp(event: TingEvent) -> datetime | None:
    """Parse one event timestamp without letting malformed history escape."""
    try:
        return datetime.fromisoformat(event.timestamp_utc.replace("Z", "+00:00"))
    except ValueError:
        return None
