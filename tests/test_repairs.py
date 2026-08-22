"""Home Assistant Repairs tests for sustained Whisker Ting failures."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from homeassistant.core import HomeAssistant
from homeassistant.helpers import issue_registry as ir

from custom_components.whisker_ting import async_remove_entry
from custom_components.whisker_ting.api import DeviceState
from custom_components.whisker_ting.const import DOMAIN
from custom_components.whisker_ting.repairs import (
    SUSTAINED_UPDATE_THRESHOLD,
    WhiskerRepairManager,
)


def _issues(hass: HomeAssistant, translation_key: str) -> list[ir.IssueEntry]:
    """Return active integration issues with one translation key."""
    return [
        issue
        for (domain, _), issue in ir.async_get(hass).issues.items()
        if domain == DOMAIN and issue.translation_key == translation_key
    ]


def test_sustained_device_repair_is_deduplicated_and_clears(
    hass: HomeAssistant,
) -> None:
    """Transient failures stay quiet; sustained failure creates one repair."""
    manager = WhiskerRepairManager(hass, "entry-1", "Home")
    device = DeviceState(
        "SERIAL-001",
        "Utility room",
        "FireSensor",
        100,
        is_online=False,
        stream_health="receiving",
        last_realtime_sample_utc=datetime.now(UTC),
    )

    for _ in range(SUSTAINED_UPDATE_THRESHOLD - 1):
        manager.evaluate([device], [])
    assert not _issues(hass, "device_offline")

    manager.evaluate([device], [])
    issue = _issues(hass, "device_offline")
    assert len(issue) == 1
    assert issue[0].translation_placeholders == {"device_name": "Utility room"}
    created = issue[0].created
    manager.evaluate([device], [])
    assert _issues(hass, "device_offline")[0].created == created

    device.is_online = True
    manager.evaluate([device], [])
    assert not _issues(hass, "device_offline")


def test_repairs_are_independently_scoped_by_device_and_condition(
    hass: HomeAssistant,
) -> None:
    """Recovery for one device cannot clear another device's stream repair."""
    manager = WhiskerRepairManager(hass, "entry-2", "Home")
    first = DeviceState(
        "SERIAL-001",
        "First",
        "FireSensor",
        100,
        is_online=True,
        stream_health="not_receiving",
        last_realtime_sample_utc=datetime.now(UTC),
    )
    second = DeviceState(
        "SERIAL-002",
        "Second",
        "FireSensor",
        100,
        is_online=True,
        stream_health="not_receiving",
        last_realtime_sample_utc=datetime.now(UTC),
    )
    for _ in range(SUSTAINED_UPDATE_THRESHOLD):
        manager.evaluate([first, second], [])
    assert len(_issues(hass, "stream_unavailable")) == 2

    first.stream_health = "receiving"
    manager.evaluate([first, second], [])
    remaining = _issues(hass, "stream_unavailable")
    assert len(remaining) == 1
    assert remaining[0].translation_placeholders == {"device_name": "Second"}


def test_authentication_and_optional_capability_repairs_clear(
    hass: HomeAssistant,
) -> None:
    """Auth and explicit authorization repairs use stable IDs and auto-clear."""
    manager = WhiskerRepairManager(hass, "entry-3", "Home account")
    manager.create_authentication_issue()
    manager.create_authentication_issue()
    assert len(_issues(hass, "reauthentication_required")) == 1
    manager.clear_authentication_issue()
    assert not _issues(hass, "reauthentication_required")

    manager.evaluate([], ["event_history"])
    issue = _issues(hass, "optional_capability_unauthorized")
    assert len(issue) == 1
    assert issue[0].translation_placeholders["capability"] == "event history"
    manager.evaluate([], [])
    assert not _issues(hass, "optional_capability_unauthorized")


async def test_removing_entry_clears_all_owned_repairs(hass: HomeAssistant) -> None:
    """Deleting a config entry removes its persistent Repair records."""
    manager = WhiskerRepairManager(hass, "entry-4", "Home")
    manager.create_authentication_issue()
    entry = MagicMock(entry_id="entry-4", title="Home")

    await async_remove_entry(hass, entry)

    assert not [
        issue
        for (domain, issue_id), issue in ir.async_get(hass).issues.items()
        if domain == DOMAIN and issue_id.startswith("entry-4_")
    ]
