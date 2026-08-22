"""Sustained, automatically clearing Home Assistant Repairs."""

from __future__ import annotations

import hashlib
from collections import defaultdict
from collections.abc import Iterable

from homeassistant.core import HomeAssistant
from homeassistant.helpers import issue_registry as ir

from .api import DeviceState
from .const import DOMAIN

SUSTAINED_UPDATE_THRESHOLD = 3

CAPABILITY_NAMES = {
    "conditions": "current conditions",
    "event_history": "event history",
    "frozen_pipe": "frozen-pipe details",
}


class WhiskerRepairManager:
    """Create deduplicated Repairs from sustained integration diagnostics."""

    def __init__(self, hass: HomeAssistant, entry_id: str, entry_title: str) -> None:
        """Initialize repair state for one config entry lifecycle."""
        self._hass = hass
        self._entry_id = entry_id
        self._entry_title = entry_title
        self._counts: defaultdict[str, int] = defaultdict(int)

    def create_authentication_issue(self) -> None:
        """Direct the user to the reauthentication flow HA starts automatically."""
        ir.async_create_issue(
            self._hass,
            DOMAIN,
            self._issue_id("reauthentication_required"),
            is_fixable=False,
            is_persistent=True,
            severity=ir.IssueSeverity.ERROR,
            translation_key="reauthentication_required",
            translation_placeholders={"entry_title": self._entry_title},
        )

    def clear_authentication_issue(self) -> None:
        """Clear the authentication Repair after a successful authenticated update."""
        ir.async_delete_issue(
            self._hass, DOMAIN, self._issue_id("reauthentication_required")
        )

    def evaluate(
        self,
        devices: Iterable[DeviceState],
        unauthorized_capabilities: Iterable[str],
    ) -> None:
        """Evaluate one successful REST update and reconcile Repairs."""
        active_ids: set[str] = set()
        for device in devices:
            device_key = hashlib.sha256(device.serial_number.encode()).hexdigest()[:12]
            conditions = (
                (
                    "stream_unavailable",
                    device.stream_health == "not_receiving",
                    ir.IssueSeverity.WARNING,
                ),
                (
                    "device_offline",
                    device.is_online is False,
                    ir.IssueSeverity.WARNING,
                ),
                (
                    "live_samples_missing",
                    device.last_realtime_sample_utc is None
                    and device.stream_health in {"delayed", "not_receiving"},
                    ir.IssueSeverity.WARNING,
                ),
            )
            for repair_key, condition, severity in conditions:
                issue_id = self._issue_id(f"{repair_key}_{device_key}")
                if condition:
                    active_ids.add(issue_id)
                self._set_sustained_issue(
                    issue_id,
                    condition,
                    repair_key,
                    severity,
                    {"device_name": device.name},
                )

        for capability in unauthorized_capabilities:
            if capability not in CAPABILITY_NAMES:
                continue
            issue_id = self._issue_id(f"capability_{capability}")
            active_ids.add(issue_id)
            ir.async_create_issue(
                self._hass,
                DOMAIN,
                issue_id,
                is_fixable=False,
                is_persistent=True,
                severity=ir.IssueSeverity.WARNING,
                translation_key="optional_capability_unauthorized",
                translation_placeholders={
                    "capability": CAPABILITY_NAMES[capability],
                    "entry_title": self._entry_title,
                },
            )

        registry = ir.async_get(self._hass)
        prefix = f"{self._entry_id}_"
        for domain, issue_id in tuple(registry.issues):
            if (
                domain == DOMAIN
                and issue_id.startswith(prefix)
                and issue_id != self._issue_id("reauthentication_required")
                and issue_id not in active_ids
            ):
                ir.async_delete_issue(self._hass, DOMAIN, issue_id)

    def clear_all(self) -> None:
        """Remove entry-scoped Repairs when the config entry is removed."""
        registry = ir.async_get(self._hass)
        prefix = f"{self._entry_id}_"
        for domain, issue_id in tuple(registry.issues):
            if domain == DOMAIN and issue_id.startswith(prefix):
                ir.async_delete_issue(self._hass, DOMAIN, issue_id)

    def _set_sustained_issue(
        self,
        issue_id: str,
        condition: bool,
        translation_key: str,
        severity: ir.IssueSeverity,
        placeholders: dict[str, str],
    ) -> None:
        """Create after consecutive failures and clear immediately on recovery."""
        if not condition:
            self._counts.pop(issue_id, None)
            ir.async_delete_issue(self._hass, DOMAIN, issue_id)
            return
        self._counts[issue_id] += 1
        if self._counts[issue_id] < SUSTAINED_UPDATE_THRESHOLD:
            return
        ir.async_create_issue(
            self._hass,
            DOMAIN,
            issue_id,
            is_fixable=False,
            is_persistent=True,
            severity=severity,
            translation_key=translation_key,
            translation_placeholders=placeholders,
        )

    def _issue_id(self, suffix: str) -> str:
        """Return a stable ID scoped to one config entry."""
        return f"{self._entry_id}_{suffix}"
