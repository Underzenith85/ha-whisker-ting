"""Binary sensor platform for Whisker Ting."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
    BinarySensorEntityDescription,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .api import DeviceState, Site, TingEvent
from .coordinator import WhiskerDataUpdateCoordinator
from .entity import WhiskerEntity, WhiskerSiteEntity

PARALLEL_UPDATES = 0  # Coordinator handles all updates


@dataclass(frozen=True, kw_only=True)
class WhiskerBinarySensorEntityDescription(BinarySensorEntityDescription):
    """Describes a Whisker Ting binary sensor entity."""

    value_fn: Callable[[DeviceState], bool | None]


@dataclass(frozen=True, kw_only=True)
class WhiskerSiteBinarySensorEntityDescription(BinarySensorEntityDescription):
    """Describe a Whisker Ting site binary sensor entity."""

    value_fn: Callable[[Site], bool | None]


EVENT_CONDITION_DESCRIPTIONS: tuple[
    tuple[str, BinarySensorDeviceClass, frozenset[str], frozenset[str]], ...
] = (
    (
        "power_outage",
        BinarySensorDeviceClass.PROBLEM,
        frozenset({"power_outage"}),
        frozenset({"power_restored"}),
    ),
    (
        "generator_running",
        BinarySensorDeviceClass.RUNNING,
        frozenset({"generator_on"}),
        frozenset({"generator_off"}),
    ),
    (
        "recurring_power_quality_problem",
        BinarySensorDeviceClass.PROBLEM,
        frozenset({"power_quality_problem"}),
        frozenset({"power_quality_restored"}),
    ),
    (
        "no_grounding",
        BinarySensorDeviceClass.PROBLEM,
        frozenset({"no_grounding"}),
        frozenset({"grounding_restored"}),
    ),
    (
        "device_online",
        BinarySensorDeviceClass.CONNECTIVITY,
        frozenset({"device_online"}),
        frozenset({"device_offline"}),
    ),
)


def _device_event_condition_value(
    key: str, on: frozenset[str], off: frozenset[str]
) -> Callable[[DeviceState], bool | None]:
    """Build a typed device event-condition accessor."""
    return lambda state: _device_event_condition(state, key, on, off)


def _site_event_condition_value(
    on: frozenset[str], off: frozenset[str]
) -> Callable[[Site], bool | None]:
    """Build a typed site event-condition accessor."""
    return lambda site: _event_condition(site.events, on, off)


BINARY_SENSOR_DESCRIPTIONS: tuple[WhiskerBinarySensorEntityDescription, ...] = (
    # Primary hazard sensors (enabled by default)
    WhiskerBinarySensorEntityDescription(
        key="fire_hazard",
        translation_key="fire_hazard",
        device_class=BinarySensorDeviceClass.SAFETY,
        value_fn=lambda state: state.is_fire,
    ),
    WhiskerBinarySensorEntityDescription(
        key="electrical_fire_hazard",
        translation_key="electrical_fire_hazard",
        device_class=BinarySensorDeviceClass.SAFETY,
        value_fn=lambda state: (
            state.fire_hazard_status.efh_status.status
            in {"ElevatedSuspicious", "PossibleFire", "HazardFound"}
        ),
    ),
    WhiskerBinarySensorEntityDescription(
        key="unverified_fire_hazard",
        translation_key="unverified_fire_hazard",
        device_class=BinarySensorDeviceClass.SAFETY,
        value_fn=lambda state: (
            state.fire_hazard_status.ufh_status.status == "PowerQualityHazard"
        ),
    ),
    WhiskerBinarySensorEntityDescription(
        key="frozen_pipe",
        translation_key="frozen_pipe",
        device_class=BinarySensorDeviceClass.COLD,
        value_fn=lambda state: (
            state.frozen_pipe.status.level > 0
            if state.frozen_pipe.status and state.frozen_pipe.status.level is not None
            else state.has_frozen_pipe
        ),
    ),
    WhiskerBinarySensorEntityDescription(
        key="learning_mode",
        translation_key="learning_mode",
        device_class=BinarySensorDeviceClass.RUNNING,
        value_fn=lambda state: state.fire_hazard_status.learning_mode,
    ),
    # Diagnostic sensors (disabled by default)
    WhiskerBinarySensorEntityDescription(
        key="hvac_verified",
        translation_key="hvac_verified",
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda state: state.is_hvac_verified,
    ),
    WhiskerBinarySensorEntityDescription(
        key="is_owner",
        translation_key="is_owner",
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda state: state.is_owner,
    ),
    *(
        WhiskerBinarySensorEntityDescription(
            key=key,
            translation_key=key,
            device_class=device_class,
            value_fn=_device_event_condition_value(key, on, off),
        )
        for key, device_class, on, off in EVENT_CONDITION_DESCRIPTIONS
    ),
)


SITE_BINARY_SENSOR_DESCRIPTIONS: tuple[
    WhiskerSiteBinarySensorEntityDescription, ...
] = tuple(
    WhiskerSiteBinarySensorEntityDescription(
        key=key,
        translation_key=key,
        device_class=device_class,
        value_fn=_site_event_condition_value(on, off),
    )
    for key, device_class, on, off in EVENT_CONDITION_DESCRIPTIONS
)


def _event_condition(
    events: list[TingEvent], on_kinds: frozenset[str], off_kinds: frozenset[str]
) -> bool | None:
    """Return state from the newest valid explicit transition."""
    matches: list[tuple[datetime, bool]] = []
    for event in events:
        if event.event_kind not in on_kinds | off_kinds:
            continue
        try:
            timestamp = datetime.fromisoformat(
                event.timestamp_utc.replace("Z", "+00:00")
            )
        except ValueError:
            continue
        matches.append((timestamp, event.event_kind in on_kinds))
    if not matches:
        return None
    return max(matches, key=lambda item: item[0])[1]


def _device_event_condition(
    state: DeviceState,
    key: str,
    on_kinds: frozenset[str],
    off_kinds: frozenset[str],
) -> bool | None:
    """Prefer an explicit REST connectivity value, then event transitions."""
    if key == "device_online" and state.is_online is not None:
        return state.is_online
    return _event_condition(state.events, on_kinds, off_kinds)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Whisker Ting binary sensors from a config entry."""
    coordinator = entry.runtime_data

    entities: list[WhiskerBinarySensor | WhiskerSiteBinarySensor] = []
    for device_id in coordinator.data:
        for description in BINARY_SENSOR_DESCRIPTIONS:
            entities.append(
                WhiskerBinarySensor(
                    coordinator=coordinator,
                    device_id=device_id,
                    description=description,
                )
            )

    for site_id in coordinator.sites:
        for site_description in SITE_BINARY_SENSOR_DESCRIPTIONS:
            entities.append(
                WhiskerSiteBinarySensor(coordinator, site_id, site_description)
            )

    async_add_entities(entities)


class WhiskerBinarySensor(WhiskerEntity, BinarySensorEntity):
    """Representation of a Whisker Ting binary sensor."""

    entity_description: WhiskerBinarySensorEntityDescription
    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: WhiskerDataUpdateCoordinator,
        device_id: str,
        description: WhiskerBinarySensorEntityDescription,
    ) -> None:
        """Initialize the binary sensor."""
        super().__init__(coordinator, device_id, description)

    @property
    def is_on(self) -> bool | None:
        """Return true if the binary sensor is on."""
        device_state = self.device_state
        if device_state is None:
            return None
        return self.entity_description.value_fn(device_state)


class WhiskerSiteBinarySensor(WhiskerSiteEntity, BinarySensorEntity):
    """Represent an event-derived condition for one Ting site."""

    entity_description: WhiskerSiteBinarySensorEntityDescription

    def __init__(
        self,
        coordinator: WhiskerDataUpdateCoordinator,
        site_id: int,
        description: WhiskerSiteBinarySensorEntityDescription,
    ) -> None:
        """Initialize the site condition sensor."""
        super().__init__(coordinator, site_id, description)

    @property
    def is_on(self) -> bool | None:
        """Return the newest explicit condition state."""
        site = self.site_state
        return self.entity_description.value_fn(site) if site else None
