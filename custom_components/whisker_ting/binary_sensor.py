"""Binary sensor platform for Whisker Ting."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
    BinarySensorEntityDescription,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .api import DeviceState
from .coordinator import WhiskerDataUpdateCoordinator
from .entity import WhiskerEntity

PARALLEL_UPDATES = 0  # Coordinator handles all updates


@dataclass(frozen=True, kw_only=True)
class WhiskerBinarySensorEntityDescription(BinarySensorEntityDescription):
    """Describes a Whisker Ting binary sensor entity."""

    value_fn: Callable[[DeviceState], bool]


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
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Whisker Ting binary sensors from a config entry."""
    coordinator = entry.runtime_data

    entities: list[WhiskerBinarySensor] = []
    for device_id in coordinator.data:
        for description in BINARY_SENSOR_DESCRIPTIONS:
            entities.append(
                WhiskerBinarySensor(
                    coordinator=coordinator,
                    device_id=device_id,
                    description=description,
                )
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
