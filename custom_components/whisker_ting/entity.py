"""Shared entity support for the Whisker Ting integration."""

from __future__ import annotations

from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity import EntityDescription
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .api import DeviceState
from .const import DOMAIN
from .coordinator import WhiskerDataUpdateCoordinator


class WhiskerEntity(CoordinatorEntity[WhiskerDataUpdateCoordinator]):
    """Base class for entities backed by one Ting device."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: WhiskerDataUpdateCoordinator,
        device_id: str,
        description: EntityDescription,
    ) -> None:
        """Initialize common coordinator and device state."""
        super().__init__(coordinator)
        self.entity_description = description
        self._device_id = device_id
        self._attr_unique_id = f"{device_id}_{description.key}"

    @property
    def device_state(self) -> DeviceState | None:
        """Return the current validated state for this entity's device."""
        return self.coordinator.data.get(self._device_id)

    @property
    def device_info(self) -> DeviceInfo:
        """Return stable registry information for this Ting device."""
        device_state = self.device_state
        if device_state is not None:
            return DeviceInfo(
                identifiers={(DOMAIN, self._device_id)},
                name=device_state.name,
                manufacturer="Whisker Labs",
                model="Ting Fire Sensor",
                sw_version=device_state.version,
            )
        return DeviceInfo(
            identifiers={(DOMAIN, self._device_id)},
            name=self._device_id,
            manufacturer="Whisker Labs",
        )

    @property
    def available(self) -> bool:
        """Return whether coordinator and device data are available."""
        return super().available and self.device_state is not None
