"""Shared entity support for the Whisker Ting integration."""

from __future__ import annotations

from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity import EntityDescription
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .api import DeviceState, Site
from .const import DOMAIN
from .coordinator import WhiskerDataUpdateCoordinator


def site_device_info(site_id: int, site: Site | None) -> DeviceInfo:
    """Return stable, address-safe registry information for a Ting site."""
    return DeviceInfo(
        identifiers={(DOMAIN, f"site_{site_id}")},
        name=site.display_name if site and site.display_name else str(site_id),
        manufacturer="Whisker Labs",
        model="Ting Site",
    )


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
            if device_state.site_id in self.coordinator.sites:
                return DeviceInfo(
                    identifiers={(DOMAIN, self._device_id)},
                    name=device_state.name,
                    manufacturer="Whisker Labs",
                    model="Ting Fire Sensor",
                    sw_version=device_state.version,
                    via_device=(DOMAIN, f"site_{device_state.site_id}"),
                )
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


class WhiskerSiteEntity(CoordinatorEntity[WhiskerDataUpdateCoordinator]):
    """Base class for entities backed by one Ting site."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: WhiskerDataUpdateCoordinator,
        site_id: int,
        description: EntityDescription,
    ) -> None:
        """Initialize common coordinator and site state."""
        super().__init__(coordinator)
        self.entity_description = description
        self._site_id = site_id
        self._attr_unique_id = f"site_{site_id}_{description.key}"

    @property
    def site_state(self) -> Site | None:
        """Return the current validated state for this entity's site."""
        return self.coordinator.sites.get(self._site_id)

    @property
    def device_info(self) -> DeviceInfo:
        """Return stable, address-safe registry information for this Ting site."""
        return site_device_info(self._site_id, self.site_state)

    @property
    def available(self) -> bool:
        """Return whether coordinator and site data are available."""
        return super().available and self.site_state is not None
