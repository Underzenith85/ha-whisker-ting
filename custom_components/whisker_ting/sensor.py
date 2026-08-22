"""Sensor platform for Whisker Ting."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    PERCENTAGE,
    EntityCategory,
    UnitOfElectricPotential,
    UnitOfFrequency,
    UnitOfTemperature,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .api import DeviceState, Site, TingEvent
from .const import DOMAIN
from .coordinator import WhiskerDataUpdateCoordinator
from .entity import WhiskerEntity, WhiskerSiteEntity

PARALLEL_UPDATES = 0  # Coordinator handles all updates

STRUCTURED_EVENT_KINDS: tuple[tuple[str, str], ...] = (
    ("last_power_outage", "power_outage"),
    ("last_power_restoration", "power_restored"),
    ("last_generator_on", "generator_on"),
    ("last_generator_off", "generator_off"),
    ("last_voltage_sag", "voltage_sag"),
    ("last_voltage_swell", "voltage_swell"),
    ("last_no_grounding_warning", "no_grounding"),
    ("last_high_temperature_alert", "high_temperature"),
    ("last_low_temperature_alert", "low_temperature"),
    ("last_fire_event", "fire_event"),
    ("last_utility_fire_event", "utility_fire_event"),
    ("last_device_online", "device_online"),
    ("last_device_offline", "device_offline"),
)


@dataclass(frozen=True, kw_only=True)
class WhiskerSensorEntityDescription(SensorEntityDescription):
    """Describes a Whisker Ting sensor entity."""

    value_fn: Callable[[DeviceState], Any]
    attributes_fn: Callable[[DeviceState], dict[str, Any] | None] | None = None


@dataclass(frozen=True, kw_only=True)
class WhiskerSiteSensorEntityDescription(SensorEntityDescription):
    """Describe a Whisker Ting site sensor entity."""

    value_fn: Callable[[Site], Any]
    attributes_fn: Callable[[Site], dict[str, Any] | None] | None = None


SITE_SENSOR_DESCRIPTIONS: tuple[WhiskerSiteSensorEntityDescription, ...] = (
    WhiskerSiteSensorEntityDescription(
        key="current_outdoor_temperature",
        translation_key="current_outdoor_temperature",
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        suggested_display_precision=1,
        value_fn=lambda site: site.current_temperature_c,
    ),
    WhiskerSiteSensorEntityDescription(
        key="current_outage_risk",
        translation_key="current_outage_risk",
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda site: _get_outage_risk_state(site.current_outage_risk),
        attributes_fn=lambda site: _get_outage_risk_attributes(
            site.current_outage_risk
        ),
    ),
    WhiskerSiteSensorEntityDescription(
        key="latest_event",
        translation_key="latest_event",
        value_fn=lambda site: site.events[0].event_type if site.events else None,
        attributes_fn=lambda site: _event_attributes(site.events),
    ),
    *(
        WhiskerSiteSensorEntityDescription(
            key=key,
            translation_key=key,
            device_class=SensorDeviceClass.TIMESTAMP,
            entity_category=EntityCategory.DIAGNOSTIC,
            entity_registry_enabled_default=False,
            value_fn=lambda site, kind=kind: _event_timestamp(site.events, kind),
        )
        for key, kind in STRUCTURED_EVENT_KINDS
    ),
)


SENSOR_DESCRIPTIONS: tuple[WhiskerSensorEntityDescription, ...] = (
    # Real-time voltage sensors (from WebSocket)
    WhiskerSensorEntityDescription(
        key="voltage",
        translation_key="voltage",
        device_class=SensorDeviceClass.VOLTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=UnitOfElectricPotential.VOLT,
        suggested_display_precision=2,
        value_fn=lambda state: (
            state.voltage.voltage if state.voltage.voltage > 0 else None
        ),
    ),
    WhiskerSensorEntityDescription(
        key="voltage_high",
        translation_key="voltage_high",
        device_class=SensorDeviceClass.VOLTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=UnitOfElectricPotential.VOLT,
        suggested_display_precision=2,
        value_fn=lambda state: (
            state.voltage.voltage_hi if state.voltage.voltage_hi > 0 else None
        ),
    ),
    WhiskerSensorEntityDescription(
        key="voltage_low",
        translation_key="voltage_low",
        device_class=SensorDeviceClass.VOLTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=UnitOfElectricPotential.VOLT,
        suggested_display_precision=2,
        value_fn=lambda state: (
            state.voltage.voltage_lo if state.voltage.voltage_lo > 0 else None
        ),
    ),
    WhiskerSensorEntityDescription(
        key="average_peaks_max",
        translation_key="average_peaks_max",
        device_class=SensorDeviceClass.VOLTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=UnitOfElectricPotential.VOLT,
        suggested_display_precision=2,
        entity_registry_enabled_default=False,
        value_fn=lambda state: (
            state.voltage.average_peaks_max
            if state.voltage.average_peaks_max > 0
            else None
        ),
    ),
    WhiskerSensorEntityDescription(
        key="frequency",
        translation_key="frequency",
        device_class=SensorDeviceClass.FREQUENCY,
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=UnitOfFrequency.HERTZ,
        suggested_display_precision=2,
        value_fn=lambda state: state.voltage.frequency_hz,
    ),
    WhiskerSensorEntityDescription(
        key="thd_min",
        translation_key="thd_min",
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=PERCENTAGE,
        suggested_display_precision=2,
        entity_registry_enabled_default=False,
        value_fn=lambda state: state.voltage.thd_min_percent,
    ),
    WhiskerSensorEntityDescription(
        key="thd_average",
        translation_key="thd_average",
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=PERCENTAGE,
        suggested_display_precision=2,
        value_fn=lambda state: state.voltage.thd_avg_percent,
    ),
    WhiskerSensorEntityDescription(
        key="thd_max",
        translation_key="thd_max",
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=PERCENTAGE,
        suggested_display_precision=2,
        entity_registry_enabled_default=False,
        value_fn=lambda state: state.voltage.thd_max_percent,
    ),
    # Primary status sensors (enabled by default)
    WhiskerSensorEntityDescription(
        key="hazard_status",
        translation_key="hazard_status",
        device_class=SensorDeviceClass.ENUM,
        options=[
            "no_hazards",
            "fire_hazard",
            "power_quality_hazard",
            "elevated_suspicious",
            "reviewed_not_fire",
            "learning",
            "unknown",
        ],
        value_fn=lambda state: _get_hazard_status(state),
        attributes_fn=lambda state: _hazard_attributes(state),
    ),
    WhiskerSensorEntityDescription(
        key="hazard_message",
        translation_key="hazard_message",
        value_fn=lambda state: state.fire_hazard_status.message,
    ),
    WhiskerSensorEntityDescription(
        key="efh_status",
        translation_key="efh_status",
        value_fn=lambda state: state.fire_hazard_status.efh_status.status or "none",
    ),
    WhiskerSensorEntityDescription(
        key="efh_message",
        translation_key="efh_message",
        value_fn=lambda state: state.fire_hazard_status.efh_status.message,
    ),
    WhiskerSensorEntityDescription(
        key="efh_level",
        translation_key="efh_level",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda state: state.fire_hazard_status.efh_status.level,
    ),
    WhiskerSensorEntityDescription(
        key="ufh_status",
        translation_key="ufh_status",
        value_fn=lambda state: state.fire_hazard_status.ufh_status.status or "none",
    ),
    WhiskerSensorEntityDescription(
        key="ufh_message",
        translation_key="ufh_message",
        value_fn=lambda state: state.fire_hazard_status.ufh_status.message,
    ),
    WhiskerSensorEntityDescription(
        key="frozen_pipe_risk_level",
        translation_key="frozen_pipe_risk_level",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda state: (
            state.frozen_pipe.status.level if state.frozen_pipe.status else None
        ),
    ),
    WhiskerSensorEntityDescription(
        key="frozen_pipe_outdoor_temperature",
        translation_key="frozen_pipe_outdoor_temperature",
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        suggested_display_precision=1,
        entity_registry_enabled_default=False,
        value_fn=lambda state: (
            state.frozen_pipe.status.outdoor_temperature_c
            if state.frozen_pipe.status
            else None
        ),
    ),
    WhiskerSensorEntityDescription(
        key="frozen_pipe_detected_location",
        translation_key="frozen_pipe_detected_location",
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda state: (
            state.frozen_pipe.status.detected_location_type
            if state.frozen_pipe.status
            else None
        ),
    ),
    WhiskerSensorEntityDescription(
        key="frozen_pipe_last_event",
        translation_key="frozen_pipe_last_event",
        device_class=SensorDeviceClass.TIMESTAMP,
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda state: _get_frozen_pipe_last_event(state),
    ),
    WhiskerSensorEntityDescription(
        key="latest_event",
        translation_key="latest_event",
        value_fn=lambda state: state.events[0].event_type if state.events else None,
        attributes_fn=lambda state: _event_attributes(state.events),
    ),
    WhiskerSensorEntityDescription(
        key="stream_health",
        translation_key="stream_health",
        device_class=SensorDeviceClass.ENUM,
        options=["receiving", "delayed", "not_receiving", "stopped"],
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda state: state.stream_health,
    ),
    WhiskerSensorEntityDescription(
        key="hazard_severity_level",
        translation_key="hazard_severity_level",
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda state: state.fire_hazard_status.hazard_severity_level,
    ),
    WhiskerSensorEntityDescription(
        key="device_type",
        translation_key="device_type",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda state: state.device_type,
    ),
    # Diagnostic sensors (disabled by default)
    WhiskerSensorEntityDescription(
        key="firmware_version",
        translation_key="firmware_version",
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda state: state.version,
    ),
    WhiskerSensorEntityDescription(
        key="wifi_mac",
        translation_key="wifi_mac",
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda state: state.wifi_mac_address,
    ),
    WhiskerSensorEntityDescription(
        key="bluetooth_mac",
        translation_key="bluetooth_mac",
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda state: state.bluetooth_mac_address,
    ),
    WhiskerSensorEntityDescription(
        key="serial_number",
        translation_key="serial_number",
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda state: state.serial_number,
    ),
    WhiskerSensorEntityDescription(
        key="group_name",
        translation_key="group_name",
        entity_category=EntityCategory.DIAGNOSTIC,
        entity_registry_enabled_default=False,
        value_fn=lambda state: state.group_name,
    ),
    *(
        WhiskerSensorEntityDescription(
            key=key,
            translation_key=key,
            device_class=SensorDeviceClass.TIMESTAMP,
            entity_category=EntityCategory.DIAGNOSTIC,
            entity_registry_enabled_default=False,
            value_fn=lambda state, kind=kind: _event_timestamp(state.events, kind),
        )
        for key, kind in STRUCTURED_EVENT_KINDS
    ),
)

REALTIME_SENSOR_KEYS = {
    "voltage",
    "voltage_high",
    "voltage_low",
    "average_peaks_max",
    "frequency",
    "thd_min",
    "thd_average",
    "thd_max",
}


def _get_outage_risk_state(
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


def _get_outage_risk_attributes(
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


def _get_frozen_pipe_last_event(state: DeviceState) -> datetime | None:
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


def _event_attributes(events: list[TingEvent]) -> dict[str, Any] | None:
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


def _event_timestamp(events: list[TingEvent], event_kind: str) -> datetime | None:
    """Return the newest valid timestamp for an explicitly classified event."""
    timestamps: list[datetime] = []
    for event in events:
        if event.event_kind != event_kind:
            continue
        try:
            timestamps.append(
                datetime.fromisoformat(event.timestamp_utc.replace("Z", "+00:00"))
            )
        except ValueError:
            continue
    return max(timestamps, default=None)


def _get_hazard_status(state: DeviceState) -> str:
    """Get the overall hazard status."""
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


def _hazard_attributes(state: DeviceState) -> dict[str, Any]:
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


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Whisker Ting sensors from a config entry."""
    coordinator = entry.runtime_data
    _migrate_site_entities(hass, coordinator)

    entities: list[WhiskerSensor | WhiskerSiteSensor] = []
    for device_id in coordinator.data:
        for description in SENSOR_DESCRIPTIONS:
            entities.append(
                WhiskerSensor(
                    coordinator=coordinator,
                    device_id=device_id,
                    description=description,
                )
            )

    for site_id in coordinator.sites:
        for description in SITE_SENSOR_DESCRIPTIONS:
            entities.append(
                WhiskerSiteSensor(
                    coordinator=coordinator,
                    site_id=site_id,
                    description=description,
                )
            )

    async_add_entities(entities)


def _migrate_site_entities(
    hass: HomeAssistant, coordinator: WhiskerDataUpdateCoordinator
) -> None:
    """Move one legacy per-device site entity and remove site duplicates."""
    registry = er.async_get(hass)
    devices_by_site: dict[int, list[str]] = {}
    for device in coordinator.data.values():
        devices_by_site.setdefault(device.site_id, []).append(device.serial_number)

    for site_id in coordinator.sites:
        for description in SITE_SENSOR_DESCRIPTIONS:
            if description.key not in {
                "current_outdoor_temperature",
                "current_outage_risk",
            }:
                continue
            new_unique_id = f"site_{site_id}_{description.key}"
            if registry.async_get_entity_id("sensor", DOMAIN, new_unique_id):
                retained_entity_id = None
            else:
                retained_entity_id = next(
                    (
                        entity_id
                        for serial_number in sorted(devices_by_site.get(site_id, []))
                        if (
                            entity_id := registry.async_get_entity_id(
                                "sensor",
                                DOMAIN,
                                f"{serial_number}_{description.key}",
                            )
                        )
                    ),
                    None,
                )
                if retained_entity_id:
                    registry.async_update_entity(
                        retained_entity_id, new_unique_id=new_unique_id
                    )

            for serial_number in devices_by_site.get(site_id, []):
                entity_id = registry.async_get_entity_id(
                    "sensor", DOMAIN, f"{serial_number}_{description.key}"
                )
                if entity_id and entity_id != retained_entity_id:
                    registry.async_remove(entity_id)


class WhiskerSensor(WhiskerEntity, SensorEntity):
    """Representation of a Whisker Ting sensor."""

    entity_description: WhiskerSensorEntityDescription
    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: WhiskerDataUpdateCoordinator,
        device_id: str,
        description: WhiskerSensorEntityDescription,
    ) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator, device_id, description)

    @property
    def available(self) -> bool:
        """Return if entity is available."""
        if not super().available:
            return False
        if self.entity_description.key in REALTIME_SENSOR_KEYS:
            return self.coordinator.is_realtime_available(self._device_id)
        return True

    @property
    def native_value(self) -> Any:
        """Return the state of the sensor."""
        device_state = self.device_state
        if device_state is None:
            return None
        return self.entity_description.value_fn(device_state)

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Return optional modeled attributes for this entity."""
        device_state = self.device_state
        if device_state is None or self.entity_description.attributes_fn is None:
            return None
        return self.entity_description.attributes_fn(device_state)


class WhiskerSiteSensor(WhiskerSiteEntity, SensorEntity):
    """Represent one site-scoped Ting condition."""

    entity_description: WhiskerSiteSensorEntityDescription

    def __init__(
        self,
        coordinator: WhiskerDataUpdateCoordinator,
        site_id: int,
        description: WhiskerSiteSensorEntityDescription,
    ) -> None:
        """Initialize the site sensor."""
        super().__init__(coordinator, site_id, description)

    @property
    def native_value(self) -> Any:
        """Return the current site-scoped value."""
        site = self.site_state
        return self.entity_description.value_fn(site) if site else None

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Return optional modeled attributes for this site condition."""
        site = self.site_state
        if site is None or self.entity_description.attributes_fn is None:
            return None
        return self.entity_description.attributes_fn(site)
