"""Validated domain models returned by the Ting REST API."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime


@dataclass
class HazardStatus:
    """Represent an electrical fire hazard status."""

    status: str | None = None
    timestamp_utc: str | None = None
    level: int | None = None
    message: str | None = None
    hex_color: str = "#00FF00"


@dataclass
class FireHazardStatus:
    """Represent the fire hazard status of a device."""

    learning_mode: bool = False
    hazard_severity_level: int | None = None
    message: str | None = None
    efh_status: HazardStatus = field(default_factory=HazardStatus)
    ufh_status: HazardStatus = field(default_factory=HazardStatus)
    hex_color_light: str = "#00FF00"
    hex_color_medium: str = "#358C15"
    hex_color_dark: str = "#233016"


@dataclass
class VoltageReading:
    """Represent a real-time voltage reading."""

    voltage: float = 0.0
    voltage_hi: float = 0.0
    voltage_lo: float = 0.0
    average_peaks_max: float = 0.0
    frequency_hz: float | None = None
    thd_min_percent: float | None = None
    thd_avg_percent: float | None = None
    thd_max_percent: float | None = None

    @property
    def has_live_data(self) -> bool:
        """Return whether any real-time metric has been populated."""
        return self.voltage > 0 or any(
            value is not None
            for value in (
                self.frequency_hz,
                self.thd_min_percent,
                self.thd_avg_percent,
                self.thd_max_percent,
            )
        )

    def with_voltage(
        self,
        *,
        voltage: float,
        voltage_hi: float,
        voltage_lo: float,
        average_peaks_max: float,
    ) -> VoltageReading:
        """Return a copy with a new primary voltage sample."""
        return replace(
            self,
            voltage=voltage,
            voltage_hi=voltage_hi,
            voltage_lo=voltage_lo,
            average_peaks_max=average_peaks_max,
        )

    def with_frequency(self, value: float) -> VoltageReading:
        """Return a copy with a new frequency sample."""
        return replace(self, frequency_hz=value)

    def with_thd_min(self, value: float) -> VoltageReading:
        """Return a copy with a new minimum THD sample."""
        return replace(self, thd_min_percent=value)

    def with_thd_average(self, value: float) -> VoltageReading:
        """Return a copy with a new average THD sample."""
        return replace(self, thd_avg_percent=value)

    def with_thd_max(self, value: float) -> VoltageReading:
        """Return a copy with a new maximum THD sample."""
        return replace(self, thd_max_percent=value)


@dataclass(frozen=True)
class VoltageHistoryPoint:
    """Represent one validated aggregate from voltage history."""

    start: datetime
    minimum_v: float
    maximum_v: float
    average_v: float
    coverage: float | None = None


@dataclass
class FrozenPipeRecord:
    """Represent a read-only frozen-pipe status or history record."""

    level: int | None = None
    outdoor_temperature_c: float | None = None
    detected_location_type: str | None = None
    timestamp_utc: str | None = None
    resolved_timestamp_utc: str | None = None
    user_action: str | None = None
    notification_type: str | None = None
    notification_delivery_mode: str | None = None


@dataclass
class FrozenPipeData:
    """Represent detailed frozen-pipe data for a device."""

    status: FrozenPipeRecord | None = None
    history: list[FrozenPipeRecord] = field(default_factory=list)


@dataclass(frozen=True)
class TingEvent:
    """Represent a normalized read-only Ting notification event."""

    event_type: str
    timestamp_utc: str
    serial_number: str | None = None
    site_id: int | None = None
    event_kind: str | None = None
    event_id: str | None = None
    category: str | None = None
    title: str | None = None
    message: str | None = None


@dataclass
class DeviceState:
    """Represent the state of a Whisker Ting device."""

    serial_number: str
    name: str
    device_type: str
    site_id: int
    version: str | None = None
    wifi_mac_address: str | None = None
    bluetooth_mac_address: str | None = None
    soc_serial_number: str | None = None
    station_id: str | None = None
    is_fire: bool = False
    is_hvac_verified: bool = False
    has_frozen_pipe: bool = False
    is_owner: bool = False
    is_online: bool | None = None
    rest_health: str = "healthy"
    last_rest_update_utc: datetime | None = None
    last_device_observation_utc: datetime | None = None
    last_realtime_sample_utc: datetime | None = None
    stream_reconnect_count: int = 0
    last_stream_reconnect_utc: datetime | None = None
    last_stream_reconnect_reason: str | None = None
    stream_health: str = "stopped"
    current_temperature_c: float | None = None
    current_outage_risk: (
        str | int | float | dict[str, str | int | float | bool] | None
    ) = None
    fire_hazard_status: FireHazardStatus = field(default_factory=FireHazardStatus)
    voltage: VoltageReading = field(default_factory=VoltageReading)
    frozen_pipe: FrozenPipeData = field(default_factory=FrozenPipeData)
    events: list[TingEvent] = field(default_factory=list)
    group_name: str | None = None
    group_id: int | None = None


@dataclass
class Site:
    """Represent a site or location."""

    id: int
    user_id: int
    display_name: str
    address_line1: str | None = None
    city: str | None = None
    state_province: str | None = None
    postal_code: str | None = None
    country: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    current_temperature_c: float | None = None
    current_outage_risk: (
        str | int | float | dict[str, str | int | float | bool] | None
    ) = None
    events: list[TingEvent] = field(default_factory=list)


@dataclass
class UserData:
    """Represent validated user data from the API."""

    user_id: int
    email: str
    first_name: str
    last_name: str
    phone_number: str | None = None
    devices: list[DeviceState] = field(default_factory=list)
    sites: list[Site] = field(default_factory=list)


@dataclass(frozen=True)
class DeviceConditions:
    """Represent validated condition overrides for one device."""

    serial_number: str
    is_fire: bool | None = None
    is_hvac_verified: bool | None = None
    has_frozen_pipe: bool | None = None
    fire_hazard_status: FireHazardStatus | None = None


@dataclass(frozen=True)
class ConditionsSnapshot:
    """Represent a validated current-conditions API snapshot."""

    temperatures: dict[int, float] = field(default_factory=dict)
    outage_risks: dict[int, str | int | float | dict[str, str | int | float | bool]] = (
        field(default_factory=dict)
    )
    devices: list[DeviceConditions] = field(default_factory=list)
