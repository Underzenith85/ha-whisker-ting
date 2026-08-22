"""API client for Whisker Ting."""

from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

import aiohttp

from .auth import AuthenticationError, WhiskerAuth
from .const import (
    API_BASE_URL,
    API_FROZEN_PIPE_HISTORY_ENDPOINT,
    API_FROZEN_PIPE_STATUS_ENDPOINT,
    API_NOTIFICATION_HISTORY_ENDPOINT,
    API_USER_CONDITIONS_ENDPOINT,
    API_USERS_ENDPOINT,
)

_LOGGER = logging.getLogger(__name__)


@dataclass
class HazardStatus:
    """Represents a hazard status (EFH or UFH)."""

    status: str | None = None
    timestamp_utc: str | None = None
    level: int | None = None
    message: str = "No Hazards Detected"
    hex_color: str = "#00FF00"


@dataclass
class FireHazardStatus:
    """Represents the fire hazard status of a device."""

    learning_mode: bool = False
    hazard_severity_level: int | None = None
    message: str = "No Hazards Detected"
    efh_status: HazardStatus = field(default_factory=HazardStatus)
    ufh_status: HazardStatus = field(default_factory=HazardStatus)
    hex_color_light: str = "#00FF00"
    hex_color_medium: str = "#358C15"
    hex_color_dark: str = "#233016"


@dataclass
class VoltageReading:
    """Real-time voltage reading."""

    voltage: float = 0.0
    voltage_hi: float = 0.0
    voltage_lo: float = 0.0
    average_peaks_max: float = 0.0
    frequency_hz: float | None = None
    thd_min_percent: float | None = None
    thd_avg_percent: float | None = None
    thd_max_percent: float | None = None


@dataclass
class FrozenPipeRecord:
    """Read-only frozen-pipe status or history record."""

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
    """Detailed frozen-pipe data for a device."""

    status: FrozenPipeRecord | None = None
    history: list[FrozenPipeRecord] = field(default_factory=list)


@dataclass(frozen=True)
class TingEvent:
    """Normalized read-only Ting notification event."""

    event_type: str
    timestamp_utc: str
    serial_number: str
    event_id: str | None = None
    category: str | None = None
    title: str | None = None
    message: str | None = None


@dataclass
class DeviceState:
    """Represents the state of a Whisker Ting device."""

    serial_number: str
    name: str
    device_type: str
    site_id: int

    # Device info
    version: str | None = None
    wifi_mac_address: str | None = None
    bluetooth_mac_address: str | None = None
    soc_serial_number: str | None = None
    station_id: str | None = None  # For WebSocket connection

    # Status flags
    is_fire: bool = False
    is_hvac_verified: bool = False
    has_frozen_pipe: bool = False
    is_owner: bool = False
    stream_health: str = "stopped"
    current_temperature_c: float | None = None
    current_outage_risk: (
        str | int | float | dict[str, str | int | float | bool] | None
    ) = None

    # Hazard status
    fire_hazard_status: FireHazardStatus = field(default_factory=FireHazardStatus)

    # Real-time voltage (from WebSocket)
    voltage: VoltageReading = field(default_factory=VoltageReading)

    # Detailed frozen-pipe information (from optional read-only endpoints)
    frozen_pipe: FrozenPipeData = field(default_factory=FrozenPipeData)
    events: list[TingEvent] = field(default_factory=list)

    # Group info
    group_name: str | None = None
    group_id: int | None = None


@dataclass
class Site:
    """Represents a site/location."""

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


@dataclass
class UserData:
    """Represents user data from the API."""

    user_id: int
    email: str
    first_name: str
    last_name: str
    phone_number: str | None = None
    devices: list[DeviceState] = field(default_factory=list)
    sites: list[Site] = field(default_factory=list)


class WhiskerApiError(Exception):
    """Base exception for Whisker API errors."""


class WhiskerAuthError(WhiskerApiError):
    """Authentication error."""


class WhiskerConnectionError(WhiskerApiError):
    """Connection error."""


def _mapping(value: Any) -> dict[str, Any]:
    """Return a mapping or an empty mapping for missing/null/malformed values."""
    return value if isinstance(value, dict) else {}


def _collection(value: Any) -> list[Any]:
    """Return an API list or an empty list for malformed collections."""
    return value if isinstance(value, list) else []


def _optional_string(value: Any) -> str | None:
    """Return a string value, excluding empty and non-string values."""
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def _string(value: Any, default: str = "") -> str:
    """Return a non-empty string or a deterministic default."""
    return _optional_string(value) or default


def _integer(value: Any, default: int = 0) -> int:
    """Return an integer without accepting booleans or coercing arbitrary data."""
    return value if isinstance(value, int) and not isinstance(value, bool) else default


def _optional_integer(value: Any) -> int | None:
    """Return an integer or None for malformed values."""
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _identifier(value: Any) -> int | None:
    """Return a positive integer identifier or None."""
    value = _optional_integer(value)
    return value if value is not None and value > 0 else None


def _optional_identifier_string(value: Any) -> str | None:
    """Return a non-empty string or integer identifier as a string."""
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    return _optional_string(value)


def _optional_number(value: Any) -> float | None:
    """Return a finite-style numeric value without accepting booleans."""
    if (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    ):
        return float(value)
    return None


def _boolean(value: Any) -> bool:
    """Return only an explicit API boolean as a boolean."""
    return value if isinstance(value, bool) else False


def _bounded_scalar_mapping(value: Any) -> dict[str, str | int | float | bool] | None:
    """Copy a small scalar-only API object without retaining its raw response."""
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


def _first_optional_string(data: dict[str, Any], *keys: str) -> str | None:
    """Return the first valid string stored under one of the supplied keys."""
    return next(
        (
            value
            for key in keys
            if (value := _optional_string(data.get(key))) is not None
        ),
        None,
    )


def _parse_datetime(value: Any) -> datetime | None:
    """Parse an API timestamp and normalize it to UTC."""
    value = _optional_string(value)
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=parsed.tzinfo or UTC).astimezone(UTC)


class WhiskerApiClient:
    """Client for the Whisker Ting API."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        username: str,
        password: str | None = None,
        *,
        refresh_token: str | None = None,
        user_id: int | None = None,
        api_key: str | None = None,
    ) -> None:
        """Initialize the API client."""
        self._session = session
        self._username = username
        self._password = password
        self._auth = WhiskerAuth(session)

        # Token storage
        self._access_token: str | None = None
        self._refresh_token = refresh_token
        self._id_token: str | None = None
        self._api_key = api_key
        self._user_id = user_id
        self._token_expiry: datetime | None = None
        self._lock = asyncio.Lock()

    @property
    def user_id(self) -> int | None:
        """Return the user ID."""
        return self._user_id

    @property
    def api_key(self) -> str | None:
        """Return the API key."""
        return self._api_key

    @property
    def refresh_token(self) -> str | None:
        """Return the refresh token for config-entry persistence."""
        return self._refresh_token

    async def _ensure_token(self) -> str:
        """Ensure we have a valid access token."""
        async with self._lock:
            if self._access_token and self._token_expiry:
                # Refresh if token expires in less than 5 minutes
                if datetime.now(UTC) < self._token_expiry - timedelta(minutes=5):
                    return self._access_token

            # Need to authenticate or refresh
            if self._refresh_token:
                try:
                    await self._refresh_access_token()
                    return self._access_token
                except WhiskerAuthError:
                    # Refresh failed, try full auth
                    if self._password is None:
                        raise

            # Full authentication
            await self._authenticate()
            return self._access_token

    async def _authenticate(self) -> None:
        """Perform full authentication."""
        _LOGGER.debug("Performing full authentication")
        if self._password is None:
            raise WhiskerAuthError("Reauthentication required")
        try:
            result = await self._auth.authenticate(self._username, self._password)

            self._access_token = result["access_token"]
            self._refresh_token = result["refresh_token"]
            self._id_token = result["id_token"]

            self._token_expiry = datetime.now(UTC) + timedelta(
                seconds=result["expires_in"]
            )

            # Extract user info from attributes
            user_attrs = {
                attr["Name"]: attr["Value"]
                for attr in result.get("user_attributes", [])
            }
            self._user_id = int(user_attrs.get("custom:user_id", 0))
            self._api_key = user_attrs.get("custom:api_key")

            _LOGGER.debug("Authentication successful, user_id=%s", self._user_id)

        except AuthenticationError as err:
            raise WhiskerAuthError(str(err)) from err

    async def _refresh_access_token(self) -> None:
        """Refresh the access token."""
        _LOGGER.debug("Refreshing access token")
        try:
            result = await self._auth.refresh_tokens(self._refresh_token)

            self._access_token = result["AccessToken"]
            self._id_token = result.get("IdToken", self._id_token)
            self._token_expiry = datetime.now(UTC) + timedelta(
                seconds=result["ExpiresIn"]
            )

            _LOGGER.debug("Access token refreshed")

        except AuthenticationError as err:
            raise WhiskerAuthError(str(err)) from err

    async def _request(
        self,
        method: str,
        endpoint: str,
        **kwargs: Any,
    ) -> Any:
        """Make an authenticated request to the API."""
        token = await self._ensure_token()

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "x-wl-api-key": self._api_key or "",
        }

        url = f"{API_BASE_URL}{endpoint}"

        try:
            async with self._session.request(
                method, url, headers=headers, **kwargs
            ) as response:
                if response.status == 401:
                    # Token might have expired, try refreshing once
                    async with self._lock:
                        await self._authenticate()
                    token = self._access_token
                    headers["Authorization"] = f"Bearer {token}"
                    async with self._session.request(
                        method, url, headers=headers, **kwargs
                    ) as retry_response:
                        if retry_response.status == 401:
                            raise WhiskerAuthError("Authentication failed")
                        retry_response.raise_for_status()
                        return await retry_response.json()

                if response.status != 200:
                    raise WhiskerApiError(
                        f"API request failed with status {response.status}"
                    )

                return await response.json()

        except aiohttp.ClientError as err:
            raise WhiskerConnectionError(f"Connection error: {err}") from err

    async def get_user_data(self) -> UserData:
        """Get user data including devices."""
        if not self._user_id:
            await self._ensure_token()

        endpoint = API_USERS_ENDPOINT.format(user_id=self._user_id)
        data = await self._request("GET", endpoint)

        return self._parse_user_data(data)

    async def get_user_conditions(self) -> dict[str, Any] | None:
        """Get the lightweight conditions snapshot used by Ting 3.0.4."""
        if not self._user_id:
            await self._ensure_token()
        endpoint = API_USER_CONDITIONS_ENDPOINT.format(user_id=self._user_id)
        data = await self._get_optional_data(endpoint)
        return data if isinstance(data, dict) else None

    async def get_frozen_pipe_data(self, serial_number: str) -> FrozenPipeData:
        """Get optional detailed frozen-pipe status and current history."""
        encoded_serial = quote(serial_number, safe="")
        status_endpoint = API_FROZEN_PIPE_STATUS_ENDPOINT.format(
            serial_number=encoded_serial
        )
        history_endpoint = API_FROZEN_PIPE_HISTORY_ENDPOINT.format(
            serial_number=encoded_serial
        )
        status_result, history_result = await asyncio.gather(
            self._get_optional_data(status_endpoint),
            self._get_optional_data(history_endpoint),
        )
        return FrozenPipeData(
            status=self._parse_frozen_pipe_record(status_result),
            history=self._parse_frozen_pipe_history(history_result),
        )

    async def get_event_history(self, days: int = 30) -> list[TingEvent]:
        """Get a bounded window of read-only notification history."""
        if not self._user_id:
            await self._ensure_token()
        now = datetime.now(UTC)
        endpoint = API_NOTIFICATION_HISTORY_ENDPOINT.format(user_id=self._user_id)
        data = await self._get_optional_data(
            endpoint,
            params={
                "sentStartUtc": (now - timedelta(days=days)).isoformat(),
                "sentEndUtc": now.isoformat(),
                "excludeStatuses": "true",
                "excludeCleared": "false",
            },
        )
        return self._parse_event_history(data)

    async def _get_optional_data(self, endpoint: str, **kwargs: Any) -> Any | None:
        """Fetch an optional feature endpoint without failing the main update."""
        try:
            return await self._request("GET", endpoint, **kwargs)
        except WhiskerApiError as err:
            _LOGGER.debug("Optional Ting feature endpoint unavailable: %s", err)
            return None

    def _parse_frozen_pipe_record(self, data: Any) -> FrozenPipeRecord | None:
        """Parse a frozen-pipe record while discarding unknown response fields."""
        if not isinstance(data, dict) or not data:
            return None
        record = FrozenPipeRecord(
            level=_optional_integer(data.get("level")),
            outdoor_temperature_c=_optional_number(data.get("outdoorTemperatureC")),
            detected_location_type=_optional_string(data.get("detectedLocationType")),
            timestamp_utc=_first_optional_string(
                data, "timestampUtc", "detectedTimestampUtc", "createdUtc"
            ),
            resolved_timestamp_utc=_first_optional_string(
                data, "resolvedTimestampUtc", "resolvedUtc"
            ),
            user_action=_optional_string(data.get("userAction")),
            notification_type=_optional_string(data.get("notificationType")),
            notification_delivery_mode=_optional_string(
                data.get("notificationDeliveryMode")
            ),
        )
        return (
            record
            if any(value is not None for value in vars(record).values())
            else None
        )

    def _parse_frozen_pipe_history(self, data: Any) -> list[FrozenPipeRecord]:
        """Parse known single-record and collection history response shapes."""
        if isinstance(data, list):
            values = data
        elif isinstance(data, dict):
            collection = next(
                (
                    data.get(key)
                    for key in ("history", "items", "records", "data")
                    if isinstance(data.get(key), list)
                ),
                None,
            )
            values = collection if collection is not None else [data]
        else:
            values = []
        return [
            record
            for value in values
            if (record := self._parse_frozen_pipe_record(value)) is not None
        ]

    def _parse_event_history(self, data: Any) -> list[TingEvent]:
        """Normalize, scope, and deterministically order notification history."""
        if not isinstance(data, list):
            return []
        events: list[TingEvent] = []
        for value in data:
            if not isinstance(value, dict):
                continue
            event_type = _optional_string(value.get("eventType"))
            serial_number = _optional_string(value.get("serialNumber"))
            timestamp_value = _first_optional_string(
                value,
                "eventTimestampUtc",
                "sentTimestampUtc",
                "sentUtc",
                "eventTimestampLocal",
            )
            timestamp = _parse_datetime(timestamp_value)
            if event_type is None or serial_number is None or timestamp is None:
                continue
            events.append(
                TingEvent(
                    event_type=event_type,
                    timestamp_utc=timestamp.isoformat(),
                    serial_number=serial_number,
                    event_id=_optional_identifier_string(value.get("id")),
                    category=_optional_string(value.get("eventCategory")),
                    title=_optional_string(value.get("title")),
                    message=_optional_string(value.get("message")),
                )
            )
        return sorted(events, key=lambda event: event.timestamp_utc, reverse=True)

    def _parse_user_data(self, data: dict[str, Any]) -> UserData:
        """Parse user data from API response."""
        devices: list[DeviceState] = []
        for index, device_data in enumerate(_collection(data.get("devices"))):
            if not isinstance(device_data, dict):
                _LOGGER.warning("Skipping malformed device at index %d", index)
                continue
            serial_number = _optional_string(device_data.get("serialNumber"))
            if serial_number is None:
                _LOGGER.warning(
                    "Skipping device at index %d without a valid serial number", index
                )
                continue
            devices.append(self._parse_device(device_data, serial_number))

        sites: list[Site] = []
        for index, site_data in enumerate(_collection(data.get("sites"))):
            if not isinstance(site_data, dict):
                _LOGGER.warning("Skipping malformed site at index %d", index)
                continue
            site_id = _identifier(site_data.get("id"))
            if site_id is None:
                _LOGGER.warning("Skipping site at index %d without a valid ID", index)
                continue
            site = Site(
                id=site_id,
                user_id=_integer(site_data.get("userId")),
                display_name=_string(site_data.get("displayName")),
                address_line1=_optional_string(site_data.get("addressLine1")),
                city=_optional_string(site_data.get("city")),
                state_province=_optional_string(site_data.get("stateProvince")),
                postal_code=_optional_string(site_data.get("postalCode")),
                country=_optional_string(site_data.get("country")),
                latitude=_optional_number(site_data.get("latitude")),
                longitude=_optional_number(site_data.get("longitude")),
            )
            sites.append(site)

        return UserData(
            user_id=_integer(data.get("id")),
            email=_string(data.get("email")),
            first_name=_string(data.get("firstName")),
            last_name=_string(data.get("lastName")),
            phone_number=_optional_string(data.get("phoneNumber")),
            devices=devices,
            sites=sites,
        )

    def _parse_device(
        self, data: dict[str, Any], serial_number: str | None = None
    ) -> DeviceState:
        """Parse device state from API response."""
        serial_number = serial_number or _optional_string(data.get("serialNumber"))
        if serial_number is None:
            raise ValueError("Device has no valid serial number")

        # Parse fire hazard status
        fhs_data = _mapping(data.get("fireHazardStatus"))
        efh_data = _mapping(fhs_data.get("efhStatus"))
        ufh_data = _mapping(fhs_data.get("ufhStatus"))
        hex_colors = _mapping(fhs_data.get("hexColor"))

        efh_status = HazardStatus(
            status=_optional_string(efh_data.get("status")),
            timestamp_utc=_optional_string(efh_data.get("timestampUtc")),
            level=_optional_integer(efh_data.get("level")),
            message=_string(efh_data.get("message"), "No Hazards Detected"),
            hex_color=_string(efh_data.get("hexColor"), "#00FF00"),
        )

        ufh_status = HazardStatus(
            status=_optional_string(ufh_data.get("status")),
            timestamp_utc=_optional_string(ufh_data.get("timestampUtc")),
            level=_optional_integer(ufh_data.get("level")),
            message=_string(ufh_data.get("message"), "No Hazards Detected"),
            hex_color=_string(ufh_data.get("hexColor"), "#00FF00"),
        )

        fire_hazard_status = FireHazardStatus(
            learning_mode=_boolean(fhs_data.get("learningMode")),
            hazard_severity_level=_optional_integer(
                fhs_data.get("hazardSeverityLevel")
            ),
            message=_string(fhs_data.get("message"), "No Hazards Detected"),
            efh_status=efh_status,
            ufh_status=ufh_status,
            hex_color_light=_string(hex_colors.get("light"), "#00FF00"),
            hex_color_medium=_string(hex_colors.get("medium"), "#358C15"),
            hex_color_dark=_string(hex_colors.get("dark"), "#233016"),
        )

        # Parse group info
        group_data = _mapping(data.get("group"))

        return DeviceState(
            serial_number=serial_number,
            name=_string(data.get("name"), serial_number),
            device_type=_string(data.get("type"), "Unknown"),
            site_id=_integer(data.get("siteId")),
            version=_optional_string(data.get("version")),
            wifi_mac_address=_optional_string(data.get("wifiMacAddress")),
            bluetooth_mac_address=_optional_string(data.get("bluetoothMacAddress")),
            soc_serial_number=_optional_string(data.get("socSerialNumber")),
            station_id=serial_number,
            is_fire=_boolean(data.get("isFire")),
            is_hvac_verified=_boolean(data.get("isHvacVerified")),
            has_frozen_pipe=_boolean(data.get("hasFrozenPipe")),
            is_owner=_boolean(data.get("isOwner")),
            fire_hazard_status=fire_hazard_status,
            group_name=_optional_string(group_data.get("name")),
            group_id=_optional_integer(group_data.get("id")),
        )

    async def get_all_device_states(self) -> dict[str, DeviceState]:
        """Get the state of all devices."""
        user_data = await self.get_user_data()
        devices = {device.serial_number: device for device in user_data.devices}
        conditions = await self.get_user_conditions()
        if conditions is None:
            return devices

        temperatures = _mapping(conditions.get("currentTemperatures"))
        outage_risks = _mapping(conditions.get("currentOutageRisks"))
        for device in devices.values():
            site_key = str(device.site_id)
            device.current_temperature_c = _optional_number(temperatures.get(site_key))
            risk = outage_risks.get(site_key)
            device.current_outage_risk = (
                risk
                if isinstance(risk, (str, int, float)) and not isinstance(risk, bool)
                else _bounded_scalar_mapping(risk)
            )

        # The conditions response also carries fresher copies of device status.
        for value in _collection(conditions.get("devices")):
            if not isinstance(value, dict):
                continue
            serial_number = _optional_string(value.get("serialNumber"))
            device = devices.get(serial_number or "")
            if device is None:
                continue
            if "isFire" in value:
                device.is_fire = _boolean(value.get("isFire"))
            if "isHvacVerified" in value:
                device.is_hvac_verified = _boolean(value.get("isHvacVerified"))
            if "hasFrozenPipe" in value:
                device.has_frozen_pipe = _boolean(value.get("hasFrozenPipe"))
            if isinstance(value.get("fireHazardStatus"), dict):
                device.fire_hazard_status = self._parse_device(
                    value, serial_number
                ).fire_hazard_status
        return devices

    async def test_connection(self) -> bool:
        """Test the connection to the API."""
        try:
            await self.get_user_data()
            return True
        except WhiskerApiError:
            return False
