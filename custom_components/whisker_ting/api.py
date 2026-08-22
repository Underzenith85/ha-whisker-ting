"""API client for Whisker Ting."""

from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import aiohttp

from .auth import AuthenticationError, WhiskerAuth
from .const import API_BASE_URL, API_USERS_ENDPOINT

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

    # Hazard status
    fire_hazard_status: FireHazardStatus = field(default_factory=FireHazardStatus)

    # Real-time voltage (from WebSocket)
    voltage: VoltageReading = field(default_factory=VoltageReading)

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
    ) -> dict[str, Any]:
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
        return {device.serial_number: device for device in user_data.devices}

    async def test_connection(self) -> bool:
        """Test the connection to the API."""
        try:
            await self.get_user_data()
            return True
        except WhiskerApiError:
            return False
