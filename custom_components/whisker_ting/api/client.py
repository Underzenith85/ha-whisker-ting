"""REST client for the Whisker Ting API."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

import aiohttp

from ..auth import AuthenticationError, WhiskerAuth
from ..const import (
    API_BASE_URL,
    API_FROZEN_PIPE_HISTORY_ENDPOINT,
    API_FROZEN_PIPE_STATUS_ENDPOINT,
    API_NOTIFICATION_HISTORY_ENDPOINT,
    API_USER_CONDITIONS_ENDPOINT,
    API_USERS_ENDPOINT,
    API_VOLTAGE_HISTORY_ENDPOINT,
)
from .errors import (
    WhiskerApiError,
    WhiskerAuthError,
    WhiskerAuthorizationError,
    WhiskerConnectionError,
    WhiskerInvalidResponseError,
    WhiskerNotFoundError,
    WhiskerRateLimitError,
    WhiskerServiceError,
)
from .models import (
    ConditionsSnapshot,
    DeviceState,
    FrozenPipeData,
    Site,
    TingEvent,
    UserData,
    VoltageHistoryPoint,
)
from .parsers import (
    parse_conditions,
    parse_event_history,
    parse_frozen_pipe_history,
    parse_frozen_pipe_record,
    parse_user_data,
    parse_voltage_history,
)

_LOGGER = logging.getLogger(__name__)


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
        self._sites: dict[int, Site] = {}
        self._unauthorized_capabilities: set[str] = set()

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

    @property
    def sites(self) -> dict[int, Site]:
        """Return the most recently validated sites by stable site ID."""
        return self._sites

    @property
    def unauthorized_capabilities(self) -> frozenset[str]:
        """Return optional capabilities rejected with explicit authorization errors."""
        return frozenset(self._unauthorized_capabilities)

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
                    if self._access_token is not None:
                        return self._access_token
                    raise WhiskerAuthError("Token refresh returned no access token")
                except WhiskerAuthError:
                    # Refresh failed, try full auth
                    if self._password is None:
                        raise

            # Full authentication
            await self._authenticate()
            if self._access_token is not None:
                return self._access_token
            raise WhiskerAuthError("Authentication returned no access token")

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
        if self._refresh_token is None:
            raise WhiskerAuthError("No refresh token is available")
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
        url = f"{API_BASE_URL}{endpoint}"

        try:
            for attempt in range(2):
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                    "x-wl-api-key": self._api_key or "",
                }
                async with self._session.request(
                    method, url, headers=headers, **kwargs
                ) as response:
                    if response.status == 401 and attempt == 0:
                        token = await self._renew_after_unauthorized(token)
                        continue
                    self._raise_for_api_status(response.status)
                    try:
                        return await response.json()
                    except (aiohttp.ContentTypeError, ValueError) as err:
                        raise WhiskerInvalidResponseError(
                            "API returned an invalid JSON response"
                        ) from err

            raise WhiskerAuthError("Authentication failed")
        except WhiskerApiError:
            raise
        except (aiohttp.ClientError, TimeoutError) as err:
            raise WhiskerConnectionError("Unable to reach the Ting API") from err

    async def _renew_after_unauthorized(self, rejected_token: str) -> str:
        """Renew credentials once after a rejected access token."""
        async with self._lock:
            if self._access_token and self._access_token != rejected_token:
                return self._access_token
            if self._refresh_token:
                try:
                    await self._refresh_access_token()
                except WhiskerAuthError:
                    if self._password is None:
                        raise
                    await self._authenticate()
            else:
                await self._authenticate()
            if self._access_token is None:
                raise WhiskerAuthError("Authentication failed")
            return self._access_token

    @staticmethod
    def _raise_for_api_status(status: int) -> None:
        """Raise a bounded exception for an unsuccessful API status."""
        if 200 <= status < 300:
            return
        if status == 401:
            raise WhiskerAuthError("Authentication failed")
        if status == 403:
            raise WhiskerAuthorizationError("API resource is not authorized")
        if status == 404:
            raise WhiskerNotFoundError("API resource was not found")
        if status == 429:
            raise WhiskerRateLimitError("API rate limit exceeded")
        if 500 <= status < 600:
            raise WhiskerServiceError("Ting API service is temporarily unavailable")
        raise WhiskerApiError(f"API request failed with status {status}")

    async def get_user_data(self) -> UserData:
        """Get user data including devices."""
        if not self._user_id:
            await self._ensure_token()

        endpoint = API_USERS_ENDPOINT.format(user_id=self._user_id)
        data = await self._request("GET", endpoint)

        return parse_user_data(data)

    async def get_user_conditions(self) -> ConditionsSnapshot | None:
        """Get the lightweight conditions snapshot used by Ting 3.0.4."""
        if not self._user_id:
            await self._ensure_token()
        endpoint = API_USER_CONDITIONS_ENDPOINT.format(user_id=self._user_id)
        data = await self._get_optional_data(endpoint, capability="conditions")
        return parse_conditions(data)

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
            self._get_optional_data(status_endpoint, capability="frozen_pipe"),
            self._get_optional_data(history_endpoint, capability="frozen_pipe"),
        )
        return FrozenPipeData(
            status=parse_frozen_pipe_record(status_result),
            history=parse_frozen_pipe_history(history_result),
        )

    async def get_event_history(self, days: int = 30) -> list[TingEvent]:
        """Get a bounded window of read-only notification history."""
        if not self._user_id:
            await self._ensure_token()
        now = datetime.now(UTC)
        endpoint = API_NOTIFICATION_HISTORY_ENDPOINT.format(user_id=self._user_id)
        data = await self._get_optional_data(
            endpoint,
            capability="event_history",
            params={
                "sentStartUtc": (now - timedelta(days=days)).isoformat(),
                "sentEndUtc": now.isoformat(),
                "excludeStatuses": "true",
                "excludeCleared": "false",
            },
        )
        return parse_event_history(data)

    async def get_voltage_history(
        self, serial_number: str, start: datetime, end: datetime
    ) -> list[VoltageHistoryPoint]:
        """Get one explicitly bounded v3 voltage-history window."""
        if start.tzinfo is None or end.tzinfo is None or start >= end:
            raise ValueError(
                "Voltage history requires an ordered, timezone-aware window"
            )
        if end - start > timedelta(hours=24):
            raise ValueError("A voltage history request cannot exceed 24 hours")
        endpoint = API_VOLTAGE_HISTORY_ENDPOINT.format(
            serial_number=quote(serial_number, safe="")
        )
        data = await self._request(
            "GET",
            endpoint,
            params={
                "startUtc": start.astimezone(UTC).isoformat(),
                "endUtc": end.astimezone(UTC).isoformat(),
            },
        )
        return parse_voltage_history(data)

    async def _get_optional_data(
        self, endpoint: str, *, capability: str | None = None, **kwargs: Any
    ) -> Any | None:
        """Fetch an optional feature endpoint without failing the main update."""
        try:
            result = await self._request("GET", endpoint, **kwargs)
            if capability:
                self._unauthorized_capabilities.discard(capability)
            return result
        except WhiskerAuthError:
            raise
        except WhiskerAuthorizationError as err:
            if capability:
                self._unauthorized_capabilities.add(capability)
            _LOGGER.debug("Optional Ting capability is not authorized: %s", err)
            return None
        except WhiskerNotFoundError as err:
            if capability:
                self._unauthorized_capabilities.discard(capability)
            _LOGGER.debug("Optional Ting capability is unsupported: %s", err)
            return None
        except WhiskerApiError as err:
            _LOGGER.debug("Optional Ting feature endpoint unavailable: %s", err)
            return None

    async def get_all_device_states(self) -> dict[str, DeviceState]:
        """Get the state of all devices."""
        user_data = await self.get_user_data()
        self._sites = {site.id: site for site in user_data.sites}
        devices = {device.serial_number: device for device in user_data.devices}
        conditions = await self.get_user_conditions()
        if conditions is None:
            return devices

        for site in self._sites.values():
            site.current_temperature_c = conditions.temperatures.get(site.id)
            site.current_outage_risk = conditions.outage_risks.get(site.id)

        # The conditions response also carries fresher copies of device status.
        for value in conditions.devices:
            device = devices.get(value.serial_number)
            if device is None:
                continue
            if value.is_fire is not None:
                device.is_fire = value.is_fire
            if value.is_hvac_verified is not None:
                device.is_hvac_verified = value.is_hvac_verified
            if value.has_frozen_pipe is not None:
                device.has_frozen_pipe = value.has_frozen_pipe
            if value.fire_hazard_status is not None:
                device.fire_hazard_status = value.fire_hazard_status
        return devices

    async def test_connection(self) -> bool:
        """Test the connection to the API."""
        try:
            await self.get_user_data()
            return True
        except (WhiskerAuthError, WhiskerConnectionError):
            raise
        except WhiskerApiError:
            return False
