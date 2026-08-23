"""Complete Home Assistant config-flow branch coverage."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.config_entries import SOURCE_REAUTH, SOURCE_USER
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.whisker_ting.api import (
    UserData,
    WhiskerAuthError,
    WhiskerConnectionError,
)
from custom_components.whisker_ting.auth import AuthenticationError
from custom_components.whisker_ting.const import (
    CONF_API_KEY,
    CONF_PASSWORD,
    CONF_REFRESH_TOKEN,
    CONF_SCAN_INTERVAL,
    CONF_USER_ID,
    CONF_USERNAME,
    DOMAIN,
)


def client_with(result: object) -> MagicMock:
    """Return a client whose account lookup returns or raises one value."""
    client = MagicMock(
        refresh_token="new-refresh",
        user_id=42,
        api_key="new-key",
    )
    if isinstance(result, Exception):
        client.get_user_data = AsyncMock(side_effect=result)
    else:
        client.get_user_data = AsyncMock(return_value=result)
    return client


async def start_user_flow(hass: HomeAssistant, result: object) -> dict:
    """Submit one synthetic user flow."""
    with patch(
        "custom_components.whisker_ting.config_flow.WhiskerApiClient",
        return_value=client_with(result),
    ):
        return await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_USER},
            data={CONF_USERNAME: "person@example.invalid", CONF_PASSWORD: "password"},
        )


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_user_form_and_username_title(hass: HomeAssistant) -> None:
    """The empty form and account-without-name success paths are available."""
    form = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )
    assert form["type"] is FlowResultType.FORM

    result = await start_user_flow(
        hass,
        UserData(42, "person@example.invalid", first_name="", last_name=""),
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["title"] == "Whisker Ting (person@example.invalid)"


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize(
    ("failure", "error"),
    [
        (AuthenticationError("bad"), "invalid_auth"),
        (WhiskerAuthError("bad"), "invalid_auth"),
        (WhiskerConnectionError("offline"), "cannot_connect"),
        (RuntimeError("unexpected"), "unknown"),
    ],
)
async def test_user_flow_errors(
    hass: HomeAssistant, failure: Exception, error: str
) -> None:
    """Known and unexpected login errors map to translated form errors."""
    result = await start_user_flow(hass, failure)
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": error}


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_duplicate_account_aborts(hass: HomeAssistant) -> None:
    """An account stable ID cannot be configured twice."""
    entry = MockConfigEntry(domain=DOMAIN, unique_id="42", data={})
    entry.add_to_hass(hass)
    result = await start_user_flow(
        hass,
        UserData(42, "person@example.invalid", first_name="Example", last_name="User"),
    )
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"


def configured_entry(hass: HomeAssistant) -> MockConfigEntry:
    """Add a loaded-shape config entry for reauth and options flows."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="42",
        data={
            CONF_USERNAME: "old@example.invalid",
            CONF_REFRESH_TOKEN: "old-refresh",
            CONF_USER_ID: 42,
            CONF_API_KEY: "old-key",
        },
        options={CONF_SCAN_INTERVAL: 120},
    )
    entry.add_to_hass(hass)
    return entry


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize(
    ("result", "error"),
    [
        (AuthenticationError("bad"), "invalid_auth"),
        (WhiskerAuthError("bad"), "invalid_auth"),
        (WhiskerConnectionError("offline"), "cannot_connect"),
        (RuntimeError("unexpected"), "unknown"),
    ],
)
async def test_reauth_form_errors(
    hass: HomeAssistant, result: object, error: str
) -> None:
    """Reauthentication exposes every translated failure branch."""
    entry = configured_entry(hass)
    with patch(
        "custom_components.whisker_ting.config_flow.WhiskerApiClient",
        return_value=client_with(result),
    ):
        initial = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_REAUTH, "entry_id": entry.entry_id},
            data=entry.data,
        )
        assert initial["type"] is FlowResultType.FORM
        submitted = await hass.config_entries.flow.async_configure(
            initial["flow_id"],
            {CONF_USERNAME: "new@example.invalid", CONF_PASSWORD: "password"},
        )
    assert submitted["type"] is FlowResultType.FORM
    assert submitted["errors"] == {"base": error}


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_reauth_success_updates_credentials(hass: HomeAssistant) -> None:
    """Successful reauth updates renewable credentials and reloads the entry."""
    entry = configured_entry(hass)
    user = UserData(42, "new@example.invalid", first_name="", last_name="")
    with (
        patch(
            "custom_components.whisker_ting.config_flow.WhiskerApiClient",
            return_value=client_with(user),
        ),
        patch.object(hass.config_entries, "async_reload", AsyncMock(return_value=True)),
    ):
        initial = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_REAUTH, "entry_id": entry.entry_id},
            data=entry.data,
        )
        result = await hass.config_entries.flow.async_configure(
            initial["flow_id"],
            {CONF_USERNAME: "new@example.invalid", CONF_PASSWORD: "password"},
        )
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reauth_successful"
    assert entry.data[CONF_REFRESH_TOKEN] == "new-refresh"


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_options_form_and_submission(hass: HomeAssistant) -> None:
    """Options show the existing interval and persist a validated replacement."""
    entry = configured_entry(hass)
    initial = await hass.config_entries.options.async_init(entry.entry_id)
    assert initial["type"] is FlowResultType.FORM
    result = await hass.config_entries.options.async_configure(
        initial["flow_id"], {CONF_SCAN_INTERVAL: 300}
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options[CONF_SCAN_INTERVAL] == 300
