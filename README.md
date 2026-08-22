# Whisker Ting Integration for Home Assistant

[![Tests](https://github.com/Underzenith85/ha-whisker-ting/actions/workflows/tests.yml/badge.svg)](https://github.com/Underzenith85/ha-whisker-ting/actions/workflows/tests.yml)

An unofficial Home Assistant integration for monitoring [Whisker Labs Ting](https://www.tingfire.com/) electrical fire safety sensors.

> [!WARNING]
> This is an independent community project. It is not created, maintained, affiliated with, authorized by, or endorsed by Whisker Labs, Inc. Ting and Whisker Labs are trademarks of their respective owner.

The integration provides read-only access to Ting account, device, hazard, frozen-pipe, notification, current-condition, and real-time electrical data. Integration version 1.1.0 is aligned with the Ting 3.0.4 service behavior.

> [!IMPORTANT]
> This integration is not currently published to HACS. Install it manually from this repository. Do not add it as a HACS custom repository yet.

## Features

- AWS Cognito authentication with automatic access-token refresh
- Multiple Ting devices under one account
- Real-time voltage, frequency, and total harmonic distortion (THD) streaming over SignalR WebSockets
- Current site temperature and bounded outage-risk diagnostics
- Explicit electrical-fire and power-quality hazard states
- Frozen-pipe risk, temperature, location, and current-history data
- Read-only notification history scoped to each device
- Per-device stream-health diagnostics
- Configurable REST polling interval

The account password is used to complete initial authentication and is not saved in the Home Assistant config entry. Home Assistant stores the renewable refresh token, Ting user ID, and API key needed for subsequent updates.

## Installation

### Manual installation

1. Download or clone [this repository](https://github.com/Underzenith85/ha-whisker-ting).
2. Copy `custom_components/whisker_ting` into your Home Assistant configuration directory:

   ```text
   config/custom_components/whisker_ting
   ```

3. Restart Home Assistant.
4. Clear the browser cache if Whisker Ting does not appear in the integration picker.

## Configuration

1. Open **Settings → Devices & services**.
2. Select **Add integration**.
3. Search for **Whisker Ting**.
4. Enter the email address and password used by the Ting application.

One config entry represents one Ting account. Every supported device returned for that account is created as a separate Home Assistant device.

### Options

The REST polling interval can be configured from the integration options. Valid values are 30–3600 seconds, with a default of 60 seconds.

Real-time voltage, frequency, and THD are delivered separately over a WebSocket and are not limited by the REST polling interval. Entity updates from the high-frequency streams are published to Home Assistant at most once per second.

Current temperature, outage risk, hazard state, and other device conditions are refreshed through the configured REST polling interval.

## Entities

Some diagnostic entities are disabled by default and can be enabled from the device page.

### Sensors

| Entity | Default | Description |
| --- | --- | --- |
| Current voltage | Enabled | Latest real-time voltage reading |
| Voltage high | Enabled | High value from the latest voltage sample |
| Voltage low | Enabled | Low value from the latest voltage sample |
| Average peaks max | Disabled | Average peak value from the stream |
| Frequency | Enabled | Latest real-time line-frequency reading |
| THD minimum | Disabled | Latest minimum total harmonic distortion reading |
| THD average | Enabled | Latest average total harmonic distortion reading |
| THD maximum | Disabled | Latest maximum total harmonic distortion reading |
| Hazard status | Enabled | Normalized overall hazard state |
| Hazard message | Enabled | Account-provided overall hazard message |
| Electrical fire hazard status | Enabled | Raw modeled EFH status |
| Electrical fire hazard message | Enabled | EFH status message |
| Electrical fire hazard level | Enabled | EFH diagnostic level; not treated as a boolean |
| Unverified fire hazard status | Enabled | Raw modeled UFH status |
| Unverified fire hazard message | Enabled | UFH status message |
| Frozen pipe risk level | Enabled | Detailed frozen-pipe risk level when supported |
| Frozen pipe outdoor temperature | Disabled | Outdoor temperature associated with frozen-pipe evaluation |
| Current outdoor temperature | Enabled | Current site temperature from the Ting conditions snapshot |
| Current outage risk | Disabled | Bounded site-level outage-risk status and diagnostic attributes |
| Frozen pipe detected location | Disabled | Conditioned, unconditioned, or unknown-space classification |
| Frozen pipe last event | Disabled | Latest modeled frozen-pipe event timestamp |
| Latest event | Enabled | Latest read-only Ting notification for the device |
| Stream health | Enabled | Health of the device's live voltage stream |
| Hazard severity level | Disabled | Ting hazard workflow severity value |
| Device type | Enabled | Ting device type |
| Firmware version | Disabled | Device firmware version |
| Wi-Fi MAC address | Disabled | Device Wi-Fi address |
| Bluetooth MAC address | Disabled | Device Bluetooth address |
| Serial number | Disabled | Ting serial number |
| Group | Disabled | Ting account group name |

### Hazard status values

The overall hazard-status sensor reports one of:

- `no_hazards`
- `fire_hazard`
- `power_quality_hazard`
- `elevated_suspicious`
- `reviewed_not_fire`
- `learning`
- `unknown`

Hazards are derived from explicit Ting EFH and UFH status values. A positive numeric level alone does not activate a hazard.

### Stream health values

- `receiving` — live data is arriving normally
- `delayed` — the last valid reading is retained, but no new data has arrived for approximately five seconds
- `not_receiving` — no data has arrived for approximately ten seconds and reconnection is attempted
- `stopped` — the stream was intentionally stopped

Only real-time voltage, frequency, and THD entities become unavailable when the stream is no longer receiving data. REST-backed hazard and diagnostic entities remain available while REST updates continue succeeding.

### Binary sensors

| Entity | Default | Description |
| --- | --- | --- |
| Fire hazard | Enabled | Ting account reports an active fire condition |
| Electrical fire hazard | Enabled | EFH status is elevated, possible fire, or hazard found |
| Power quality hazard | Enabled | UFH status reports a power-quality hazard |
| Frozen pipe risk | Enabled | Detailed frozen-pipe risk, falling back to the account flag |
| Learning mode | Enabled | Device is learning the home's electrical environment |
| HVAC verified | Disabled | Ting reports HVAC verification complete |
| Is owner | Disabled | Account is marked as the device owner |

## Latest-event attributes

When notification history is available, the latest-event sensor can expose:

- Event ID
- Event category
- UTC timestamp
- Title and message
- Number of retained events in the current history window

Notification history is read-only. The integration does not acknowledge, clear, or alter Ting notifications.

## Limitations

- Ting is a cloud service; an internet connection and working Ting services are required.
- The cloud APIs are not documented as a public third-party integration API and may change without notice.
- Historical voltage and power-quality time series are not currently imported.
- Site-level events without a reliable device serial number are not assigned to a device.
- The integration does not register for mobile push notifications.
- BLE/Wi-Fi provisioning, device setup and reset, billing, subscriptions, surveys, contractor scheduling, and notification-management writes are intentionally unsupported.
- This integration does not replace Ting or emergency-safety guidance. Do not rely on Home Assistant as the sole notification path for a fire or electrical hazard.

## Troubleshooting

### Real-time electrical data is unknown after startup

The integration waits for the SignalR connection, subscription acknowledgement, and first valid data packet. Voltage uses the primary stream; frequency and THD use optional secondary streams and can remain unknown when those streams are unavailable for an account. Check the stream-health sensor if voltage remains unknown.

### Stream health is delayed or not receiving

Confirm that the Ting device is online and the Home Assistant host can reach Ting's cloud services. The integration automatically attempts reconnection when data stops arriving.

### Authentication failed

Use the same email address and password used by the Ting application. If the refresh token is rejected, Home Assistant starts a reauthentication flow so the account can be connected again.

### Temperature, outage-risk, frozen-pipe, or event entities are unknown

These are optional account/device capabilities. Unsupported or unauthorized optional endpoints do not prevent the rest of the integration from loading.

### Collecting diagnostics

Enable debug logging temporarily:

```yaml
logger:
  logs:
    custom_components.whisker_ting: debug
```

Debug output must not be shared without reviewing it for account or device information. Never publish passwords, tokens, API keys, serial numbers, MAC addresses, addresses, or unsanitized API responses.

## Development

Python 3.12 is required by the pinned Home Assistant test environment. From a clean checkout:

```bash
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements_test.txt
python -m pytest
python -m ruff check custom_components tests
python -m compileall -q custom_components tests
```

Tests must mock Ting and Cognito traffic. Never commit account credentials, tokens, device identifiers, or unsanitized API responses.

The integration keeps REST transport, errors, validated models, and untrusted-response
parsing in `custom_components/whisker_ting/api/`. Parser-focused tests live in
`tests/api/`; sanitized response fixtures remain in `tests/fixtures/`.

## Support

Report integration problems through the [issue tracker](https://github.com/Underzenith85/ha-whisker-ting/issues).

Do not contact Whisker Labs for support with this Home Assistant integration. For Ting device, account, or safety-service support, use Whisker Labs' official support channels.

## Attribution

This software is an independent, unofficial integration and is not affiliated with or endorsed by Whisker Labs, Inc. Product and company names are used only to identify compatibility.

## License

MIT License — see [LICENSE](LICENSE).
