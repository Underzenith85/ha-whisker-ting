# REST field mapping

The integration converts the account response into typed `UserData`, `Site`, and
`DeviceState` models. It does not retain the original response or expose it as
entity state or attributes.

## Retained and exposed

- Account: numeric user ID, email, name, and optional phone number.
- Site: numeric IDs, display name, address fields, and numeric coordinates.
- Device identity: serial number, name, type, site, firmware, network identifiers,
  ownership, HVAC, frozen-pipe, and fire flags.
- Hazard state: learning mode, overall message and colors, plus modeled EFH and UFH
  status, timestamp, level, message, and color fields.
- Group: optional numeric ID and name.

Unknown non-empty device type strings are retained as diagnostic values. A missing,
null, or non-string type becomes `Unknown`.

## Validation and defaults

Devices without a non-empty string serial number and sites without an integer ID are
skipped. Diagnostics identify only the collection index; response contents are not
logged. Missing, null, or incorrectly typed nested objects use safe defaults.

## Intentionally ignored

Unmodeled response fields are discarded. Historical voltage, power-quality data,
and new entities based on those endpoints are outside this parser reliability work.
