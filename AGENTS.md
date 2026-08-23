# Repository guidance

These instructions apply to the entire repository. Preserve the integration's
status as an independent, unofficial community project that is not affiliated
with or endorsed by Whisker Labs, Inc.

## Development environment

- Use Python 3.12 and uv. Do not recreate `requirements_test.txt` or install
  development dependencies with ad hoc pip commands.
- After changing `pyproject.toml`, run `uv lock` and commit the resulting
  `uv.lock` changes.
- Runtime dependencies belong in
  `custom_components/whisker_ting/manifest.json`. Development and test
  dependencies belong in the `dev` dependency group in `pyproject.toml`.
- Keep the uv project non-packaged. This Home Assistant custom integration is
  not a Python distribution.

Before submitting a change, run:

```bash
uv lock --check
uv sync --locked
uv run --no-sync ruff check custom_components tests
uv run --no-sync ruff format --check custom_components tests
uv run --no-sync mypy custom_components/whisker_ting
uv run --no-sync python -m compileall -q custom_components tests
uv run --no-sync pytest
uv run --no-sync python scripts/check_coverage.py coverage.json
```

## Architecture and Home Assistant conventions

- Keep REST transport, response parsing, models, and API errors under
  `custom_components/whisker_ting/api/`.
- Keep Cognito orchestration and SRP calculations under
  `custom_components/whisker_ting/auth/`.
- Keep SignalR framing, stream clients, parsers, models, and connection
  management under `custom_components/whisker_ting/stream/`.
- Protocol and domain packages must not depend on Home Assistant entity
  platforms. Put Home Assistant coordination and entity behavior in the
  integration's root modules.
- Use coordinator-backed entities and stable unique IDs. Avoid changing entity
  keys or device identifiers because Home Assistant registries persist them.
- Put user-visible static text in `strings.json` and
  `translations/en.json`. Keep those files synchronized and use entity
  translation keys instead of hardcoded names.
- Treat absent upstream data as unknown rather than inventing a user-visible
  fallback value. Preserve genuine API-provided text as source data.
- Tests must mock all Ting, Cognito, and SignalR network traffic.

## Security and data handling

- Never commit or publish credentials, tokens, API keys, device serial numbers,
  MAC addresses, street addresses, or unsanitized service responses.
- Use only synthetic or fully sanitized fixtures and logs.
- Do not weaken TLS verification, authentication checks, token redaction, or
  bounded parsing behavior to make a test pass.
- Keep the integration read-only unless a write operation is separately
  reviewed and explicitly approved.
- Do not commit APKs, decompiler output, packet captures, or other large binary
  research artifacts.

## Versioning and releases

Use [Semantic Versioning](https://semver.org/) for integration releases:

- **MAJOR**: incompatible changes such as removing or renaming entity keys,
  changing persisted configuration without migration, or dropping previously
  supported behavior.
- **MINOR**: backward-compatible capabilities such as new entities, options,
  service data, or supported Ting features.
- **PATCH**: backward-compatible bug fixes, security hardening, translations,
  documentation, refactors, tests, and development-tooling changes that do not
  add integration functionality.

Pre-release versions may use SemVer suffixes such as `1.3.0-beta.1`. Do not bump
the version for every pull request; bump it when preparing a release or when the
repository owner explicitly requests it.

For a version bump, update all of the following in the same change:

1. `custom_components/whisker_ting/manifest.json`
2. The project version in `pyproject.toml`
3. The version statement in `README.md`
4. `uv.lock`, regenerated with `uv lock`

The repository test suite must continue to enforce version alignment.

## Pull requests and distribution

- Base pull requests on the fork's current `main` and keep each pull request
  focused on one issue or cohesive change.
- Do not merge until tests, lint, HACS validation, Hassfest, and security checks
  are green.
- HACS custom-repository installation is supported. Do not submit or publish
  this project to the default HACS repository list unless the repository owner
  explicitly requests it.
- Update the README when installation, configuration, entities, compatibility,
  or contributor commands change.
