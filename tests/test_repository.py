"""Repository-level tests that do not contact Ting or Cognito."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parents[1]
INTEGRATION = ROOT / "custom_components" / "whisker_ting"


def _load_json(path: Path) -> dict[str, Any]:
    """Load a JSON object from the repository."""
    with path.open(encoding="utf-8") as file:
        value = json.load(file)
    assert isinstance(value, dict)
    return value


def test_all_json_files_are_valid_objects() -> None:
    """Every checked-in JSON file parses to an object."""
    paths = sorted(ROOT.glob("*.json")) + sorted(INTEGRATION.rglob("*.json"))
    assert paths

    for path in paths:
        assert _load_json(path)


def test_manifest_identity_and_required_fields() -> None:
    """The integration manifest has a consistent identity and release version."""
    manifest = _load_json(INTEGRATION / "manifest.json")

    assert manifest["domain"] == INTEGRATION.name
    assert manifest["name"] == "Whisker Ting"
    assert manifest["config_flow"] is True
    assert manifest["iot_class"] == "cloud_push"
    assert re.fullmatch(r"\d+\.\d+\.\d+", manifest["version"])
    assert isinstance(manifest["requirements"], list)


def test_english_translation_matches_source_strings() -> None:
    """English translations remain synchronized with strings.json."""
    assert _load_json(INTEGRATION / "translations" / "en.json") == _load_json(
        INTEGRATION / "strings.json"
    )
