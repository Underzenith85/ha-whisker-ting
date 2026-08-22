"""Repository-level tests that do not contact Ting or Cognito."""

from __future__ import annotations

import ast
import json
import re
import tomllib
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


def test_uv_project_matches_integration_release() -> None:
    """The locked, non-packaged uv project tracks the integration release."""
    manifest = _load_json(INTEGRATION / "manifest.json")
    with (ROOT / "pyproject.toml").open("rb") as file:
        pyproject = tomllib.load(file)

    assert pyproject["project"]["version"] == manifest["version"]
    assert pyproject["project"]["requires-python"] == ">=3.12,<3.13"
    assert pyproject["tool"]["uv"]["package"] is False
    with (ROOT / "uv.lock").open("rb") as file:
        lockfile = tomllib.load(file)
    project = next(
        package
        for package in lockfile["package"]
        if package["name"] == pyproject["project"]["name"]
    )
    assert project["version"] == manifest["version"]
    assert f"Integration version {manifest['version']} " in (
        ROOT / "README.md"
    ).read_text(encoding="utf-8")
    assert not (ROOT / "requirements_test.txt").exists()


def test_english_translation_matches_source_strings() -> None:
    """English translations remain synchronized with strings.json."""
    assert _load_json(INTEGRATION / "translations" / "en.json") == _load_json(
        INTEGRATION / "strings.json"
    )


def _imports(path: Path) -> set[str]:
    """Return imports found in one Python source file."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    return imports


def test_final_package_layout_has_no_legacy_monoliths() -> None:
    """Structural refactors cannot silently regress to root-level monoliths."""
    for legacy in ("api.py", "auth.py", "signalr.py", "websocket.py"):
        assert not (INTEGRATION / legacy).exists()
    for package in ("api", "auth", "stream"):
        assert (INTEGRATION / package / "__init__.py").is_file()


def test_generic_protocol_modules_have_no_home_assistant_dependencies() -> None:
    """Pure SRP and SignalR helpers remain reusable and side-effect free."""
    forbidden = {
        "homeassistant",
        "custom_components.whisker_ting.binary_sensor",
        "custom_components.whisker_ting.coordinator",
        "custom_components.whisker_ting.entity",
        "custom_components.whisker_ting.sensor",
    }
    for relative_path in ("auth/srp.py", "stream/signalr.py"):
        imports = _imports(INTEGRATION / relative_path)
        assert not any(
            imported == name or imported.startswith(f"{name}.")
            for imported in imports
            for name in forbidden
        )


def test_domain_packages_do_not_import_entity_platforms() -> None:
    """API, auth, and stream packages cannot depend on HA entity layers."""
    forbidden_parts = {"binary_sensor", "coordinator", "entity", "sensor"}
    for package in ("api", "auth", "stream"):
        for path in (INTEGRATION / package).glob("*.py"):
            assert not any(
                imported.rsplit(".", maxsplit=1)[-1] in forbidden_parts
                for imported in _imports(path)
            ), path
