"""Tests for the per-module coverage enforcement helper."""

from __future__ import annotations

from scripts.check_coverage import deficient_modules


def test_coverage_gate_reports_only_deficient_integration_modules() -> None:
    """The gate ignores external files and reports every weak integration module."""
    report = {
        "files": {
            "custom_components/whisker_ting/good.py": {
                "summary": {"percent_covered": 95}
            },
            "custom_components/whisker_ting/weak.py": {
                "summary": {"percent_covered": 94.9}
            },
            "tests/test_example.py": {"summary": {"percent_covered": 0}},
        }
    }
    assert deficient_modules(report) == [
        ("custom_components/whisker_ting/weak.py", 94.9)
    ]
