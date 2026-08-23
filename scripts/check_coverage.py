"""Require every Whisker Ting integration module to meet the coverage floor."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

MINIMUM_COVERAGE = 95.0
INTEGRATION_PREFIX = "custom_components/whisker_ting/"


def deficient_modules(report: dict[str, Any]) -> list[tuple[str, float]]:
    """Return integration modules below the required statement coverage."""
    deficient: list[tuple[str, float]] = []
    for filename, details in report.get("files", {}).items():
        if not filename.startswith(INTEGRATION_PREFIX):
            continue
        percent = float(details["summary"]["percent_covered"])
        if percent < MINIMUM_COVERAGE:
            deficient.append((filename, percent))
    return sorted(deficient)


def main() -> int:
    """Validate the coverage JSON path supplied by pytest-cov."""
    if len(sys.argv) != 2:
        print("usage: check_coverage.py COVERAGE_JSON", file=sys.stderr)
        return 2
    path = Path(sys.argv[1])
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as err:
        print(f"Unable to read coverage report {path}: {err}", file=sys.stderr)
        return 2

    deficient = deficient_modules(report)
    if deficient:
        print(
            f"Integration modules below {MINIMUM_COVERAGE:.0f}% coverage:",
            file=sys.stderr,
        )
        for filename, percent in deficient:
            print(f"- {filename}: {percent:.2f}%", file=sys.stderr)
        return 1
    print(f"Every integration module meets {MINIMUM_COVERAGE:.0f}% coverage.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
