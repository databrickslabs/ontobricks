#!/usr/bin/env python3
"""Per-package coverage threshold checker.

Reads `coverage.xml` (produced by `pytest --cov-report=xml`) and enforces the
per-package floors declared in `ci/coverage_thresholds.yaml`. Exits 1 with a
violation table if any package falls below its line OR branch threshold.

This script is more flexible than `[tool.coverage.report] fail_under`, which
supports only a single global value.

Usage:
    python scripts/check_coverage.py [--config ci/coverage_thresholds.yaml] [--xml coverage.xml]

CI wiring:
    - run: uv run pytest tests/ --cov=src --cov-report=xml --cov-branch -m "not e2e and not property and not eval and not external"
    - run: uv run python scripts/check_coverage.py
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover
    print("ERROR: PyYAML is required (uv add pyyaml --group dev)", file=sys.stderr)
    sys.exit(2)


@dataclass(frozen=True)
class Threshold:
    line: float
    branch: float


@dataclass(frozen=True)
class Coverage:
    line_rate: float
    branch_rate: float
    line_total: int
    line_covered: int


def parse_thresholds(path: Path) -> tuple[Threshold, dict[str, Threshold]]:
    raw = yaml.safe_load(path.read_text())
    defaults = raw.get("defaults", {})
    default_threshold = Threshold(
        line=float(defaults.get("line", 0.90)),
        branch=float(defaults.get("branch", 0.80)),
    )
    package_thresholds: dict[str, Threshold] = {}
    for prefix, spec in (raw.get("packages") or {}).items():
        package_thresholds[prefix] = Threshold(
            line=float(spec.get("line", default_threshold.line)),
            branch=float(spec.get("branch", default_threshold.branch)),
        )
    return default_threshold, package_thresholds


def parse_coverage(path: Path) -> dict[str, Coverage]:
    tree = ET.parse(path)
    root = tree.getroot()
    by_file: dict[str, Coverage] = {}
    for cls in root.iter("class"):
        filename = cls.attrib.get("filename", "")
        if not filename:
            continue
        line_rate = float(cls.attrib.get("line-rate", "0"))
        branch_rate = float(cls.attrib.get("branch-rate", "0"))
        # Compute totals from <line> elements for accuracy.
        line_total = 0
        line_covered = 0
        for line in cls.iter("line"):
            line_total += 1
            if int(line.attrib.get("hits", "0")) > 0:
                line_covered += 1
        by_file[filename] = Coverage(
            line_rate=line_rate,
            branch_rate=branch_rate,
            line_total=line_total,
            line_covered=line_covered,
        )
    return by_file


def aggregate_by_prefix(
    files: dict[str, Coverage], prefixes: list[str]
) -> dict[str, Coverage]:
    """Group files by the most-specific matching prefix; sum line/branch counts."""
    sorted_prefixes = sorted(prefixes, key=len, reverse=True)
    grouped: dict[str, dict[str, float]] = {}

    for filename, cov in files.items():
        norm = filename.replace("\\", "/")
        # Match against either "src/..." or just the path
        candidates = [norm, f"src/{norm}"] if not norm.startswith("src/") else [norm]
        prefix = None
        for cand in candidates:
            for p in sorted_prefixes:
                if cand.startswith(p):
                    prefix = p
                    break
            if prefix:
                break
        if prefix is None:
            continue
        bucket = grouped.setdefault(
            prefix,
            {"line_total": 0, "line_covered": 0, "branch_rate_weighted": 0.0, "files": 0},
        )
        bucket["line_total"] += cov.line_total
        bucket["line_covered"] += cov.line_covered
        bucket["branch_rate_weighted"] += cov.branch_rate
        bucket["files"] += 1

    result: dict[str, Coverage] = {}
    for prefix, b in grouped.items():
        lt = int(b["line_total"])
        lc = int(b["line_covered"])
        fc = int(b["files"]) or 1
        result[prefix] = Coverage(
            line_rate=(lc / lt) if lt else 0.0,
            branch_rate=(b["branch_rate_weighted"] / fc),
            line_total=lt,
            line_covered=lc,
        )
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="ci/coverage_thresholds.yaml")
    parser.add_argument("--xml", default="coverage.xml")
    args = parser.parse_args(argv)

    config_path = Path(args.config)
    xml_path = Path(args.xml)
    if not config_path.exists():
        print(f"ERROR: thresholds config not found: {config_path}", file=sys.stderr)
        return 2
    if not xml_path.exists():
        print(f"ERROR: coverage xml not found: {xml_path}", file=sys.stderr)
        return 2

    default_thr, package_thrs = parse_thresholds(config_path)
    files = parse_coverage(xml_path)
    aggregated = aggregate_by_prefix(files, list(package_thrs.keys()))

    violations: list[tuple[str, str, float, float]] = []
    print("\n=== Per-package coverage (CNS T-M0.P5 gate) ===")
    print(f"{'Package':<40} {'Line':>10} {'Branch':>10} {'Min line':>10} {'Min branch':>12} {'Status':>10}")
    for prefix in sorted(package_thrs, key=len, reverse=True):
        thr = package_thrs[prefix]
        cov = aggregated.get(prefix)
        if cov is None:
            print(f"{prefix:<40} {'(no data)':>10}")
            continue
        line_ok = cov.line_rate >= thr.line
        branch_ok = cov.branch_rate >= thr.branch
        status = "OK" if (line_ok and branch_ok) else "FAIL"
        if not line_ok:
            violations.append((prefix, "line", cov.line_rate, thr.line))
        if not branch_ok:
            violations.append((prefix, "branch", cov.branch_rate, thr.branch))
        print(
            f"{prefix:<40} {cov.line_rate*100:>9.1f}% {cov.branch_rate*100:>9.1f}%"
            f" {thr.line*100:>9.1f}% {thr.branch*100:>11.1f}% {status:>10}"
        )

    # Overall global gate.
    all_files = files.values()
    total_lines = sum(f.line_total for f in all_files) or 1
    total_covered = sum(f.line_covered for f in all_files)
    overall_line = total_covered / total_lines
    print(f"\nOverall line coverage: {overall_line*100:.1f}% (gate: {default_thr.line*100:.1f}%)")
    if overall_line < default_thr.line:
        violations.append(("OVERALL", "line", overall_line, default_thr.line))

    if violations:
        print("\n=== Coverage gate FAILED ===")
        for prefix, kind, actual, required in violations:
            print(f"  - {prefix}: {kind}={actual*100:.1f}%  required>={required*100:.1f}%")
        return 1
    print("\nCoverage gate PASS.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
