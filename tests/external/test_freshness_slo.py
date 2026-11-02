"""A configured domain meets the freshness and drift SLOs of its mode (``scripts/measure_freshness.py``).

Opt-in (``external``): skipped unless the environment of
``tests/external/test_mv_incremental_plan.py`` is set (``ONTOBRICKS_EXTERNAL_WAREHOUSE=1``
and either ``DATABRICKS_CONFIG_PROFILE`` + ``ONTOBRICKS_EXTERNAL_WAREHOUSE_ID`` or
``DATABRICKS_HOST`` + ``DATABRICKS_TOKEN`` + ``DATABRICKS_SQL_WAREHOUSE_ID``) and the
domain to measure is configured — ``ONTOBRICKS_FRESHNESS_STEM`` (the version's gateway
VIEW) and the source row ``ONTOBRICKS_FRESHNESS_SOURCE_TABLE`` / ``_KEY_COLUMN`` /
``_KEY_VALUE`` / ``_VALUE_COLUMN``. ``ONTOBRICKS_FRESHNESS_MODE`` (default
``microbatch``) picks the SLOs; ``ONTOBRICKS_FRESHNESS_READ_WAREHOUSE_ID`` the
Lakehouse//RT warehouse; ``ONTOBRICKS_FRESHNESS_PG_DSN`` with ``_SYNC_TABLE`` /
``_LIVE_TABLE`` adds the Lakebase paths; ``ONTOBRICKS_FRESHNESS_REPORT`` receives the
Markdown report (the nightly step summary).

The domain must already run in that mode (a build left its pipeline and views); the
test changes one source row a few times and restores it.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/measure_freshness.py"


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def _credentials() -> bool:
    profile = _env("DATABRICKS_CONFIG_PROFILE") and _env("ONTOBRICKS_EXTERNAL_WAREHOUSE_ID")
    token = _env("DATABRICKS_HOST") and _env("DATABRICKS_TOKEN") and _env("DATABRICKS_SQL_WAREHOUSE_ID")
    return bool(profile or token)


def _domain() -> bool:
    names = ("STEM", "SOURCE_TABLE", "KEY_COLUMN", "KEY_VALUE", "VALUE_COLUMN")
    return all(_env(f"ONTOBRICKS_FRESHNESS_{name}") for name in names)


pytestmark = [
    pytest.mark.external,
    pytest.mark.skipif(
        not (_env("ONTOBRICKS_EXTERNAL_WAREHOUSE") == "1" and _credentials() and _domain()),
        reason=(
            "external: set ONTOBRICKS_EXTERNAL_WAREHOUSE=1, the Databricks credentials and "
            "ONTOBRICKS_FRESHNESS_STEM / _SOURCE_TABLE / _KEY_COLUMN / _KEY_VALUE / _VALUE_COLUMN"
        ),
    ),
]


@pytest.fixture(scope="module")
def script():
    spec = importlib.util.spec_from_file_location("measure_freshness", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_domain_meets_the_slos_of_its_mode(script):
    args = script.parse_args([])

    report = script.run(args)

    if args.summary_file:
        Path(args.summary_file).write_text(report["markdown"] + "\n", encoding="utf-8")
    print(report["markdown"])
    assert report["violations"] == [], report["markdown"]
