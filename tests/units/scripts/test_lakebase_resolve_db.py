#!/usr/bin/env python3
"""Unit tests for scripts/_lakebase-resolve-db.py (no pytest import of bash)."""
import importlib.util
import pathlib
import unittest

_ROOT = pathlib.Path(__file__).resolve().parents[3]
_SPEC = importlib.util.spec_from_file_location(
    "_lakebase_resolve_db",
    _ROOT / "scripts" / "_lakebase-resolve-db.py",
)
_mod = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(_mod)
resolve = _mod.resolve

_LIST_JSON = """{
  "databases": [
    {
      "name": "projects/demo/branches/production/databases/ontobricks-demo",
      "status": {"postgres_database": "ontobricks_demo"}
    },
    {
      "name": "projects/demo/branches/production/databases/db-abc123",
      "status": {"postgres_database": "other_db"}
    }
  ]
}"""


class TestLakebaseResolveDb(unittest.TestCase):
    def test_match_by_postgres_datname(self):
        hit = resolve("ontobricks_demo", _LIST_JSON)
        self.assertEqual(hit, ("ontobricks-demo", "ontobricks_demo"))

    def test_match_by_hyphenated_resource_id(self):
        hit = resolve("ontobricks-demo", _LIST_JSON)
        self.assertEqual(hit, ("ontobricks-demo", "ontobricks_demo"))

    def test_no_match(self):
        self.assertIsNone(resolve("missing", _LIST_JSON))


if __name__ == "__main__":
    unittest.main()
