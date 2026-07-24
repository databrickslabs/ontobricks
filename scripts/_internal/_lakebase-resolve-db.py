#!/usr/bin/env python3
"""Resolve Lakebase db segment + Postgres datname from list-databases JSON.

Lakebase exposes two different names per database:
  - ``name`` trailing segment (``database_id``, RFC-1123 — hyphens only)
  - ``status.postgres_database`` (actual ``PGDATABASE`` / datname — underscores)

Callers may pass either form in *requested*; we match exact first, then
underscore/hyphen slug variants on both fields.

Prints ``<segment>\\t<postgres_database>`` on stdout, or nothing if no match.
"""
from __future__ import annotations

import json
import sys


def _slug_variants(value: str) -> set[str]:
    if not value:
        return set()
    return {value, value.replace("_", "-"), value.replace("-", "_")}


def resolve(requested: str, raw: str) -> tuple[str, str] | None:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    dbs = data if isinstance(data, list) else data.get("databases", [])
    wanted = _slug_variants(requested)
    if not wanted:
        return None

    for db in dbs:
        status = db.get("status") or {}
        pg_name = (status.get("postgres_database") or "").strip()
        segment = (db.get("name") or "").rsplit("/", 1)[-1]
        if pg_name in wanted or segment in wanted:
            return segment, pg_name
    return None


def main() -> None:
    if len(sys.argv) != 2:
        print("usage: list-databases.json | _lakebase-resolve-db.py <requested>", file=sys.stderr)
        sys.exit(2)
    hit = resolve(sys.argv[1], sys.stdin.read())
    if hit:
        segment, pg_name = hit
        print(f"{segment}\t{pg_name}")


if __name__ == "__main__":
    main()
