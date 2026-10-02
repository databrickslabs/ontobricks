#!/usr/bin/env python3
"""Deploy-time Lakebase registry helpers (warehouse pick + initialize).

Used by ``scripts/deploy.sh``. Does not provision Lakebase projects —
that stays in ``scripts/bootstrap/setup-lakebase.sh``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Callable, Optional

STARTER_NAME = "serverless starter warehouse"
DEFAULT_VOLUME = "OntoBricksRegistry"


def parse_warehouses_payload(raw: str) -> list[dict]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if isinstance(data, list):
        return [w for w in data if isinstance(w, dict)]
    if isinstance(data, dict):
        warehouses = data.get("warehouses") or []
        return [w for w in warehouses if isinstance(w, dict)]
    return []


def _is_serverless(wh: dict) -> bool:
    if wh.get("enable_serverless_compute") is True:
        return True
    flag = str(wh.get("enable_serverless_compute") or "").strip().lower()
    return flag in {"true", "1", "yes"}


def pick_serverless_warehouse(warehouses: list[dict]) -> Optional[tuple[str, str]]:
    starter = None
    first_serverless = None
    for wh in warehouses:
        wid = str(wh.get("id") or "").strip()
        name = str(wh.get("name") or "").strip()
        if not wid:
            continue
        if name.lower() == STARTER_NAME:
            starter = (wid, name or "Serverless Starter Warehouse")
            break
        if first_serverless is None and _is_serverless(wh):
            first_serverless = (wid, name or wid)
    return starter or first_serverless


def initialize_registry(
    *,
    catalog: str,
    schema: str,
    volume: str,
    lakebase_schema: str,
    lakebase_database: str,
    store_factory: Optional[Callable[..., Any]] = None,
) -> tuple[bool, str]:
    from back.objects.registry.RegistryService import RegistryCfg

    if store_factory is None:
        from back.objects.registry.store import RegistryFactory

        store_factory = RegistryFactory.lakebase
    cfg = RegistryCfg(
        catalog=catalog,
        schema=schema,
        volume=volume or DEFAULT_VOLUME,
        lakebase_schema=lakebase_schema,
        lakebase_database=lakebase_database,
    )
    store = store_factory(
        registry_cfg=cfg,
        schema=lakebase_schema,
        database=lakebase_database,
    )
    # Existing registries are often owned by the app SP. Replaying
    # initialize() as the deployer hits "must be owner of table" even
    # for IF NOT EXISTS DDL. Skip when the schema is already live.
    probe = getattr(store, "is_initialized", None)
    if callable(probe) and probe():
        return True, (
            f"Lakebase registry already initialized "
            f"(schema={lakebase_schema})"
        )
    return store.initialize()


def _cmd_pick_warehouse(_args: argparse.Namespace) -> int:
    raw = sys.stdin.read()
    picked = pick_serverless_warehouse(parse_warehouses_payload(raw))
    if picked is None:
        print(
            "No serverless SQL warehouse found. Set DEFAULT_WAREHOUSE_ID "
            "or create a serverless warehouse in the workspace.",
            file=sys.stderr,
        )
        return 1
    print(f"{picked[0]}\t{picked[1]}")
    return 0


def _cmd_initialize(_args: argparse.Namespace) -> int:
    catalog = os.environ.get("REGISTRY_CATALOG", "").strip()
    schema = os.environ.get("REGISTRY_SCHEMA", "").strip()
    volume = os.environ.get("REGISTRY_VOLUME", "").strip() or DEFAULT_VOLUME
    lb_schema = os.environ.get("LAKEBASE_SCHEMA", "").strip()
    lb_database = os.environ.get("LAKEBASE_DATABASE", "").strip()
    missing = [
        name
        for name, val in (
            ("REGISTRY_CATALOG", catalog),
            ("REGISTRY_SCHEMA", schema),
            ("LAKEBASE_SCHEMA", lb_schema),
            ("LAKEBASE_DATABASE", lb_database),
        )
        if not val
    ]
    if missing:
        print("Missing env: " + ", ".join(missing), file=sys.stderr)
        return 1
    ok, msg = initialize_registry(
        catalog=catalog,
        schema=schema,
        volume=volume,
        lakebase_schema=lb_schema,
        lakebase_database=lb_database,
    )
    print(msg)
    return 0 if ok else 1


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("pick-warehouse", help="Read warehouses JSON on stdin; print id\\tname")
    sub.add_parser("initialize", help="Run LakebaseRegistryStore.initialize() from env")
    args = parser.parse_args(argv)
    if args.command == "pick-warehouse":
        return _cmd_pick_warehouse(args)
    if args.command == "initialize":
        return _cmd_initialize(args)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
