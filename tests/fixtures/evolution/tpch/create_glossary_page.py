"""Create (or update) the TPC-H glossary Knowledge Page used by the Evolve agent eval.

The page is created as a DRAFT in the workspace's general domain, so only its
owner sees it (and Genie One uses it only in the owner's conversations).
Re-running updates the body of the existing page with the same title.

    uv run --frozen python tests/fixtures/evolution/tpch/create_glossary_page.py --profile <p>
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import uuid
from pathlib import Path

TITLE = "OntoBricks Evolve test: TPC-H order-to-fulfilment glossary"
BODY = Path(__file__).with_name("glossary_page.md")
SYNONYMS = ["VOR", "vendor of record", "account", "market", "theatre", "SKU", "book"]
GENERAL_DOMAIN_TAG = "system.general_domain"


def _cli(profile: str, *args: str):
    out = subprocess.run(
        ["databricks", *args, "--profile", profile, "-o", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if out.returncode != 0:
        raise RuntimeError(f"CLI call failed: {out.stderr.strip()[:300]}")
    return json.loads(out.stdout or "{}")


def general_domain(profile: str) -> str:
    data = _cli(profile, "domains", "list-domains")
    rows = data if isinstance(data, list) else data.get("domains") or []
    for row in rows:
        if row.get("tag_key") == GENERAL_DOMAIN_TAG:
            return row["name"]
    raise RuntimeError("No general domain in this workspace")


def existing_page(profile: str, domain: str) -> str:
    token = ""
    while True:
        path = f"/api/2.0/knowledge/pages?page_size=100&filter=domain%20%3D%20%22{domain}%22"
        data = _cli(
            profile, "api", "get", path + (f"&page_token={token}" if token else "")
        )
        for page in data.get("pages") or []:
            if page.get("display_name") == TITLE:
                return page["name"]
        token = data.get("next_page_token") or ""
        if not token:
            return ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--profile", required=True)
    args = ap.parse_args()
    domain = general_domain(args.profile)
    fields = {
        "display_name": TITLE,
        "body": BODY.read_text(encoding="utf-8"),
        "synonyms": SYNONYMS,
        "description": "Test glossary for the OntoBricks Evolve agent eval (TPC-H).",
        "state": "DRAFT",
    }
    name = existing_page(args.profile, domain)
    if name:
        mask = "display_name,body,synonyms,description"
        page = _cli(
            args.profile,
            "api",
            "patch",
            f"/api/2.0/knowledge/{name}?update_mask={mask}",
            "--json",
            json.dumps({"name": name, **fields}),
        )
    else:
        page = _cli(
            args.profile,
            "api",
            "post",
            f"/api/2.0/knowledge/pages?request_id={uuid.uuid4()}",
            "--json",
            json.dumps({"domain": domain, **fields}),
        )
    print(
        json.dumps(
            {
                k: page.get(k)
                for k in ("name", "display_name", "state", "domain", "update_time")
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
