"""In-memory reference implementation of the RegistryStore document API.

Mirrors ``LakebaseRegistryStore``'s ``domain_documents`` behaviour without a
Postgres backend, so the behavioural layers on top (``DocumentParseService``,
the document routes, Generate/Mapping/agent readers) can be unit-tested against
a real, deterministic store contract.

The reference semantics locked here:

- ``list_documents`` / ``get_document`` return metadata only (never
  ``parsed_text`` or ``source_bytes``);
- ``set_document_ready`` persists text and clears the transient bytes;
- ``set_document_failed`` keeps the bytes so a retry can run;
- ``copy_documents_to_version`` carries text forward but not bytes.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class FakeDocumentStore:
    """Duck-typed stand-in exposing the RegistryStore document API."""

    def __init__(self) -> None:
        # (folder, version, filename) -> full row dict
        self._rows: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    # -- helpers -----------------------------------------------------

    @staticmethod
    def _meta(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "filename": row["filename"],
            "source_hash": row["source_hash"],
            "parser": row["parser"],
            "status": row["status"],
            "size_bytes": row["size_bytes"],
            "output_schema": row["output_schema"],
            "error": row["error"],
            "parsed_at": row["parsed_at"] or "",
        }

    # -- API ---------------------------------------------------------

    def upsert_document(
        self,
        folder: str,
        version: str,
        *,
        filename: str,
        source_hash: str,
        parser: str,
        status: str,
        size_bytes: int,
        output_schema: str = "",
        source_bytes: Optional[bytes] = None,
        parsed_text: str = "",
        error: str = "",
    ) -> Tuple[bool, str]:
        self._rows[(folder, version, filename)] = {
            "filename": filename,
            "source_hash": source_hash,
            "parser": parser,
            "status": status,
            "parsed_text": parsed_text,
            "source_bytes": source_bytes,
            "size_bytes": int(size_bytes or 0),
            "output_schema": output_schema,
            "error": error,
            "parsed_at": _now_iso() if status == "ready" else "",
        }
        return True, ""

    def list_documents(self, folder: str, version: str) -> List[Dict[str, Any]]:
        return [
            self._meta(row)
            for (f, v, _fn), row in sorted(self._rows.items())
            if f == folder and v == version
        ]

    def get_document(
        self, folder: str, version: str, filename: str
    ) -> Optional[Dict[str, Any]]:
        row = self._rows.get((folder, version, filename))
        return self._meta(row) if row else None

    def read_document_text(
        self, folder: str, version: str, filename: str
    ) -> Optional[Tuple[str, str, str]]:
        row = self._rows.get((folder, version, filename))
        if not row:
            return None
        return (row["parsed_text"] or "", row["parser"], row["status"])

    def read_document_bytes(
        self, folder: str, version: str, filename: str
    ) -> Optional[bytes]:
        row = self._rows.get((folder, version, filename))
        if not row:
            return None
        return row["source_bytes"]

    def set_document_ready(
        self, folder: str, version: str, filename: str, *, parsed_text: str
    ) -> Tuple[bool, str]:
        row = self._rows.get((folder, version, filename))
        if not row:
            return False, "not found"
        row.update(
            status="ready",
            parsed_text=parsed_text,
            source_bytes=None,
            error="",
            parsed_at=_now_iso(),
        )
        return True, ""

    def set_document_failed(
        self, folder: str, version: str, filename: str, *, error: str
    ) -> Tuple[bool, str]:
        row = self._rows.get((folder, version, filename))
        if not row:
            return False, "not found"
        row.update(status="failed", error=error, parsed_at=_now_iso())
        return True, ""

    def set_document_pending(
        self, folder: str, version: str, filename: str
    ) -> Tuple[bool, str]:
        row = self._rows.get((folder, version, filename))
        if not row:
            return False, "not found"
        row.update(status="pending", error="", parsed_at="")
        return True, ""

    def delete_documents(
        self, folder: str, version: str, filenames: List[str]
    ) -> List[str]:
        for fn in filenames:
            self._rows.pop((folder, version, fn), None)
        return []

    def copy_documents_to_version(
        self, folder: str, from_version: str, to_version: str
    ) -> Tuple[bool, str]:
        for (f, v, fn), row in list(self._rows.items()):
            if f == folder and v == from_version:
                clone = dict(row)
                clone["source_bytes"] = None
                self._rows[(folder, to_version, fn)] = clone
        return True, ""

    def count_documents(self, folder: str, version: str) -> int:
        return sum(
            1 for (f, v, _fn) in self._rows if f == folder and v == version
        )
