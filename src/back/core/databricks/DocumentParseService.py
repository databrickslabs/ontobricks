"""Parsed-document corpus persisted in the Lakebase ``domain_documents`` table.

The Knowledge Store keeps one row per uploaded document, scoped by
``(folder, version)``. Binary documents are parsed Volume-free via
``DocumentExtractor.extract_from_bytes`` (inline base64 ``ai_parse_document``);
plain-text documents are stored decoded. The original bytes are held only
transiently on the row (``source_bytes``) so a parse/retry can run, then NULLed
once the document reaches ``ready``. No Unity Catalog Volume is involved.
"""

from __future__ import annotations

import hashlib
import os
import threading
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from back.core.logging import get_logger
from back.core.databricks.DocumentExtractor import DocumentExtractor

logger = get_logger(__name__)


class ParseStatus(str, Enum):
    """Durable document parse states (mirrors the DB ``status`` column)."""

    PENDING = "pending"
    READY = "ready"
    FAILED = "failed"


@dataclass(frozen=True)
class ParseSubmission:
    """Result of preparing an upload or retry for parsing."""

    filename: str
    uploaded: bool
    no_op: bool
    should_parse: bool
    parse_status: ParseStatus
    error: Optional[str] = None


class DocumentParseService:
    """Manage uploads, parse state, and ready corpus text in Lakebase."""

    # Hard per-file cap enforced at upload. Base64 inflation (×4/3) plus the
    # ~16 MiB Statement Execution API statement-text limit for inline
    # ``ai_parse_document`` bound this; 10 MB keeps a safe margin.
    MAX_UPLOAD_BYTES = 10 * 1024 * 1024

    OUTPUT_SCHEMA_VERSION = DocumentExtractor.OUTPUT_SCHEMA_VERSION

    TEXT_EXTENSIONS = frozenset(
        {
            "txt",
            "md",
            "json",
            "csv",
            "xml",
            "ttl",
            "owl",
            "rdf",
            "yaml",
            "yml",
            "toml",
            "ini",
            "cfg",
            "log",
            "sql",
            "py",
            "js",
            "ts",
            "html",
            "css",
        }
    )

    _state_lock = threading.Lock()
    _path_locks: Dict[str, threading.Lock] = {}
    _active_parses: Set[str] = set()

    def __init__(self, store: Any, extractor: Any = None) -> None:
        self._store = store
        self._extractor = extractor

    # -- identity / validation --------------------------------------

    @staticmethod
    def _source_hash(content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    @classmethod
    def _validate_filename(cls, filename: str) -> str:
        candidate = (filename or "").strip()
        if (
            not candidate
            or candidate in {".", ".."}
            or candidate != os.path.basename(candidate)
            or "/" in candidate
            or "\\" in candidate
        ):
            raise ValueError("Invalid filename")
        return candidate

    @classmethod
    def _parser_for(cls, extension: str) -> str:
        if extension in cls.TEXT_EXTENSIONS:
            return "plaintext"
        if DocumentExtractor.supports(extension):
            return "ai_parse_document"
        return "unsupported"

    # -- concurrency guards -----------------------------------------

    @staticmethod
    def _key(folder: str, version: str, filename: str) -> str:
        return f"{folder}/{version}/{filename}"

    @classmethod
    def _lock_for(cls, key: str) -> threading.Lock:
        with cls._state_lock:
            return cls._path_locks.setdefault(key, threading.Lock())

    @classmethod
    def _set_active(cls, key: str, active: bool) -> None:
        with cls._state_lock:
            if active:
                cls._active_parses.add(key)
            else:
                cls._active_parses.discard(key)

    @classmethod
    def _is_active(cls, key: str) -> bool:
        with cls._state_lock:
            return key in cls._active_parses

    # -- upload -----------------------------------------------------

    def prepare_upload(
        self, folder: str, version: str, filename: str, content: bytes
    ) -> ParseSubmission:
        """Record an upload and create its initial durable parse state."""
        filename = self._validate_filename(filename)
        source_hash = self._source_hash(content)
        extension = DocumentExtractor.file_extension(filename)
        key = self._key(folder, version, filename)

        with self._lock_for(key):
            size = len(content)
            if size > self.MAX_UPLOAD_BYTES:
                error = (
                    f"File exceeds the {self.MAX_UPLOAD_BYTES // (1024 * 1024)} MB "
                    "upload limit"
                )
                self._store.upsert_document(
                    folder,
                    version,
                    filename=filename,
                    source_hash=source_hash,
                    parser=self._parser_for(extension),
                    status=ParseStatus.FAILED.value,
                    size_bytes=size,
                    error=error,
                )
                return ParseSubmission(
                    filename, True, False, False, ParseStatus.FAILED, error
                )

            existing = self._store.get_document(folder, version, filename)
            if (
                existing
                and existing.get("source_hash") == source_hash
                and existing.get("status") == ParseStatus.READY.value
            ):
                return ParseSubmission(
                    filename, False, True, False, ParseStatus.READY
                )
            if (
                existing
                and existing.get("source_hash") == source_hash
                and existing.get("status") == ParseStatus.PENDING.value
                and self._is_active(key)
            ):
                return ParseSubmission(
                    filename, False, True, False, ParseStatus.PENDING
                )

            if extension in self.TEXT_EXTENSIONS:
                return self._store_plaintext(
                    folder, version, filename, content, source_hash, size
                )

            if not DocumentExtractor.supports(extension):
                error = "Unsupported document type"
                self._store.upsert_document(
                    folder,
                    version,
                    filename=filename,
                    source_hash=source_hash,
                    parser="unsupported",
                    status=ParseStatus.FAILED.value,
                    size_bytes=size,
                    error=error,
                )
                return ParseSubmission(
                    filename, True, False, False, ParseStatus.FAILED, error
                )

            # Supported binary: retain bytes transiently and queue a parse.
            self._store.upsert_document(
                folder,
                version,
                filename=filename,
                source_hash=source_hash,
                parser="ai_parse_document",
                status=ParseStatus.PENDING.value,
                size_bytes=size,
                output_schema=self.OUTPUT_SCHEMA_VERSION,
                source_bytes=content,
            )
            self._set_active(key, True)
            return ParseSubmission(
                filename, True, False, True, ParseStatus.PENDING
            )

    def _store_plaintext(
        self,
        folder: str,
        version: str,
        filename: str,
        content: bytes,
        source_hash: str,
        size: int,
    ) -> ParseSubmission:
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            error = "Document is not valid UTF-8"
            self._store.upsert_document(
                folder,
                version,
                filename=filename,
                source_hash=source_hash,
                parser="plaintext",
                status=ParseStatus.FAILED.value,
                size_bytes=size,
                error=error,
            )
            return ParseSubmission(
                filename, True, False, False, ParseStatus.FAILED, error
            )
        self._store.upsert_document(
            folder,
            version,
            filename=filename,
            source_hash=source_hash,
            parser="plaintext",
            status=ParseStatus.READY.value,
            size_bytes=size,
            parsed_text=text,
        )
        return ParseSubmission(filename, True, False, False, ParseStatus.READY)

    # -- parse / retry ----------------------------------------------

    def parse_pending(
        self, folder: str, version: str, filename: str
    ) -> Dict[str, Any]:
        """Extract one pending binary and persist its parsed text."""
        filename = self._validate_filename(filename)
        key = self._key(folder, version, filename)
        row = self._store.get_document(folder, version, filename)
        if row is None:
            raise ValueError("Document has no parse record")
        if row.get("status") != ParseStatus.PENDING.value:
            return row

        try:
            content = self._store.read_document_bytes(folder, version, filename)
            if not content:
                self._store.set_document_failed(
                    folder,
                    version,
                    filename,
                    error="Original document is no longer available",
                )
                return self._store.get_document(folder, version, filename)

            try:
                parsed = (
                    self._extractor.extract_from_bytes(content)
                    if self._extractor is not None
                    else None
                )
            except Exception as exc:
                logger.warning(
                    "Document extraction raised %s for %s",
                    type(exc).__name__,
                    filename,
                )
                parsed = None

            if not parsed:
                self._store.set_document_failed(
                    folder, version, filename, error="Document parsing failed"
                )
                return self._store.get_document(folder, version, filename)

            self._store.set_document_ready(
                folder, version, filename, parsed_text=parsed
            )
            return self._store.get_document(folder, version, filename)
        finally:
            self._set_active(key, False)

    def retry(
        self, folder: str, version: str, filename: str
    ) -> ParseSubmission:
        """Return a failed/stale binary document to pending for re-parsing."""
        filename = self._validate_filename(filename)
        extension = DocumentExtractor.file_extension(filename)
        if not DocumentExtractor.supports(extension):
            raise ValueError("Only supported binary documents can be retried")
        key = self._key(folder, version, filename)

        with self._lock_for(key):
            row = self._store.get_document(folder, version, filename)
            if row is None:
                raise ValueError("Document not found")
            if row.get("status") == ParseStatus.READY.value:
                raise ValueError("Document is already parsed")
            if (
                row.get("status") == ParseStatus.PENDING.value
                and self._is_active(key)
            ):
                return ParseSubmission(
                    filename, False, True, False, ParseStatus.PENDING
                )
            content = self._store.read_document_bytes(folder, version, filename)
            if not content:
                raise ValueError(
                    "Original document is no longer available; re-upload it"
                )
            self._store.set_document_pending(folder, version, filename)
            self._set_active(key, True)
            return ParseSubmission(
                filename, False, False, True, ParseStatus.PENDING
            )

    # -- read / list / delete ---------------------------------------

    def read_document(
        self,
        folder: str,
        version: str,
        filename: str,
        max_chars: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Read ready corpus text without ever invoking the extractor."""
        filename = self._validate_filename(filename)
        result = self._store.read_document_text(folder, version, filename)
        if result is None:
            return {
                "filename": filename,
                "parse_status": ParseStatus.FAILED.value,
                "error": "Document has not been parsed; retry parsing",
            }
        parsed_text, parser, status = result
        if status != ParseStatus.READY.value:
            row = self._store.get_document(folder, version, filename) or {}
            return {
                "filename": filename,
                "parse_status": status,
                "error": row.get("error") or "Document parsing is not ready",
            }

        content = parsed_text or ""
        original_size = len(content)
        truncated = bool(max_chars is not None and original_size > max_chars)
        if truncated:
            content = (
                content[:max_chars]
                + f"\n\n[…truncated, {original_size} total chars]"
            )
        return {
            "filename": filename,
            "content": content,
            "size": original_size,
            "truncated": truncated,
            "parsed_with": parser,
            "parse_status": ParseStatus.READY.value,
        }

    def list_documents(
        self, folder: str, version: str
    ) -> List[Dict[str, Any]]:
        """Return document metadata rows (no text/bytes) with ``parse_status``."""
        rows = self._store.list_documents(folder, version)
        return [{**row, "parse_status": row.get("status")} for row in rows]

    def count_documents(self, folder: str, version: str) -> int:
        """Return the number of documents in ``(folder, version)``."""
        return self._store.count_documents(folder, version)

    def delete_documents(
        self, folder: str, version: str, filenames: List[str]
    ) -> List[str]:
        """Delete one or several documents; return per-item error messages."""
        valid = [self._validate_filename(f) for f in filenames]
        errors = self._store.delete_documents(folder, version, valid)
        for filename in valid:
            self._set_active(self._key(folder, version, filename), False)
        return errors
