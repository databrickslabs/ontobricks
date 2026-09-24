"""Document text extraction via Databricks ``ai_parse_document``.

Databricks-specific: converts in-memory binary documents (PDF, Office, images)
into text using the ``ai_parse_document`` SQL function on a SQL warehouse — no
Unity Catalog Volume involved. The bytes are base64-encoded and inlined into
the SQL statement, then decoded warehouse-side with ``unbase64``. Generic and
reusable across the app (agents, services, jobs); it has no dependency on the
agent layer.

Typical use::

    extractor = DocumentExtractor(host=host, token=token, warehouse_id=wh_id)
    if extractor.is_available() and DocumentExtractor.supports(ext):
        text = extractor.extract_from_bytes(raw_pdf_bytes)

Output schema is pinned to v2.0 (verified live): text lives in
``document.elements[].content`` (figures expose an AI ``description``);
``document.pages[]`` only holds ``id``/``image_uri``.
"""

import base64
import json
from typing import Any, Dict, Optional

from back.core.logging import get_logger
from .DatabricksClient import DatabricksClient

logger = get_logger(__name__)


class DocumentExtractor:
    """Extract text from binary documents via Databricks ``ai_parse_document``.

    Accepts either a :class:`DatabricksClient` directly **or** the legacy
    ``(host, token, warehouse_id)`` signature.
    """

    # File extensions that are NOT plain text and require parsing (a SQL
    # warehouse). Plain-text extensions should be read directly by the caller.
    SUPPORTED_EXTENSIONS = {"pdf", "png", "jpg", "jpeg", "docx", "doc", "pptx", "ppt"}

    # ai_parse_document output schema version this extractor targets.
    OUTPUT_SCHEMA_VERSION = "2.0"

    def __init__(
        self,
        host: Optional[str] = None,
        token: Optional[str] = None,
        warehouse_id: Optional[str] = None,
        *,
        client: Optional[DatabricksClient] = None,
    ) -> None:
        if client is not None:
            self._client = client
        else:
            self._client = DatabricksClient(
                host=host, token=token, warehouse_id=warehouse_id
            )

    @classmethod
    def from_credentials(
        cls, host: str, token: str, warehouse_id: str
    ) -> "DocumentExtractor":
        """Build an extractor from raw credentials (resolves the client lazily)."""
        from .DatabricksClient import DatabricksClient as _Client

        return cls(client=_Client(host=host, token=token, warehouse_id=warehouse_id))

    @classmethod
    def supports(cls, extension: str) -> bool:
        """Return *True* when *extension* (without dot) can be parsed."""
        return (extension or "").lower() in cls.SUPPORTED_EXTENSIONS

    @staticmethod
    def file_extension(filename: str) -> str:
        """Return the lower-case extension (without dot), or '' if none."""
        _, _, ext = (filename or "").rpartition(".")
        return ext.lower() if ext and ext != filename else ""

    def is_available(self) -> bool:
        """Return *True* when a SQL warehouse is configured (parsing needs one)."""
        return bool(getattr(self._client, "warehouse_id", ""))

    def extract_from_bytes(self, content: bytes) -> Optional[str]:
        """Parse in-memory document *content* to text — no UC Volume involved.

        The bytes are base64-encoded and inlined into the SQL statement, then
        decoded warehouse-side with ``unbase64`` and fed straight to
        ``ai_parse_document``. This is the Volume-free path used by the
        Knowledge Store (callers must enforce the upload size cap so the
        statement text stays within the Statement Execution API limit).

        Returns the extracted text, or ``None`` when no SQL warehouse is
        configured or parsing fails / yields nothing.
        """
        if not self.is_available():
            logger.info("DocumentExtractor: no SQL warehouse — cannot parse bytes")
            return None
        if not content:
            return None

        raw = self._run_query_bytes(content)
        if not raw:
            return None
        return self._text_from_raw(raw)

    # -- internals ---------------------------------------------------

    def _text_from_raw(self, raw: Any) -> Optional[str]:
        """Turn an ``ai_parse_document`` JSON payload into text (or ``None``)."""
        try:
            parsed = json.loads(raw) if isinstance(raw, str) else raw
        except (ValueError, TypeError) as exc:
            logger.warning("DocumentExtractor: bad parsed JSON: %s", exc)
            return None
        text = self.extract_text_from_parsed(parsed) if isinstance(parsed, dict) else ""
        if not text:
            logger.info("DocumentExtractor: no text extracted from parsed payload")
            return None
        return text

    def _run_query_bytes(self, content: bytes) -> Optional[str]:
        b64 = base64.b64encode(content).decode("ascii")
        query = (
            "SELECT to_json(ai_parse_document(unbase64('"
            f"{b64}'), map('version', '{self.OUTPUT_SCHEMA_VERSION}'))) AS parsed"
        )
        try:
            rows = self._client.execute_query(query)
        except Exception as exc:
            logger.warning("DocumentExtractor: warehouse parse failed: %s", exc)
            return None
        if not rows:
            logger.info("DocumentExtractor: no rows returned for inline parse")
            return None
        return rows[0].get("parsed")

    @staticmethod
    def extract_text_from_parsed(parsed: Dict[str, Any]) -> str:
        """Pull readable text out of an ``ai_parse_document`` result.

        On schema version 2.0 the text lives in ``document.elements[].content``
        (figures carry an AI ``description`` instead); ``document.pages[]`` only
        holds ``id``/``image_uri``. Prefer the elements path and keep
        page-content / markdown-blob fallbacks for other schema versions.
        """
        document = parsed.get("document") or {}

        elements = document.get("elements") or []
        el_texts = [
            (e.get("content") or e.get("description") or e.get("text") or "")
            for e in elements
            if isinstance(e, dict)
        ]
        el_texts = [t for t in el_texts if t]
        if el_texts:
            return "\n\n".join(el_texts)

        pages = document.get("pages") or []
        page_texts = [p.get("content", "") for p in pages if isinstance(p, dict)]
        page_texts = [t for t in page_texts if t]
        if page_texts:
            return "\n\n".join(page_texts)

        for key in ("markdown", "content", "text"):
            blob = document.get(key) or parsed.get(key)
            if blob:
                return blob
        return ""
