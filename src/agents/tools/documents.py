"""
Document tools – used by the OWL generator agent.

Provides tools to list and read documents from a Unity Catalog volume.
"""

import json
from typing import Callable, Dict, List, Optional

import requests

from back.core.logging import get_logger
from agents.tools.context import ToolContext

logger = get_logger(__name__)

_TOOL_TIMEOUT = 30
_MAX_DOC_CHARS = 80_000  # Increased to allow more context for mapping decisions


def _headers(ctx: ToolContext) -> dict:
    return {"Authorization": f"Bearer {ctx.token}"}


def _volume_docs_path(ctx: ToolContext) -> Optional[str]:
    reg = ctx.registry
    if not reg or not reg.get("catalog") or not reg.get("volume"):
        logger.debug("_volume_docs_path: missing registry fields — reg=%s", reg)
        return None
    from back.objects.registry import RegistryCfg

    c = RegistryCfg.from_dict(reg)
    folder = ctx.domain_folder or ""
    if not folder:
        from back.objects.session.DomainSession import sanitize_domain_folder

        folder = sanitize_domain_folder(ctx.domain_name or "untitled_domain")
    version = ctx.domain_version or "1"
    from back.objects.registry.service import _DOMAINS_FOLDER

    path = f"/Volumes/{c.catalog}/{c.schema}/{c.volume}/{_DOMAINS_FOLDER}/{folder}/V{version}/documents"
    logger.debug("_volume_docs_path: resolved to %s", path)
    return path


# =====================================================
# Tool implementations
# =====================================================


def tool_list_documents(ctx: ToolContext, **_kwargs) -> str:
    """List documents available in the domain UC volume."""
    logger.info("tool_list_documents: listing documents in domain volume")
    base_path = _volume_docs_path(ctx)
    if not base_path:
        logger.info("tool_list_documents: no UC location configured — returning error")
        return json.dumps({"error": "Domain not saved to Unity Catalog"})

    url = f"{ctx.host}/api/2.0/fs/directories{base_path}"
    logger.info("tool_list_documents: GET %s", base_path)
    logger.debug("tool_list_documents: full url=%s", url)
    try:
        resp = requests.get(url, headers=_headers(ctx), timeout=_TOOL_TIMEOUT)
        logger.debug(
            "tool_list_documents: response status=%d, size=%d bytes",
            resp.status_code,
            len(resp.content),
        )
        if resp.status_code == 404:
            logger.info("tool_list_documents: documents directory not found (404)")
            return json.dumps({"files": [], "message": "No documents directory yet"})
        resp.raise_for_status()
        entries = resp.json().get("contents", [])
        logger.debug("tool_list_documents: raw entries count=%d", len(entries))
        files = [
            {
                "name": e.get("name", e.get("path", "").split("/")[-1]),
                "size": e.get("file_size"),
            }
            for e in entries
            if not e.get("is_directory", False)
        ]
        logger.info("tool_list_documents: found %d file(s)", len(files))
        logger.debug("tool_list_documents: files=%s", [f["name"] for f in files])
        return json.dumps({"files": files, "count": len(files)})
    except Exception as exc:
        logger.error("tool_list_documents: request failed: %s", exc)
        return json.dumps({"error": str(exc)})


def tool_read_document(ctx: ToolContext, *, filename: str = "", **_kwargs) -> str:
    """Read the text content of a document from the domain volume."""
    logger.info("tool_read_document: reading '%s'", filename)
    if not filename:
        logger.warning("tool_read_document: called without filename parameter")
        return json.dumps({"error": "filename is required"})

    base_path = _volume_docs_path(ctx)
    if not base_path:
        logger.info("tool_read_document: no UC location configured — returning error")
        return json.dumps({"error": "Domain not saved to Unity Catalog"})

    file_path = f"{base_path}/{filename}"
    url = f"{ctx.host}/api/2.0/fs/files{file_path}"
    logger.info("tool_read_document: GET %s", file_path)
    logger.debug("tool_read_document: full url=%s", url)
    try:
        resp = requests.get(url, headers=_headers(ctx), timeout=60)
        logger.debug(
            "tool_read_document: response status=%d, content_type=%s, size=%d bytes",
            resp.status_code,
            resp.headers.get("content-type", "?"),
            len(resp.content),
        )
        resp.raise_for_status()
        try:
            content = resp.content.decode("utf-8")
        except UnicodeDecodeError:
            logger.warning(
                "tool_read_document: '%s' is binary (decode failed) — %d bytes",
                filename,
                len(resp.content),
            )
            return json.dumps(
                {"filename": filename, "error": "Binary file – cannot read as text"}
            )

        original_len = len(content)
        truncated = original_len > _MAX_DOC_CHARS
        if truncated:
            logger.info(
                "tool_read_document: '%s' truncated %d → %d chars (limit=%d)",
                filename,
                original_len,
                _MAX_DOC_CHARS,
                _MAX_DOC_CHARS,
            )
            content = (
                content[:_MAX_DOC_CHARS]
                + f"\n\n[…truncated, {original_len} total chars]"
            )
        logger.info(
            "tool_read_document: '%s' read OK — %d chars, truncated=%s",
            filename,
            original_len,
            truncated,
        )
        logger.debug(
            "tool_read_document: '%s' content preview (300 chars): %.300s",
            filename,
            content,
        )
        return json.dumps(
            {
                "filename": filename,
                "content": content,
                "size": original_len,
                "truncated": truncated,
            }
        )
    except requests.exceptions.HTTPError as exc:
        logger.error(
            "tool_read_document: HTTP error for '%s': status=%s, body=%.300s",
            filename,
            exc.response.status_code if exc.response is not None else "?",
            exc.response.text[:300] if exc.response is not None else "N/A",
        )
        return json.dumps({"error": str(exc)})
    except Exception as exc:
        logger.error("tool_read_document: unexpected error for '%s': %s", filename, exc)
        return json.dumps({"error": str(exc)})


_MAX_DOCS_IN_CONTEXT = 10
_MAX_TOTAL_DOC_CHARS = 150_000


def tool_get_documents_context(ctx: ToolContext, **_kwargs) -> str:
    """Return pre-loaded document content from imported domain documents.
    Does NOT query Unity Catalog — uses documents loaded at agent start.
    Limited to avoid context overflow when many/large documents are loaded."""
    logger.info(
        "tool_get_documents_context: returning %d pre-loaded document(s)",
        len(ctx.documents),
    )
    if not ctx.documents:
        return json.dumps(
            {
                "documents": [],
                "message": "No documents were loaded. Upload documents in Domain → Documents to enrich mapping context.",
            }
        )
    result = []
    total_chars = 0
    for d in ctx.documents[:_MAX_DOCS_IN_CONTEXT]:
        content = d.get("content", "")
        if total_chars + len(content) > _MAX_TOTAL_DOC_CHARS:
            remaining = _MAX_TOTAL_DOC_CHARS - total_chars
            if remaining > 5000:
                content = (
                    content[:remaining]
                    + f"\n\n[…truncated, document has {len(d.get('content', ''))} chars total]"
                )
                result.append(
                    {
                        "name": d.get("name", "?"),
                        "content": content,
                        "size": len(content),
                    }
                )
                total_chars = _MAX_TOTAL_DOC_CHARS
            break
        result.append(
            {"name": d.get("name", "?"), "content": content, "size": len(content)}
        )
        total_chars += len(content)
    truncated = len(ctx.documents) > len(result) or total_chars < sum(
        len(d.get("content", "")) for d in ctx.documents
    )
    out = {"documents": result, "count": len(result), "total_chars": total_chars}
    if truncated:
        out["_message"] = (
            f"Showing first {len(result)} document(s), {total_chars} chars total (limit to avoid context overflow)."
        )
    logger.info(
        "tool_get_documents_context: returning %d doc(s), %d total chars%s",
        len(result),
        total_chars,
        " (truncated)" if truncated else "",
    )
    return json.dumps(out)


# =====================================================
# OpenAI function-calling definitions
# =====================================================

GET_DOCUMENTS_CONTEXT_DEF = {
    "type": "function",
    "function": {
        "name": "get_documents_context",
        "description": (
            "Get the domain's imported documents (context loaded at agent start). "
            "Use this to enrich domain knowledge for mapping decisions. "
            "Does NOT query Unity Catalog."
        ),
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
}

DOCUMENT_TOOL_DEFINITIONS: List[dict] = [
    {
        "type": "function",
        "function": {
            "name": "list_documents",
            "description": (
                "List all documents in the domain's Unity Catalog volume. "
                "Call this first to discover available documents before reading them."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_document",
            "description": (
                "Read the text content of a document from the domain volume. "
                "Supports .txt, .csv, .json, .md and similar text formats."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "filename": {
                        "type": "string",
                        "description": "File name to read, e.g. 'business_rules.txt'",
                    }
                },
                "required": ["filename"],
            },
        },
    },
]

DOCUMENT_TOOL_HANDLERS: Dict[str, Callable] = {
    "list_documents": tool_list_documents,
    "read_document": tool_read_document,
    "get_documents_context": tool_get_documents_context,
}
