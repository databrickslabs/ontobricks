"""
Document tools – used by the OWL generator agent.

Provides tools to list and read the domain's parsed documents from the
Lakebase Knowledge Store (no Unity Catalog Volume).
"""

import json
from typing import Callable, Dict, List, Optional, Tuple

from back.core.logging import get_logger
from back.core.databricks import DocumentParseService
from agents.tools.context import ToolContext

logger = get_logger(__name__)

_MAX_DOC_CHARS = 80_000  # Increased to allow more context for mapping decisions


def _document_scope(
    ctx: ToolContext,
) -> Optional[Tuple[DocumentParseService, str, str]]:
    """Resolve ``(service, folder, version)`` for the agent's Knowledge Store.

    Returns ``None`` when the domain is not saved to a registry yet.
    """
    reg = ctx.registry
    if not reg or not reg.get("catalog"):
        logger.debug("_document_scope: missing registry fields — reg=%s", reg)
        return None
    from back.objects.registry import RegistryCfg
    from back.objects.registry.store import RegistryFactory

    cfg = RegistryCfg.from_dict(reg)
    folder = ctx.domain_folder or ""
    if not folder:
        from back.objects.session.DomainSession import sanitize_domain_folder

        folder = sanitize_domain_folder(ctx.domain_name or "untitled_domain")
    version = ctx.domain_version or "1"
    store = RegistryFactory.from_cfg(cfg)
    return DocumentParseService(store), folder, version


# =====================================================
# Tool implementations
# =====================================================


def tool_list_documents(ctx: ToolContext, **_kwargs) -> str:
    """List documents available in the domain's Knowledge Store."""
    logger.info("tool_list_documents: listing Knowledge Store documents")
    scope = _document_scope(ctx)
    if scope is None:
        logger.info("tool_list_documents: no registry configured — returning error")
        return json.dumps({"error": "Domain not saved to the registry"})

    service, folder, version = scope
    try:
        rows = service.list_documents(folder, version)
        files = []
        for row in rows:
            name = row.get("filename") or ""
            if not name:
                continue
            item = {
                "name": name,
                "size": row.get("size_bytes"),
                "parse_status": row.get("parse_status"),
                "parser": row.get("parser"),
            }
            if row.get("error"):
                item["parse_error"] = row.get("error")
            files.append(item)
        logger.info("tool_list_documents: found %d file(s)", len(files))
        return json.dumps({"files": files, "count": len(files)})
    except Exception as exc:
        logger.error("tool_list_documents: request failed: %s", exc)
        return json.dumps({"error": str(exc)})


def tool_read_document(ctx: ToolContext, *, filename: str = "", **_kwargs) -> str:
    """Read one ready document from the Knowledge Store parsed corpus."""
    logger.info("tool_read_document: reading '%s'", filename)
    if not filename:
        logger.warning("tool_read_document: called without filename parameter")
        return json.dumps({"error": "filename is required"})

    scope = _document_scope(ctx)
    if scope is None:
        logger.info("tool_read_document: no registry configured — returning error")
        return json.dumps({"error": "Domain not saved to the registry"})

    service, folder, version = scope
    try:
        payload = service.read_document(
            folder,
            version,
            filename,
            max_chars=_MAX_DOC_CHARS,
        )
        return json.dumps(payload)
    except Exception as exc:
        logger.error("tool_read_document: unexpected error for '%s': %s", filename, exc)
        return json.dumps({"filename": filename, "error": str(exc)})


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
                "message": "No documents were loaded. Upload documents in Domain → Knowledge Store to enrich mapping context.",
            }
        )
    result = []
    unavailable = []
    total_chars = 0
    for d in ctx.documents[:_MAX_DOCS_IN_CONTEXT]:
        status = d.get("parse_status", "ready")
        if status != "ready":
            unavailable.append(
                {
                    "name": d.get("name", "?"),
                    "parse_status": status,
                    "error": d.get("error") or "Document parsing is not ready",
                }
            )
            continue
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
    if unavailable:
        out["unavailable_documents"] = unavailable
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
                "List all documents in the domain's Knowledge Store. "
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
                "Read a ready document from the domain's Knowledge Store corpus. "
                "Returns parse_status=pending or failed when text is unavailable. "
                "This tool never starts document parsing."
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
