"""OntoBricks eXport (.obx) file format helpers.

The ``.obx`` file is plain JSON used by the Registry → Browse page to export
and import one or several domains in a single payload. This module owns the
envelope schema, version compatibility contract, and validation helpers used
by the export/import routes.

Envelope shape (current version)::

    {
        "format_version": 1,
        "ontobricks_version": "0.4.0",
        "min_ontobricks_version": "0.4.0",   # optional
        "exported_at": "2026-05-16T17:00:00Z",
        "exported_by": "user@example.com",
        "domains": [
            {
                "name": "claims",
                "info": { ... },             # informational, from latest exported version
                "versions": {
                    "1": { ... },            # full RegistryService.read_version() doc
                    "2": { ... }
                }
            }
        ]
    }

Compatibility contract
----------------------

* ``format_version`` is an **integer** and is the sole file-format signature.
  No separate ``"format"`` string is used.
* Bump ``CURRENT_OBX_FORMAT_VERSION`` only when the envelope or per-version
  payload changes shape in a way old code cannot read transparently.
* When bumping, write an ``_upgrade_vN_to_vN+1(env)`` function and register
  it in :data:`_UPGRADERS`. :func:`load` will run the upgrade chain
  automatically on import.
* Export always writes :data:`CURRENT_OBX_FORMAT_VERSION`. There is no
  downgrade path — files exported by a newer build cannot be loaded by an
  older build (the importer raises a clear "please upgrade" error).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from back.core.errors import ValidationError
from shared.config.constants import APP_VERSION

CURRENT_OBX_FORMAT_VERSION: int = 1
"""Current envelope schema version. Bump when the shape changes."""


_UPGRADERS: Dict[int, Optional[Callable[[Dict[str, Any]], Dict[str, Any]]]] = {
    # version -> function that upgrades from this version to the next.
    # The current version maps to ``None`` (nothing further to do).
    # Example for a future v2 bump:
    #     1: _upgrade_v1_to_v2,
    #     2: None,
    1: None,
}


def _parse_version_tuple(version: str) -> tuple:
    """Parse a dotted version string into a tuple of ints for comparison.

    Non-numeric segments are dropped so pre-release tags like ``0.4.0rc1``
    still compare cleanly. Returns ``(0,)`` for an empty or unparseable
    string so the gate is permissive on malformed values.
    """
    if not version:
        return (0,)
    parts: List[int] = []
    for raw in str(version).split("."):
        digits = ""
        for ch in raw:
            if ch.isdigit():
                digits += ch
            else:
                break
        if digits:
            parts.append(int(digits))
        else:
            break
    return tuple(parts) if parts else (0,)


def build_envelope(
    domains: List[Dict[str, Any]],
    *,
    exported_by: str = "",
    min_ontobricks_version: str = "",
) -> Dict[str, Any]:
    """Wrap a list of exported *domains* into a fully-formed envelope.

    Args:
        domains: List of ``{"name": str, "info": dict, "versions": {str: dict}}``
            entries, one per exported domain.
        exported_by: Email or display name of the user triggering the export.
        min_ontobricks_version: Optional gate; importers running an older
            OntoBricks build will refuse the file. Leave empty when no gate
            is needed.

    Returns:
        The envelope dict ready to be JSON-serialised.
    """
    envelope: Dict[str, Any] = {
        "format_version": CURRENT_OBX_FORMAT_VERSION,
        "ontobricks_version": APP_VERSION,
        "exported_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "exported_by": exported_by or "",
        "domains": domains,
    }
    if min_ontobricks_version:
        envelope["min_ontobricks_version"] = min_ontobricks_version
    return envelope


def load(envelope: Any) -> Dict[str, Any]:
    """Validate *envelope* and upgrade it to the current format version.

    Raises:
        ValidationError: when the envelope is not a dict, is missing
            ``format_version``, declares a non-integer/negative
            ``format_version``, asks for a newer OntoBricks build via
            ``min_ontobricks_version``, or declares a ``format_version``
            higher than :data:`CURRENT_OBX_FORMAT_VERSION`.

    Returns:
        A normalised envelope at :data:`CURRENT_OBX_FORMAT_VERSION`.
    """
    if not isinstance(envelope, dict):
        raise ValidationError("Invalid .obx file: top-level JSON must be an object")

    if "format_version" not in envelope:
        raise ValidationError(
            "Invalid .obx file: missing required 'format_version' field"
        )

    raw_version = envelope.get("format_version")
    if not isinstance(raw_version, int) or isinstance(raw_version, bool) or raw_version < 1:
        raise ValidationError(
            f"Invalid .obx file: 'format_version' must be a positive integer "
            f"(got {raw_version!r})"
        )

    min_required = envelope.get("min_ontobricks_version", "")
    if min_required:
        if _parse_version_tuple(APP_VERSION) < _parse_version_tuple(min_required):
            raise ValidationError(
                f"This .obx file requires OntoBricks >= {min_required} "
                f"(running {APP_VERSION}). Please upgrade OntoBricks before importing."
            )

    if raw_version > CURRENT_OBX_FORMAT_VERSION:
        producer = envelope.get("ontobricks_version") or "unknown"
        raise ValidationError(
            f"Unsupported .obx format_version={raw_version}; please upgrade "
            f"OntoBricks (file was created by ontobricks_version={producer}). "
            f"This build only understands format_version<={CURRENT_OBX_FORMAT_VERSION}."
        )

    current = envelope
    version = raw_version
    while version < CURRENT_OBX_FORMAT_VERSION:
        upgrader = _UPGRADERS.get(version)
        if upgrader is None:
            raise ValidationError(
                f"No upgrade path registered from .obx format_version={version} "
                f"to {CURRENT_OBX_FORMAT_VERSION}"
            )
        current = upgrader(current)
        version += 1
        current["format_version"] = version

    if not isinstance(current.get("domains"), list):
        raise ValidationError("Invalid .obx file: 'domains' must be a list")

    return current
