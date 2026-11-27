"""The kind of a Complex (SPARQL) rule, read from its query.

A Complex rule is either a ``CONSTRUCT`` (infers triples) or a ``DELETE``
(hides triples). The query decides: its first keyword after any ``PREFIX`` /
``BASE`` prologue. No separate ``kind`` is stored or sent, so the editor, the
assistant and the engine cannot disagree about what a rule does.

``ontology-business-rules.js`` (``_sparqlKindOf``) mirrors this rule; the
parity test in ``tests/units/reasoning/test_sparql_rule_kind.py`` keeps them
in step.
"""

import re
from typing import Optional

RULE_KIND_CONSTRUCT = "construct"
RULE_KIND_REMOVAL = "removal"

NOT_A_RULE_MESSAGE = (
    "A Complex rule must start with CONSTRUCT (infers triples) or DELETE (hides triples)"
)

_PROLOGUE_RE = re.compile(
    r"\s*(?:(?:PREFIX\s+[^\s<]*\s*<[^>]*>|BASE\s*<[^>]*>)\s*)*",
    re.IGNORECASE,
)
_FORM_RE = re.compile(r"(CONSTRUCT|DELETE)\b", re.IGNORECASE)


def sparql_rule_kind(query: Optional[str]) -> Optional[str]:
    """``construct``, ``removal``, or None when the query is neither form."""
    text = query or ""
    head = text[_PROLOGUE_RE.match(text).end():]
    form = _FORM_RE.match(head)
    if form is None:
        return None
    return RULE_KIND_CONSTRUCT if form.group(1).upper() == "CONSTRUCT" else RULE_KIND_REMOVAL
