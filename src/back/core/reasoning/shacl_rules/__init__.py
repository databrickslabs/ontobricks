"""SHACL-AF rule layer: canonical rule model, SWRL transpiler, SQL engine.

This package replaces the raw SWRL execution path: IF/THEN rules authored in
the UI are transpiled to canonical SHACL-AF rules and executed at SQL scale
against the flat triple-store table, preserving the legacy engine's result and
stats contract.
"""

from back.core.reasoning.shacl_rules.ShaclRule import (
    ShaclRule,
    SPARQL_RULE,
    TRIPLE_RULE,
)
from back.core.reasoning.shacl_rules.ShaclRuleEngine import ShaclRuleEngine
from back.core.reasoning.shacl_rules.ShaclRuleSqlTranslator import (
    ShaclRuleSqlTranslator,
)
from back.core.reasoning.shacl_rules.SwrlToShaclTranspiler import SwrlToShaclTranspiler

__all__ = [
    "ShaclRule",
    "ShaclRuleEngine",
    "ShaclRuleSqlTranslator",
    "SwrlToShaclTranspiler",
    "SPARQL_RULE",
    "TRIPLE_RULE",
]
