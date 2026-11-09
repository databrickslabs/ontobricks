"""SHACL-AF rule layer: canonical rule model, IF-THEN transpiler, SQL engine.

This package replaces the raw IF-THEN execution path: IF/THEN rules authored in
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
from back.core.reasoning.shacl_rules.IfThenToShaclTranspiler import IfThenToShaclTranspiler

__all__ = [
    "ShaclRule",
    "ShaclRuleEngine",
    "ShaclRuleSqlTranslator",
    "IfThenToShaclTranspiler",
    "SPARQL_RULE",
    "TRIPLE_RULE",
]
