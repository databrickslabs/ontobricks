"""Reasoning engine — OWL 2 RL inference, SHACL-AF rule execution, graph
reasoning, decision tables, SPARQL CONSTRUCT rules, and aggregate rules.

IF/THEN business rules authored in the UI (SWRL-style syntax) are transpiled
to canonical SHACL-AF rules and executed by :mod:`back.core.reasoning.shacl_rules`.
"""

# ── Results ──────────────────────────────────────────────────────────────
from back.core.reasoning.results.InferredTriple import InferredTriple  # noqa: F401
from back.core.reasoning.results.ReasoningResult import ReasoningResult  # noqa: F401
from back.core.reasoning.results.RuleViolation import RuleViolation  # noqa: F401

# ── Core services ────────────────────────────────────────────────────────
from back.core.reasoning.ReasoningService import ReasoningService  # noqa: F401
from back.core.reasoning.OWLRLReasoner import OWLRLReasoner  # noqa: F401

# ── SHACL-AF rule layer (absorbed former SWRL lib) ──────────────────────
from back.core.reasoning.shacl_rules.RuleAtomPartition import (  # noqa: F401
    RuleAtomPartition,
)
from back.core.reasoning.shacl_rules.RuleBuiltin import RuleBuiltin  # noqa: F401
from back.core.reasoning.shacl_rules.RuleBuiltinRegistry import (  # noqa: F401
    RuleBuiltinRegistry,
)
from back.core.reasoning.shacl_rules.RuleExpressionParser import (  # noqa: F401
    RuleExpressionParser,
)
from back.core.reasoning.shacl_rules.ShaclFlatSqlBackend import (  # noqa: F401
    ShaclFlatSqlBackend,
)
from back.core.reasoning.shacl_rules.ShaclRule import ShaclRule  # noqa: F401
from back.core.reasoning.shacl_rules.ShaclRuleEngine import ShaclRuleEngine  # noqa: F401
from back.core.reasoning.shacl_rules.ShaclRuleSqlTranslator import (  # noqa: F401
    ShaclRuleSqlTranslator,
)
from back.core.reasoning.shacl_rules.SwrlToShaclTranspiler import (  # noqa: F401
    SwrlToShaclTranspiler,
)
from back.core.reasoning.shacl_rules.RuleTextCodec import (  # noqa: F401
    parse_rules as parse_swrl_text,
    serialize_rules as serialize_swrl_text,
)

# ── Other rule engines ──────────────────────────────────────────────────
from back.core.reasoning.rules.DecisionTableEngine import DecisionTableEngine  # noqa: F401
from back.core.reasoning.rules.SPARQLRuleEngine import SPARQLRuleEngine  # noqa: F401
from back.core.reasoning.rules.AggregateRuleEngine import AggregateRuleEngine  # noqa: F401

# ── Convenience wrappers for the function-based builtin API ─────────────
get_builtin = RuleBuiltinRegistry.get
is_builtin = RuleBuiltinRegistry.is_builtin
all_builtins = RuleBuiltinRegistry.all
builtins_by_category = RuleBuiltinRegistry.by_category

__all__ = [
    "AggregateRuleEngine",
    "DecisionTableEngine",
    "InferredTriple",
    "OWLRLReasoner",
    "ReasoningResult",
    "ReasoningService",
    "RuleAtomPartition",
    "RuleBuiltin",
    "RuleBuiltinRegistry",
    "RuleExpressionParser",
    "RuleViolation",
    "SPARQLRuleEngine",
    "ShaclFlatSqlBackend",
    "ShaclRule",
    "ShaclRuleEngine",
    "ShaclRuleSqlTranslator",
    "SwrlToShaclTranspiler",
    "all_builtins",
    "builtins_by_category",
    "get_builtin",
    "is_builtin",
    "parse_swrl_text",
    "serialize_swrl_text",
]
