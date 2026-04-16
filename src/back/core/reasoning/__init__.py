"""Reasoning engine — OWL 2 RL inference, SWRL rule execution, graph reasoning,
decision tables, SPARQL CONSTRUCT rules, and aggregate rules."""

# ── New (PascalCase) module imports ──────────────────────────────────────
from back.core.reasoning.models import (  # noqa: F401
    InferredTriple,
    ReasoningResult,
    RuleViolation,
    SWRLAtomPartition,
)
from back.core.reasoning.ReasoningService import ReasoningService  # noqa: F401
from back.core.reasoning.OWLRLReasoner import OWLRLReasoner  # noqa: F401
from back.core.reasoning.SWRLEngine import SWRLEngine  # noqa: F401
from back.core.reasoning.SWRLSQLTranslator import SWRLSQLTranslator  # noqa: F401
from back.core.reasoning.SWRLCypherTranslator import SWRLCypherTranslator  # noqa: F401
from back.core.reasoning.SWRLFlatCypherTranslator import SWRLFlatCypherTranslator  # noqa: F401
from back.core.reasoning.SWRLBuiltinRegistry import (  # noqa: F401
    SWRLBuiltin,
    SWRLBuiltinRegistry,
)
from back.core.reasoning.SWRLParser import SWRLParser  # noqa: F401
from back.core.reasoning.DecisionTableEngine import DecisionTableEngine  # noqa: F401
from back.core.reasoning.SPARQLRuleEngine import SPARQLRuleEngine  # noqa: F401
from back.core.reasoning.AggregateRuleEngine import AggregateRuleEngine  # noqa: F401

# ── Backward-compatible wrappers for the old function-based API ──────────
# Callers that imported `get_builtin`, `is_builtin`, etc. from the package
# or from `swrl_builtins` continue to work via these thin wrappers.

get_builtin = SWRLBuiltinRegistry.get
is_builtin = SWRLBuiltinRegistry.is_builtin
all_builtins = SWRLBuiltinRegistry.all
builtins_by_category = SWRLBuiltinRegistry.by_category

__all__ = [
    "AggregateRuleEngine",
    "DecisionTableEngine",
    "InferredTriple",
    "OWLRLReasoner",
    "ReasoningResult",
    "ReasoningService",
    "RuleViolation",
    "SPARQLRuleEngine",
    "SWRLAtomPartition",
    "SWRLBuiltin",
    "SWRLBuiltinRegistry",
    "SWRLCypherTranslator",
    "SWRLEngine",
    "SWRLFlatCypherTranslator",
    "SWRLParser",
    "SWRLSQLTranslator",
    "all_builtins",
    "builtins_by_category",
    "get_builtin",
    "is_builtin",
]
