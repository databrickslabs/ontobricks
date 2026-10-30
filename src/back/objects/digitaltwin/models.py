"""Data-transfer objects for the knowledge graph domain."""

from __future__ import annotations


class DomainSnapshot:
    """Lightweight, thread-safe domain session snapshot.

    Copies all relevant domain data so background threads do not depend
    on the (request-scoped) session after the HTTP request completes.

    This is the **single canonical snapshot class** — do not create local
    ``_Snap`` variants elsewhere; import and use this one instead.
    """

    def __init__(self, domain, *, host: str = "", token: str = ""):
        self.info = dict(domain.info or {})
        raw_db = dict(domain.databricks or {})
        raw_db.pop("token", None)
        if host:
            raw_db["host"] = host
        if token:
            raw_db["token"] = token
        self.databricks = raw_db
        self.delta = dict(getattr(domain, "delta", None) or {})
        self.triplestore = dict(getattr(domain, "triplestore", None) or {})
        self.settings = dict(getattr(domain, "settings", None) or {})
        self.current_version = getattr(domain, "current_version", "1") or "1"

        ont = getattr(domain, "ontology", None)
        self.ontology = dict(ont) if isinstance(ont, dict) else {}

        gen = getattr(domain, "generated", None)
        self.generated = dict(gen) if isinstance(gen, dict) else {}
        self.generated_owl = self.generated.get("owl", "")

        self.assignment = dict(getattr(domain, "assignment", None) or {})
        self.constraints = list(getattr(domain, "constraints", None) or [])
        self.swrl_rules = list(getattr(domain, "swrl_rules", None) or [])
        self.axioms = list(getattr(domain, "axioms", None) or [])
        self.expressions = list(getattr(domain, "expressions", None) or [])
        self.shacl_shapes = list(getattr(domain, "shacl_shapes", None) or [])
        self._classes = self._copy_classes(domain)
        if not self.swrl_rules:
            self.swrl_rules = list(self.ontology.get("swrl_rules") or [])
        if not self.ontology.get("classes") and self._classes:
            self.ontology["classes"] = list(self._classes)
        getter = getattr(domain, "get_properties", None)
        if callable(getter) and not self.ontology.get("properties"):
            try:
                props = getter()
            except Exception:
                props = []
            if isinstance(props, list) and props:
                self.ontology["properties"] = list(props)

        self._data = {
            "ontology": self.ontology,
            "generated": self.generated,
        }

    @staticmethod
    def _copy_classes(domain) -> list:
        getter = getattr(domain, "get_classes", None)
        if not callable(getter):
            return []
        try:
            classes = getter()
        except Exception:
            return []
        return list(classes) if isinstance(classes, list) else []

    def get_classes(self) -> list:
        """Ontology classes captured at snapshot time."""
        return list(self._classes)
