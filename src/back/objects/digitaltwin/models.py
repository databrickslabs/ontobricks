"""Data-transfer objects for the digital twin domain."""
from __future__ import annotations


class DomainSnapshot:
    """Lightweight, thread-safe domain session snapshot.

    Copies all relevant domain data so background threads do not depend
    on the (request-scoped) session after the HTTP request completes.

    This is the **single canonical snapshot class** — do not create local
    ``_Snap`` variants elsewhere; import and use this one instead.
    """
    def __init__(self, domain):
        self.info = dict(domain.info or {})
        self.databricks = dict(domain.databricks or {})
        self.delta = dict(getattr(domain, 'delta', None) or {})
        self.ladybug = dict(getattr(domain, 'ladybug', None) or {})
        self.triplestore = dict(getattr(domain, 'triplestore', None) or {})
        self.settings = dict(getattr(domain, 'settings', None) or {})
        self.current_version = getattr(domain, 'current_version', '1') or '1'

        ont = getattr(domain, 'ontology', None)
        self.ontology = dict(ont) if isinstance(ont, dict) else {}

        gen = getattr(domain, 'generated', None)
        self.generated = dict(gen) if isinstance(gen, dict) else {}
        self.generated_owl = self.generated.get('owl', '')

        self.assignment = dict(getattr(domain, 'assignment', None) or {})
        self.constraints = list(getattr(domain, 'constraints', None) or [])
        self.swrl_rules = list(getattr(domain, 'swrl_rules', None) or [])
        self.axioms = list(getattr(domain, 'axioms', None) or [])
        self.expressions = list(getattr(domain, 'expressions', None) or [])
        self.shacl_shapes = list(getattr(domain, 'shacl_shapes', None) or [])

        self._data = {
            'ontology': self.ontology,
            'generated': self.generated,
        }
