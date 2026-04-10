"""Data-transfer objects for the digital twin domain."""
from __future__ import annotations


class ProjectSnapshot:
    """Lightweight, thread-safe project snapshot.

    Copies all relevant project data so background threads do not depend
    on the (request-scoped) session after the HTTP request completes.

    This is the **single canonical snapshot class** — do not create local
    ``_Snap`` variants elsewhere; import and use this one instead.
    """
    def __init__(self, project):
        self.info = dict(project.info or {})
        self.databricks = dict(project.databricks or {})
        self.delta = dict(getattr(project, 'delta', None) or {})
        self.ladybug = dict(getattr(project, 'ladybug', None) or {})
        self.triplestore = dict(getattr(project, 'triplestore', None) or {})
        self.settings = dict(getattr(project, 'settings', None) or {})
        self.current_version = getattr(project, 'current_version', '1') or '1'

        ont = getattr(project, 'ontology', None)
        self.ontology = dict(ont) if isinstance(ont, dict) else {}

        gen = getattr(project, 'generated', None)
        self.generated = dict(gen) if isinstance(gen, dict) else {}
        self.generated_owl = self.generated.get('owl', '')

        self.assignment = dict(getattr(project, 'assignment', None) or {})
        self.constraints = list(getattr(project, 'constraints', None) or [])
        self.swrl_rules = list(getattr(project, 'swrl_rules', None) or [])
        self.axioms = list(getattr(project, 'axioms', None) or [])
        self.expressions = list(getattr(project, 'expressions', None) or [])
        self.shacl_shapes = list(getattr(project, 'shacl_shapes', None) or [])

        self._data = {
            'ontology': self.ontology,
            'generated': self.generated,
        }
