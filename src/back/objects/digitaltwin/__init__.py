"""Digital twin domain: triple-store query pipeline, R2RML augmentation, API helpers."""

from back.objects.digitaltwin.constants import RDF_TYPE, RDFS_LABEL
from back.objects.digitaltwin.models import ProjectSnapshot
from back.objects.digitaltwin.digitaltwin import DigitalTwin

__all__ = [
    "DigitalTwin",
    "ProjectSnapshot",
    "RDF_TYPE",
    "RDFS_LABEL",
    "augment_mappings_from_config",
    "augment_relationships_from_config",
    "build_quality_sql",
    "classify_predicates",
    "complete_dq_task",
    "effective_backend_label",
    "execute_spark_query",
    "get_ts_cache",
    "is_owlrl_available",
    "run_graph_checks",
    "run_sql_checks",
    "set_ts_cache",
]


# ---------------------------------------------------------------------------
# Backward-compatible module-level wrappers
# ---------------------------------------------------------------------------

def augment_mappings_from_config(*a, **kw):
    return DigitalTwin.augment_mappings_from_config(*a, **kw)

def augment_relationships_from_config(*a, **kw):
    return DigitalTwin.augment_relationships_from_config(*a, **kw)

def build_quality_sql(*a, **kw):
    return DigitalTwin.build_quality_sql(*a, **kw)

def classify_predicates(top_predicates, project):
    return DigitalTwin(project).classify_predicates(top_predicates)

def complete_dq_task(*a, **kw):
    return DigitalTwin.complete_dq_task(*a, **kw)

def effective_backend_label(project):
    return DigitalTwin(project).effective_backend_label()

def execute_spark_query(sparql_query, r2rml_content, limit, project, settings):
    return DigitalTwin(project).execute_spark_query(sparql_query, r2rml_content, limit, settings)

def get_ts_cache(project, section):
    return DigitalTwin(project).get_ts_cache(section)

def is_owlrl_available():
    return DigitalTwin.is_owlrl_available()

def run_graph_checks(*a, **kw):
    return DigitalTwin.run_graph_checks(*a, **kw)

def run_sql_checks(*a, **kw):
    return DigitalTwin.run_sql_checks(*a, **kw)

def set_ts_cache(project, section, data):
    return DigitalTwin(project).set_ts_cache(section, data)
