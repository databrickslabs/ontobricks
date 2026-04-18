"""Digital twin domain: triple-store query pipeline, R2RML augmentation, API helpers."""

from back.objects.digitaltwin.constants import RDF_TYPE, RDFS_LABEL
from back.objects.digitaltwin.models import DomainSnapshot
from back.objects.digitaltwin.DigitalTwin import DigitalTwin

__all__ = [
    "DigitalTwin",
    "DomainSnapshot",
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
    "run_build_task",
    "run_data_quality_task",
    "run_graph_checks",
    "run_inference_task",
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


def classify_predicates(top_predicates, domain):
    return DigitalTwin(domain).classify_predicates(top_predicates)


def complete_dq_task(*a, **kw):
    return DigitalTwin.complete_dq_task(*a, **kw)


def effective_backend_label(domain):
    return DigitalTwin(domain).effective_backend_label()


def execute_spark_query(sparql_query, r2rml_content, limit, domain, settings):
    return DigitalTwin(domain).execute_spark_query(
        sparql_query, r2rml_content, limit, settings
    )


def get_ts_cache(domain, section):
    return DigitalTwin(domain).get_ts_cache(section)


def is_owlrl_available():
    return DigitalTwin.is_owlrl_available()


def run_build_task(*a, **kw):
    return DigitalTwin.run_build_task(*a, **kw)


def run_data_quality_task(*a, **kw):
    return DigitalTwin.run_data_quality_task(*a, **kw)


def run_graph_checks(*a, **kw):
    return DigitalTwin.run_graph_checks(*a, **kw)


def run_inference_task(*a, **kw):
    return DigitalTwin.run_inference_task(*a, **kw)


def run_sql_checks(*a, **kw):
    return DigitalTwin.run_sql_checks(*a, **kw)


def set_ts_cache(domain, section, data):
    return DigitalTwin(domain).set_ts_cache(section, data)
