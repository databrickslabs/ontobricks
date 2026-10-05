"""Digital twin domain: triple-store query pipeline, R2RML augmentation, API helpers."""

from back.objects.digitaltwin.constants import RDF_TYPE, RDFS_LABEL
from back.objects.digitaltwin.models import DomainSnapshot
from back.objects.digitaltwin.CohortService import CohortService
from back.objects.digitaltwin.CohortEngineContext import CohortEngineContext
from back.objects.digitaltwin.DigitalTwin import DigitalTwin
from back.objects.digitaltwin.GraphFilter import GraphFilter
from back.objects.digitaltwin.GraphFind import GraphFind
from back.objects.digitaltwin.TwinStoreCache import TwinStoreCache
from back.objects.digitaltwin.SqlQualityChecks import SqlQualityChecks
from back.objects.digitaltwin.QualitySqlBuilder import QualitySqlBuilder
from back.objects.digitaltwin.TwinBackgroundTasks import TwinBackgroundTasks
from back.objects.digitaltwin.TwinMapping import TwinMapping
from back.objects.digitaltwin.TwinAnalytics import TwinAnalytics
from back.objects.digitaltwin.TwinResolve import TwinResolve
from back.objects.digitaltwin.TwinAssistantCache import TwinAssistantCache
from back.objects.digitaltwin.TwinNeighborTriples import TwinNeighborTriples
from back.objects.digitaltwin.TwinGraphAccess import TwinGraphAccess
from back.objects.digitaltwin.TwinGraphBuild import TwinGraphBuild
from back.objects.digitaltwin.TwinDataQualityRun import TwinDataQualityRun
from back.objects.digitaltwin.TwinOntologyGroups import TwinOntologyGroups
from back.objects.digitaltwin.TwinSparqlTranslate import TwinSparqlTranslate
from back.objects.digitaltwin.TwinLakehouseBuild import TwinLakehouseBuild
from back.objects.digitaltwin.TwinInferredMaterialize import TwinInferredMaterialize
from back.objects.digitaltwin.TwinGraphStats import TwinGraphStats
from back.objects.digitaltwin.NodeContextService import NodeContextService
from back.objects.digitaltwin.NodeBusinessRuleService import NodeBusinessRuleService
from back.objects.digitaltwin.VirtualAttributeService import VirtualAttributeService

__all__ = [
    "CohortEngineContext",
    "CohortService",
    "DigitalTwin",
    "DomainSnapshot",
    "GraphFilter",
    "GraphFind",
    "NodeBusinessRuleService",
    "NodeContextService",
    "QualitySqlBuilder",
    "SqlQualityChecks",
    "TwinAnalytics",
    "TwinAssistantCache",
    "TwinBackgroundTasks",
    "TwinDataQualityRun",
    "TwinGraphAccess",
    "TwinGraphBuild",
    "TwinGraphStats",
    "TwinInferredMaterialize",
    "TwinLakehouseBuild",
    "TwinMapping",
    "TwinOntologyGroups",
    "TwinSparqlTranslate",
    "TwinNeighborTriples",
    "TwinResolve",
    "TwinStoreCache",
    "VirtualAttributeService",
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


def run_inference_task(*a, **kw):
    return DigitalTwin.run_inference_task(*a, **kw)


def run_sql_checks(*a, **kw):
    return DigitalTwin.run_sql_checks(*a, **kw)


def set_ts_cache(domain, section, data):
    return DigitalTwin(domain).set_ts_cache(section, data)
