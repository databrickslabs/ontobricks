"""SHACL-related constants shared by parser, generator, and service."""

from typing import Dict

from rdflib import Namespace
from rdflib.namespace import XSD

SH = Namespace("http://www.w3.org/ns/shacl#")

PARAM_CATEGORY_HINTS: Dict[str, str] = {
    str(SH.minCount): "completeness",
    str(SH.maxCount): "cardinality",
    str(SH.pattern): "conformance",
    str(SH.flags): "conformance",
    str(SH.hasValue): "conformance",
    str(SH["in"]): "conformance",
    str(SH.minInclusive): "conformance",
    str(SH.maxInclusive): "conformance",
    str(SH.minExclusive): "conformance",
    str(SH.maxExclusive): "conformance",
    str(SH.minLength): "conformance",
    str(SH.maxLength): "conformance",
    str(SH["class"]): "consistency",
    str(SH.node): "consistency",
    str(SH.datatype): "consistency",
}

SEVERITY_REVERSE = {
    str(SH.Violation): "sh:Violation",
    str(SH.Warning): "sh:Warning",
    str(SH.Info): "sh:Info",
}

QUALITY_CATEGORIES = (
    "completeness",
    "cardinality",
    "uniqueness",
    "consistency",
    "conformance",
    "structural",
)

#: A data quality run also executes rule families that are not SHACL shapes.
#: Their results are filed under these dimensions, so a run must honour the
#: same dimension selection the shapes do.
SWRL_CATEGORY = "structural"
BUSINESS_RULE_CATEGORY = "conformance"

#: Non-SHACL rules have no shape id, so a run addresses them by a synthetic
#: one. The list endpoint, the run selection and the reported result must all
#: derive it the same way or a rule becomes unselectable.
SWRL_ID_PREFIX = "swrl"
DECISION_TABLE_ID_PREFIX = "dt"
AGGREGATE_ID_PREFIX = "agg"

RULE_FAMILY_CATEGORIES = {
    SWRL_ID_PREFIX: SWRL_CATEGORY,
    DECISION_TABLE_ID_PREFIX: BUSINESS_RULE_CATEGORY,
    AGGREGATE_ID_PREFIX: BUSINESS_RULE_CATEGORY,
}


def rule_check_id(prefix: str, rule: Dict, index: int) -> str:
    """Return the check id a non-SHACL *rule* is selected and reported under."""
    return f"{prefix}:{rule.get('name', index)}"

SEVERITY_MAP = {
    "sh:Violation": SH.Violation,
    "sh:Warning": SH.Warning,
    "sh:Info": SH.Info,
}

DATATYPE_MAP = {
    "string": XSD.string,
    "integer": XSD.integer,
    "int": XSD.integer,
    "decimal": XSD.decimal,
    "float": XSD.float,
    "double": XSD.double,
    "boolean": XSD.boolean,
    "date": XSD.date,
    "dateTime": XSD.dateTime,
    "time": XSD.time,
    "anyURI": XSD.anyURI,
}

XSD_TO_SPARK_TYPE = {
    "string": None,
    "xsd:string": None,
    "integer": "INT",
    "xsd:integer": "INT",
    "int": "INT",
    "xsd:int": "INT",
    "long": "BIGINT",
    "xsd:long": "BIGINT",
    "decimal": "DECIMAL(38,10)",
    "xsd:decimal": "DECIMAL(38,10)",
    "float": "FLOAT",
    "xsd:float": "FLOAT",
    "double": "DOUBLE",
    "xsd:double": "DOUBLE",
    "boolean": "BOOLEAN",
    "xsd:boolean": "BOOLEAN",
    "date": "DATE",
    "xsd:date": "DATE",
    "dateTime": "TIMESTAMP",
    "xsd:dateTime": "TIMESTAMP",
}

from back.core.graphdb.constants import RDFS_LABEL  # noqa: F401
