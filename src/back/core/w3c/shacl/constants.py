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

RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
