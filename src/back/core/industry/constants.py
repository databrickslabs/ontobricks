"""Shared URL constants and RDF namespaces for industry ontology imports."""

from rdflib import Namespace

CDISC_BASE_URL_SCHEMAS = (
    "https://raw.githubusercontent.com/phuse-org/rdf.cdisc.org/master/schemas"
)
CDISC_BASE_URL_STD = (
    "https://raw.githubusercontent.com/phuse-org/rdf.cdisc.org/master/std"
)

FIBO_BASE_URL = "https://spec.edmcouncil.org/fibo/ontology/master/latest"

IOF_BASE_URL = "https://raw.githubusercontent.com/iofoundry/ontology/master"

MMS = Namespace("http://rdf.cdisc.org/mms#")
