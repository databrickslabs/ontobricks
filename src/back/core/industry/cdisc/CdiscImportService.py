"""CDISC RDF import service.

Fetches CDISC (Clinical Data Interchange Standards Consortium) foundational
standards in RDF from the PhUSE GitHub repository, merges them using rdflib,
and converts them to OntoBricks ontology structures.

Because CDISC standards use an ISO 11179 meta-model (instances of
mms:DataElement, mms:Domain, mms:Context, etc.) rather than standard
owl:Class definitions, a custom mapper transforms the RDF instance data
into OntoBricks-compatible classes and properties:

    DomainContext  →  top-level OWL classes  (Events, Findings, …)
    Domain         →  OWL classes            (AE, CM, LB, …)
    DataElement    →  dataProperties          (AETERM, LBTEST, …)

Source repository: https://github.com/phuse-org/rdf.cdisc.org
CDISC Standard:   https://www.cdisc.org/standards/foundational/rdf/cdisc-standards-rdf
"""
import time
from typing import Dict, List, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from back.core.logging import get_logger
from shared.config.constants import HTTP_USER_AGENT
from rdflib import Graph, RDF, URIRef
from rdflib.namespace import SKOS

from back.core.helpers import extract_local_name as _extract_local
from back.core.industry.constants import (
    CDISC_BASE_URL_SCHEMAS,
    CDISC_BASE_URL_STD,
    MMS,
)

logger = get_logger(__name__)


class CdiscImportService:
    # ---------------------------------------------------------------------------
    # Human-readable labels for CDISC domain codes
    # ---------------------------------------------------------------------------

    _DOMAIN_LABELS: Dict[str, str] = {
        "AE": "Adverse Events",
        "CM": "Concomitant Medications",
        "CO": "Comments",
        "DA": "Drug Accountability",
        "DM": "Demographics",
        "DS": "Disposition",
        "DV": "Protocol Deviations",
        "EG": "ECG Test Results",
        "EX": "Exposure",
        "IE": "Inclusion/Exclusion Criteria",
        "LB": "Laboratory Tests",
        "MH": "Medical History",
        "PE": "Physical Examination",
        "SC": "Subject Characteristics",
        "SU": "Substance Use",
        "VS": "Vital Signs",
        "Common": "Common Variables",
        "Timing": "Timing Variables",
        "QS": "Questionnaires",
        "TI": "Trial Inclusion",
        "TV": "Trial Visits",
        "TA": "Trial Arms",
        "TE": "Trial Elements",
        "SE": "Subject Elements",
        "SV": "Subject Visits",
        "FA": "Findings About",
        "RS": "Disease Response",
        "TU": "Tumor Identification",
        "TR": "Tumor Results",
    }

    _DOMAIN_CONTEXT_LABELS: Dict[str, str] = {
        "Events": "Events Observation Class",
        "Findings": "Findings Observation Class",
        "FindingsAbout": "Findings About Observation Class",
        "Interventions": "Interventions Observation Class",
        "SpecialPurposeDomains": "Special Purpose Domains",
        "TrialDesign": "Trial Design Model",
        "CommonVariables": "Common Variables",
        "CommonTimingVariables": "Common Timing Variables",
        "Relationships": "Relationship Datasets",
    }

    # ---------------------------------------------------------------------------
    # CDISC Catalog – curated list of schemas and standard modules
    # ---------------------------------------------------------------------------

    CDISC_DOMAINS: Dict[str, Dict[str, Any]] = {
        "SCHEMAS": {
            "name": "Schemas (required)",
            "description": "Core OWL schemas: CDISC Meta-Model (ISO 11179 based), "
            "Controlled Terminology schema, and CDISC domain-specific schema. "
            "Required as the foundation for all other standards.",
            "icon": "bi-diagram-3",
            "color": "secondary",
            "required": True,
            "modules": [
                {
                    "url": f"{CDISC_BASE_URL_SCHEMAS}/meta-model-schema.owl",
                    "format": "xml",
                    "label": "Meta-Model Schema",
                },
                {
                    "url": f"{CDISC_BASE_URL_SCHEMAS}/ct-schema.owl",
                    "format": "xml",
                    "label": "CT Schema",
                },
                {
                    "url": f"{CDISC_BASE_URL_SCHEMAS}/cdisc-schema.owl",
                    "format": "xml",
                    "label": "CDISC Schema",
                },
            ],
        },
        "SDTM": {
            "name": "SDTM (Study Data Tabulation Model)",
            "description": "The primary standard for organizing and submitting clinical "
            "trial tabulation data. Includes SDTM v1.2, v1.3, and "
            "Implementation Guides 3.1.2 & 3.1.3.",
            "icon": "bi-table",
            "color": "primary",
            "required": False,
            "modules": [
                {
                    "url": f"{CDISC_BASE_URL_STD}/sdtm-1-2.ttl",
                    "format": "turtle",
                    "label": "SDTM 1.2",
                },
                {
                    "url": f"{CDISC_BASE_URL_STD}/sdtm-1-3.ttl",
                    "format": "turtle",
                    "label": "SDTM 1.3",
                },
                {
                    "url": f"{CDISC_BASE_URL_STD}/sdtmig-3-1-2.ttl",
                    "format": "turtle",
                    "label": "SDTM IG 3.1.2",
                },
                {
                    "url": f"{CDISC_BASE_URL_STD}/sdtmig-3-1-3.ttl",
                    "format": "turtle",
                    "label": "SDTM IG 3.1.3",
                },
            ],
        },
        "CDASH": {
            "name": "CDASH (Clinical Data Acquisition)",
            "description": "Standards for clinical data acquisition – defines the basic "
            "recommended data collection fields for studies.",
            "icon": "bi-clipboard2-data",
            "color": "success",
            "required": False,
            "modules": [
                {
                    "url": f"{CDISC_BASE_URL_STD}/cdash-1-1.ttl",
                    "format": "turtle",
                    "label": "CDASH 1.1",
                },
            ],
        },
        "SEND": {
            "name": "SEND (Nonclinical Data Exchange)",
            "description": "Standard for the Exchange of Nonclinical Data – covers "
            "preclinical/nonclinical study data.",
            "icon": "bi-virus",
            "color": "info",
            "required": False,
            "modules": [
                {
                    "url": f"{CDISC_BASE_URL_STD}/sendig-3-0.ttl",
                    "format": "turtle",
                    "label": "SEND IG 3.0",
                },
            ],
        },
        "ADaM": {
            "name": "ADaM (Analysis Data Model)",
            "description": "Defines standards for analysis datasets derived from SDTM "
            "for statistical analysis and regulatory submission.",
            "icon": "bi-bar-chart-line",
            "color": "warning",
            "required": False,
            "modules": [
                {
                    "url": f"{CDISC_BASE_URL_STD}/adam-2-1.ttl",
                    "format": "turtle",
                    "label": "ADaM 2.1",
                },
                {
                    "url": f"{CDISC_BASE_URL_STD}/adamig-1-0.ttl",
                    "format": "turtle",
                    "label": "ADaM IG 1.0",
                },
            ],
        },
    }

    _REQUEST_TIMEOUT = 30
    _MAX_WORKERS = 5

    @staticmethod
    def get_cdisc_catalog() -> List[Dict[str, Any]]:
        """Return the CDISC domain catalog for the frontend."""
        catalog = []
        for key, domain in CdiscImportService.CDISC_DOMAINS.items():
            catalog.append({
                "key": key,
                "name": domain["name"],
                "description": domain["description"],
                "icon": domain["icon"],
                "color": domain["color"],
                "required": domain.get("required", False),
                "module_count": len(domain["modules"]),
            })
        return catalog

    @staticmethod
    def _fetch_single_module(
        module: Dict[str, str],
    ) -> Tuple[str, Optional[str], Optional[str]]:
        """Fetch a single CDISC module from GitHub."""
        url = module["url"]
        label = module["label"]

        try:
            resp = requests.get(
                url,
                timeout=CdiscImportService._REQUEST_TIMEOUT,
                headers={"User-Agent": HTTP_USER_AGENT, "Accept": "*/*"},
                allow_redirects=True,
            )
            if resp.status_code == 200:
                text = resp.text.strip()
                if text.startswith("<!DOCTYPE") or text.startswith("<html"):
                    return label, None, f"{url} returned HTML instead of RDF"
                return label, text, None
            return label, None, f"{url} -> HTTP {resp.status_code}"
        except requests.exceptions.Timeout:
            return label, None, f"{url} -> timeout"
        except requests.exceptions.RequestException as exc:
            return label, None, f"{url} -> {exc}"

    @staticmethod
    def _collect_modules(domain_keys: List[str]) -> List[Dict[str, str]]:
        """Build a deduplicated list of CDISC modules for the selected domains.

        SCHEMAS is always included when any other domain is selected.
        """
        modules: list[Dict[str, str]] = []
        seen_urls: set[str] = set()

        keys_to_fetch = list(domain_keys)
        if any(k != "SCHEMAS" for k in keys_to_fetch) and "SCHEMAS" not in keys_to_fetch:
            keys_to_fetch.insert(0, "SCHEMAS")

        for key in keys_to_fetch:
            domain = CdiscImportService.CDISC_DOMAINS.get(key)
            if not domain:
                continue
            for mod in domain["modules"]:
                if mod["url"] not in seen_urls:
                    seen_urls.add(mod["url"])
                    modules.append(mod)

        return modules

    @staticmethod
    def _xsd_to_simple(xsd_type: str) -> str:
        """Map xsd type URIs to simple type names."""
        t = str(xsd_type).lower()
        if "string" in t:
            return "string"
        if "integer" in t or "int" in t or "positiveinteger" in t:
            return "integer"
        if "decimal" in t or "float" in t or "double" in t:
            return "decimal"
        if "boolean" in t:
            return "boolean"
        if "date" in t and "time" not in t:
            return "date"
        if "time" in t and "date" not in t:
            return "time"
        if "datetime" in t:
            return "dateTime"
        return "string"

    @staticmethod
    def _transform_cdisc_to_ontobricks(graph: Graph) -> Dict[str, Any]:
        """Transform CDISC RDF instance data into OntoBricks classes and properties.

        CDISC standards use different RDF patterns:

        CDASH:     DomainContext → Domain → DataElement (direct)
        SDTM IG:   DatasetContext → Dataset ← Column (→ DataElement)
        SEND IG:   DatasetContext → Dataset ← Column (with inline props)
        ADaM IG:   Dataset ← VariableGrouping ← Column (with inline props)
        SDTM base: VariableGrouping ← DataElement (generic templates)

        This mapper normalises all patterns into:
            Top-level class ← Domain/Dataset class ← dataProperties
        """
        base_uri = "http://rdf.cdisc.org/ontobricks#"

        # =====================================================================
        # Step 1: Collect top-level context classes
        #         Sources: DomainContext, DatasetContext
        # =====================================================================
        context_classes: Dict[str, Dict] = {}
        _seen_context_names: set = set()

        def _make_class(class_name: str, human_label: str, comment: str,
                        parent: str = "") -> Dict:
            return {
                "uri": f"{base_uri}{class_name}",
                "name": class_name,
                "label": human_label,
                "comment": comment,
                "emoji": "",
                "parent": parent,
                "dashboard": "",
                "dashboardParams": {},
                "dataProperties": [],
            }

        for rdf_type in (MMS.DomainContext, MMS.DatasetContext):
            for ctx_uri in graph.subjects(RDF.type, rdf_type):
                name = ""
                for n in graph.objects(ctx_uri, MMS.contextName):
                    name = str(n)
                if not name:
                    name = _extract_local(ctx_uri)

                class_name = name.replace(" ", "")
                if class_name in _seen_context_names:
                    # Reuse existing context under new URI
                    for existing_uri, existing_cls in context_classes.items():
                        if existing_cls["name"] == class_name:
                            context_classes[str(ctx_uri)] = existing_cls
                            break
                    continue
                _seen_context_names.add(class_name)

                human_label = CdiscImportService._DOMAIN_CONTEXT_LABELS.get(name, name)
                context_classes[str(ctx_uri)] = _make_class(
                    class_name, human_label, f"CDISC {human_label}"
                )

        # =====================================================================
        # Step 2: Collect domain / dataset classes
        #         Sources: mms:Domain, mms:Dataset
        # =====================================================================
        domain_classes: Dict[str, Dict] = {}
        _seen_domain_names: set = set()

        for rdf_type in (MMS.Domain, MMS.Dataset):
            for d_uri in graph.subjects(RDF.type, rdf_type):
                d_name = ""
                for n in graph.objects(d_uri, MMS.contextName):
                    d_name = str(n)
                if not d_name:
                    d_name = _extract_local(d_uri)

                if d_name in _seen_domain_names:
                    # Reuse existing domain under new URI
                    for existing_uri, existing_cls in domain_classes.items():
                        if existing_cls["name"] == d_name:
                            domain_classes[str(d_uri)] = existing_cls
                            break
                    continue
                _seen_domain_names.add(d_name)

                # Resolve parent context
                parent_name = ""
                for ctx_uri in graph.objects(d_uri, MMS.context):
                    ctx_str = str(ctx_uri)
                    if ctx_str in context_classes:
                        resolved = context_classes[ctx_str]["name"]
                        # Avoid self-referencing parent (ADaM pattern: Dataset
                        # and its DatasetContext share the same name)
                        if resolved != d_name:
                            parent_name = resolved
                        break

                human_label = CdiscImportService._DOMAIN_LABELS.get(d_name, d_name)
                desc = ""
                for dd in graph.objects(d_uri, MMS.contextDescription):
                    desc = str(dd)
                if not desc:
                    for dd in graph.objects(d_uri, SKOS.definition):
                        desc = str(dd)

                domain_classes[str(d_uri)] = _make_class(
                    d_name, human_label,
                    desc or f"CDISC domain {d_name} – {human_label}",
                    parent=parent_name,
                )

        # Build a VariableGrouping → parent Dataset lookup for ADaM-like patterns
        vg_to_dataset: Dict[str, str] = {}  # VG URI → Dataset URI
        for vg_uri in graph.subjects(RDF.type, MMS.VariableGrouping):
            for ctx_uri in graph.objects(vg_uri, MMS.context):
                if str(ctx_uri) in domain_classes:
                    vg_to_dataset[str(vg_uri)] = str(ctx_uri)
                    break

        # =====================================================================
        # Step 3: If no contexts/domains found from DomainContext/DatasetContext/
        #         Domain/Dataset, fall back to VariableGroupings as top-level
        #         classes (SDTM base model scenario).
        # =====================================================================
        has_structure = bool(context_classes) or bool(domain_classes)
        vg_as_toplevel: Dict[str, Dict] = {}

        if not has_structure:
            for vg_uri in graph.subjects(RDF.type, MMS.VariableGrouping):
                vg_name = _extract_local(vg_uri)
                if vg_name in _seen_context_names:
                    continue
                _seen_context_names.add(vg_name)
                human_label = CdiscImportService._DOMAIN_CONTEXT_LABELS.get(vg_name, vg_name)
                cls = _make_class(vg_name, human_label, f"CDISC {human_label}")
                vg_as_toplevel[str(vg_uri)] = cls
                context_classes[str(vg_uri)] = cls

        # =====================================================================
        # Step 4: Collect DataElement info
        # =====================================================================
        de_info: Dict[str, Dict] = {}
        for de_uri in graph.subjects(RDF.type, MMS.DataElement):
            de_name = ""
            for n in graph.objects(de_uri, MMS.dataElementName):
                de_name = str(n)
            if not de_name:
                de_name = _extract_local(de_uri)

            de_label = de_name
            for ll in graph.objects(de_uri, MMS.dataElementLabel):
                de_label = str(ll)

            de_desc = ""
            for dd in graph.objects(de_uri, MMS.dataElementDescription):
                de_desc = str(dd)

            de_type = "string"
            for tt in graph.objects(de_uri, MMS.dataElementType):
                de_type = CdiscImportService._xsd_to_simple(str(tt))

            de_info[str(de_uri)] = {
                "name": de_name,
                "label": de_label,
                "desc": de_desc,
                "type": de_type,
            }

        # =====================================================================
        # Step 5: Assign properties
        # =====================================================================
        properties_list: List[Dict] = []
        seen_props: set = set()

        def _add_data_prop(target_cls: Dict, de: Dict):
            """Register a data element as a property of a class."""
            domain_name = target_cls["name"]
            clean_name = de["name"].lstrip("-")
            prop_key = f"{domain_name}.{clean_name}"
            if prop_key in seen_props:
                return
            seen_props.add(prop_key)

            target_cls["dataProperties"].append({
                "name": clean_name,
                "localName": clean_name,
                "label": de["label"],
                "uri": f"{base_uri}{domain_name}/{clean_name}",
            })
            properties_list.append({
                "uri": f"{base_uri}{domain_name}/{clean_name}",
                "name": clean_name,
                "label": de["label"],
                "comment": de["desc"],
                "type": "DatatypeProperty",
                "domain": domain_name,
                "range": de["type"],
            })

        # 5a: Direct DataElement → Domain (CDASH pattern)
        for de_uri_str, de in de_info.items():
            for ctx_uri in graph.objects(URIRef(de_uri_str), MMS.context):
                ctx_str = str(ctx_uri)
                if ctx_str in domain_classes:
                    _add_data_prop(domain_classes[ctx_str], de)
                    break

        # 5b: Column-based assignment (SDTM IG / SEND IG / ADaM IG)
        for col_uri in graph.subjects(RDF.type, MMS.Column):
            # Read column-level info (preferred: available even without DE)
            col_name = ""
            for n in graph.objects(col_uri, MMS.dataElementName):
                col_name = str(n)
            col_label = col_name
            for ll in graph.objects(col_uri, MMS.dataElementLabel):
                col_label = str(ll)
            col_desc = ""
            for dd in graph.objects(col_uri, MMS.dataElementDescription):
                col_desc = str(dd)
            col_type = "string"
            for tt in graph.objects(col_uri, MMS.dataElementType):
                val = str(tt)
                if val.startswith("xsd:") or "XMLSchema" in val:
                    col_type = CdiscImportService._xsd_to_simple(val)

            # Resolve which dataset/domain this column belongs to
            target_cls = None
            for ctx_uri in graph.objects(col_uri, MMS.context):
                ctx_str = str(ctx_uri)
                # Direct: Column → Dataset/Domain
                if ctx_str in domain_classes:
                    target_cls = domain_classes[ctx_str]
                    break
                # Indirect: Column → VariableGrouping → Dataset
                if ctx_str in vg_to_dataset:
                    ds_uri = vg_to_dataset[ctx_str]
                    if ds_uri in domain_classes:
                        target_cls = domain_classes[ds_uri]
                        break

            if not target_cls or not col_name:
                continue

            # Build DE info: prefer column-level, fall back to DataElement ref
            de_ref_info = None
            for de_ref in graph.objects(col_uri, MMS.dataElement):
                de_ref_info = de_info.get(str(de_ref))
                break

            col_de = {
                "name": col_name,
                "label": col_label or (de_ref_info["label"] if de_ref_info else col_name),
                "desc": col_desc or (de_ref_info["desc"] if de_ref_info else ""),
                "type": col_type,
            }
            _add_data_prop(target_cls, col_de)

        # 5c: DataElement → VariableGrouping (SDTM base model fallback)
        if vg_as_toplevel:
            for de_uri_str, de in de_info.items():
                clean = de["name"].lstrip("-")
                already = any(f".{clean}" in k for k in seen_props)
                if already:
                    continue
                for ctx_uri in graph.objects(URIRef(de_uri_str), MMS.context):
                    ctx_str = str(ctx_uri)
                    if ctx_str in vg_as_toplevel:
                        _add_data_prop(vg_as_toplevel[ctx_str], de)
                        break

        # =====================================================================
        # Step 6: Deduplicate and assemble
        # =====================================================================
        # Collect unique classes.  When a context and a domain share the
        # same name (e.g. ADaM DatasetContext "ADAE" and Dataset "ADAE"),
        # merge the domain's dataProperties into the context class.
        name_to_cls: Dict[str, Dict] = {}

        for cls in context_classes.values():
            name = cls["name"]
            if name not in name_to_cls:
                name_to_cls[name] = cls

        for cls in domain_classes.values():
            name = cls["name"]
            if name in name_to_cls:
                # Merge dataProperties from the domain into the existing class
                existing = name_to_cls[name]
                existing_dp_names = {
                    dp["name"] for dp in existing["dataProperties"]
                }
                for dp in cls["dataProperties"]:
                    if dp["name"] not in existing_dp_names:
                        existing["dataProperties"].append(dp)
                        existing_dp_names.add(dp["name"])
                # Inherit parent if the existing class has none
                # Guard against self-referencing parent
                if not existing["parent"] and cls["parent"] and cls["parent"] != name:
                    existing["parent"] = cls["parent"]
            else:
                name_to_cls[name] = cls

        all_classes = list(name_to_cls.values())

        for cls in all_classes:
            cls["dataProperties"] = sorted(
                cls["dataProperties"], key=lambda x: x["name"]
            )

        all_classes.sort(key=lambda c: (0 if c["parent"] == "" else 1, c["name"]))
        properties_list.sort(key=lambda p: (p["domain"], p["name"]))

        return {
            "classes": all_classes,
            "properties": properties_list,
        }

    @staticmethod
    def fetch_and_parse_cdisc(
        domain_keys: List[str],
    ) -> Dict[str, Any]:
        """Fetch CDISC modules for the selected domains, merge, and parse.

        For SCHEMAS-only imports, uses the standard OWL parser.
        When standard data files are included (SDTM, CDASH, etc.), uses the
        custom CDISC mapper to transform RDF instances into classes/properties.
        """
        start = time.time()
        modules = CdiscImportService._collect_modules(domain_keys)

        if not modules:
            return {
                "success": False,
                "message": "No valid CDISC domains selected.",
                "turtle": "",
                "stats": {},
                "fetched": 0,
                "failed": [],
            }

        logger.info("Fetching %d modules for domains: %s", len(modules), domain_keys)

        # Fetch modules concurrently
        merged_graph = Graph()
        fetched_count = 0
        failed_modules: list[str] = []

        with ThreadPoolExecutor(max_workers=CdiscImportService._MAX_WORKERS) as pool:
            futures = {
                pool.submit(CdiscImportService._fetch_single_module, mod): mod
                for mod in modules
            }
            for future in as_completed(futures):
                label, content, error = future.result()
                if content:
                    mod = futures[future]
                    fmt = mod["format"]
                    try:
                        merged_graph.parse(data=content, format=fmt)
                        fetched_count += 1
                        logger.info("OK  %s", label)
                    except Exception as parse_err:
                        failed_modules.append(label)
                        logger.warning("PARSE_ERR  %s: %s", label, parse_err)
                else:
                    failed_modules.append(label)
                    logger.warning("FAIL %s: %s", label, error)

        elapsed = time.time() - start
        logger.info("Fetched %d/%d modules in %.1fs", fetched_count, len(modules), elapsed)

        if fetched_count == 0:
            hint = (
                "Could not reach the CDISC RDF GitHub repository. "
                "If you are running in Databricks Apps, outbound internet access "
                "may be restricted. Try downloading CDISC TTL/OWL files manually "
                "from https://github.com/phuse-org/rdf.cdisc.org and importing "
                "via 'Import OWL'."
            )
            return {
                "success": False,
                "message": hint,
                "turtle": "",
                "stats": {},
                "fetched": 0,
                "failed": failed_modules,
            }

        # Determine which parsing strategy to use
        has_standard_data = any(k != "SCHEMAS" for k in domain_keys)

        if has_standard_data:
            # Use custom CDISC mapper for instance data
            mapped = CdiscImportService._transform_cdisc_to_ontobricks(merged_graph)
            classes = mapped["classes"]
            properties = mapped["properties"]
            constraints: List[Dict] = []
            swrl_rules: List[Dict] = []
            axioms: List[Dict] = []
            expressions: List[Dict] = []

            # Build ontology info from the first standard ontology found
            ontology_info = {
                "uri": "http://rdf.cdisc.org/ontobricks",
                "label": "CDISC",
                "comment": "CDISC Foundational Standards",
                "namespace": "http://rdf.cdisc.org/ontobricks#",
            }
            # Try to get a better name from the graph
            from rdflib import OWL as OWL_NS
            for onto_uri in merged_graph.subjects(RDF.type, OWL_NS.Ontology):
                for lbl in merged_graph.objects(onto_uri, SKOS.prefLabel):
                    ontology_info["label"] = str(lbl)
                    break
                break
        else:
            # Schemas-only: use the generic OWL parser
            turtle_content = merged_graph.serialize(format="turtle")
            from back.objects.ontology import Ontology
            result = Ontology.parse_owl(turtle_content, extract_advanced=True)
            ontology_info, classes, properties, constraints, swrl_rules, axioms, expressions, _groups = result

        # Count relationships (ObjectProperty) vs attributes (DatatypeProperty)
        relationships = [p for p in properties if p.get("type") == "ObjectProperty"]
        attributes = [p for p in properties if p.get("type") != "ObjectProperty"]

        stats = {
            "classes": len(classes),
            "properties": len(properties),
            "relationships": len(relationships),
            "attributes": len(attributes),
            "constraints": len(constraints) if isinstance(constraints, list) else 0,
            "modules_fetched": fetched_count,
            "modules_failed": len(failed_modules),
        }

        domain_names = ", ".join(
            CdiscImportService.CDISC_DOMAINS[k]["name"] for k in domain_keys if k in CdiscImportService.CDISC_DOMAINS
        )
        # Build a readable summary, omitting zero counts
        parts = [f"{stats['classes']} classes"]
        if stats["relationships"] > 0:
            parts.append(f"{stats['relationships']} relationships")
        if stats["attributes"] > 0:
            parts.append(f"{stats['attributes']} attributes")
        msg = (
            f"CDISC imported: {', '.join(parts)} "
            f"from {domain_names} ({fetched_count} modules loaded"
        )
        if failed_modules:
            msg += f", {len(failed_modules)} modules unavailable"
        msg += f") in {elapsed:.1f}s"

        logger.info("%s", msg)

        return {
            "success": True,
            "message": msg,
            "turtle": merged_graph.serialize(format="turtle"),
            "ontology_info": ontology_info,
            "classes": classes,
            "properties": properties,
            "constraints": constraints,
            "swrl_rules": swrl_rules,
            "axioms": axioms,
            "expressions": expressions,
            "stats": stats,
            "fetched": fetched_count,
            "failed": failed_modules,
        }
