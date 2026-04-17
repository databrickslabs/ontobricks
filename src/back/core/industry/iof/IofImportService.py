"""IOF import service.

Fetches IOF (Industrial Ontologies Foundry) domain ontology modules from the
IOF GitHub repository, merges them using rdflib, and converts them to
OntoBricks ontology structures via the existing OWL parser.

IOF is a suite of OWL ontologies for digital manufacturing published by OAGi.
All IOF ontologies are based on BFO (Basic Formal Ontology) and share a
common Core ontology.

Because IOF ontologies define most relationships through OWL restrictions
(owl:someValuesFrom / owl:allValuesFrom inside rdfs:subClassOf or
owl:equivalentClass) rather than explicit rdfs:domain/rdfs:range on
properties, a post-processing step extracts these restriction-based
relationships so they appear correctly in OntoBricks.

Reference: https://github.com/iofoundry/ontology
"""
import time
from typing import Dict, List, Any, Optional, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from back.core.errors import InfrastructureError, ValidationError
from back.core.logging import get_logger
from shared.config.constants import HTTP_USER_AGENT
from rdflib import Graph, RDF, RDFS, OWL, BNode, URIRef

from back.core.helpers import extract_local_name as _extract_local_name
from back.core.industry.constants import IOF_BASE_URL

logger = get_logger(__name__)


class IofImportService:
    # ---------------------------------------------------------------------------
    # IOF Catalog – curated list of domain modules
    # ---------------------------------------------------------------------------

    IOF_DOMAINS: Dict[str, Dict[str, Any]] = {
        "CORE": {
            "name": "Core",
            "description": "Common manufacturing concepts: agents, processes, capabilities, "
            "organizations, products, and business functions. Foundation for "
            "all other IOF domains.",
            "icon": "bi-gear-wide-connected",
            "color": "primary",
            "required": True,
            "modules": [
                {"path": "core/meta/AnnotationVocabulary.rdf", "label": "Annotation Vocabulary"},
                {"path": "core/Core.rdf", "label": "IOF Core"},
            ],
        },
        "MAINTENANCE": {
            "name": "Maintenance",
            "description": "Maintenance management, procedures, asset failure analysis, "
            "and failure modes and effects analysis (FMEA).",
            "icon": "bi-wrench-adjustable",
            "color": "warning",
            "required": False,
            "modules": [
                {"path": "maintenance/Maintenance.rdf", "label": "Maintenance"},
            ],
        },
        "SUPPLYCHAIN": {
            "name": "Supply Chain",
            "description": "Supply chain and logistics concepts: procurement, "
            "transportation, warehousing, and distribution.",
            "icon": "bi-truck",
            "color": "success",
            "required": False,
            "modules": [
                {"path": "supplychain/SupplyChain.rdf", "label": "Supply Chain"},
            ],
        },
    }

    _REQUEST_TIMEOUT = 30   # seconds per module
    _MAX_WORKERS = 5        # concurrent download threads

    # Hardcoded labels for common BFO / RO properties whose rdfs:label is
    # not present in the IOF modules (the full BFO ontology is not loaded).
    _BFO_PROPERTY_LABELS: Dict[str, str] = {
        "BFO_0000050": "partOf",
        "BFO_0000051": "hasPart",
        "BFO_0000054": "realizedIn",
        "BFO_0000055": "realizes",
        "BFO_0000056": "participatesInAtSomeTime",
        "BFO_0000057": "hasParticipantAtSomeTime",
        "BFO_0000066": "occursIn",
        "BFO_0000067": "containsProcess",
        "BFO_0000084": "genericallyDependsOn",
        "BFO_0000101": "hasMemberPartAtSomeTime",
        "BFO_0000108": "existsAt",
        "BFO_0000110": "hasPartAtSomeTime",
        "BFO_0000111": "precedes",
        "BFO_0000117": "hasRealization",
        "BFO_0000121": "hasTempPartFrom",
        "BFO_0000124": "locatedInAtAllTimes",
        "BFO_0000129": "memberPartOfAtSomeTime",
        "BFO_0000132": "occurrentPartOf",
        "BFO_0000153": "temporallyProjectsOnto",
        "BFO_0000169": "specificallyDependsOn",
        "BFO_0000176": "partOfContinuantAtAllTimes",
        "BFO_0000177": "partOfOccurrentAtAllTimes",
        "BFO_0000178": "hasContinuantPartAtAllTimes",
        "BFO_0000196": "bearerOf",
        "BFO_0000197": "inheresIn",
        "BFO_0000199": "occupiesTemporalRegion",
        "BFO_0000200": "occupiesSpatialRegion",
        "BFO_0000210": "hasFunction",
        "BFO_0000215": "hasDisposition",
        "BFO_0000218": "hasRole",
        "BFO_0000219": "roleOf",
        "BFO_0000221": "functionOf",
        "BFO_0000223": "dispositionOf",
        "BFO_0000307": "isAbout",
        "RO_0000052": "inheresIn",
        "RO_0000053": "bearerOf",
        "RO_0000056": "participatesIn",
        "RO_0000057": "hasParticipant",
        "RO_0000079": "functionOf",
        "RO_0000080": "qualityOf",
        "RO_0000081": "roleOf",
        "RO_0000085": "hasFunction",
        "RO_0000086": "hasQuality",
        "RO_0000087": "hasRole",
        "RO_0000091": "hasDisposition",
        "RO_0001000": "derivesFrom",
        "RO_0001001": "derivesInto",
        "RO_0002353": "output_of",
        "IAO_0000136": "isAbout",
    }

    @staticmethod
    def get_iof_catalog() -> List[Dict[str, Any]]:
        """Return the IOF domain catalog for the frontend.

        Returns:
            list[dict]: Each entry has key, name, description, icon, color,
                        required, and module_count.
        """
        catalog = []
        for key, domain in IofImportService.IOF_DOMAINS.items():
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
        """Fetch a single IOF module from GitHub.

        Args:
            module: dict with 'path' and 'label' keys.

        Returns:
            (label, content_or_None, error_or_None)
        """
        path = module["path"]
        label = module["label"]
        url = f"{IOF_BASE_URL}/{path}"

        try:
            resp = requests.get(
                url,
                timeout=IofImportService._REQUEST_TIMEOUT,
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
        """Build a deduplicated list of IOF modules for the selected domains.

        CORE is always included when any other domain is selected because all
        IOF domain ontologies depend on the Core ontology.
        """
        modules: list[Dict[str, str]] = []
        seen_paths: set[str] = set()

        keys_to_fetch = list(domain_keys)
        if any(k != "CORE" for k in keys_to_fetch) and "CORE" not in keys_to_fetch:
            keys_to_fetch.insert(0, "CORE")

        for key in keys_to_fetch:
            domain = IofImportService.IOF_DOMAINS.get(key)
            if not domain:
                continue
            for mod in domain["modules"]:
                if mod["path"] not in seen_paths:
                    seen_paths.add(mod["path"])
                    modules.append(mod)

        return modules

    _property_label_cache: Dict[str, str] = {}

    @staticmethod
    def _resolve_property_label(graph: Graph, prop_uri: str) -> str:
        """Return a human-readable label for a property URI.

        Resolution order:
          1. Cache hit
          2. rdfs:label in the merged graph
          3. Hardcoded BFO/RO label dictionary
          4. Local name from URI (last resort)
        """
        _cache = IofImportService._property_label_cache
        if prop_uri in _cache:
            return _cache[prop_uri]

        local_name = _extract_local_name(prop_uri)
        label = ""

        # Try rdfs:label in the graph
        for lbl in graph.objects(URIRef(prop_uri), RDFS.label):
            text = str(lbl).strip()
            if text:
                label = text
                break

        # Fallback to hardcoded BFO dictionary
        if not label:
            label = IofImportService._BFO_PROPERTY_LABELS.get(local_name, "")

        # Last resort: use URI local name
        if not label:
            label = local_name

        # Convert space-separated labels to camelCase
        if " " in label:
            parts = label.split()
            label = parts[0].lower() + "".join(w.capitalize() for w in parts[1:])

        _cache[prop_uri] = label
        return label

    @staticmethod
    def _extract_relationships_from_restrictions(
        graph: Graph,
        known_class_names: Set[str],
    ) -> List[Dict[str, Any]]:
        """Extract relationships from OWL restrictions on classes.

        IOF/BFO ontologies encode most class-to-class relationships through
        OWL restrictions rather than rdfs:domain/rdfs:range.  For example::

            :Carrier rdfs:subClassOf [
                a owl:Restriction ;
                owl:onProperty :hasRole ;
                owl:someValuesFrom :TransportationServiceProviderRole
            ] .

        This function scans `rdfs:subClassOf` and `owl:equivalentClass` axioms
        for restrictions that use `owl:someValuesFrom` or `owl:allValuesFrom`,
        and produces ObjectProperty entries when both ends are known classes.
        """
        relationships: List[Dict[str, Any]] = []
        seen: set = set()

        def _try_add(class_name: str, prop_name: str, target_name: str,
                     prop_uri: str):
            """Add a relationship if both ends are known and not yet seen."""
            if class_name in known_class_names and target_name in known_class_names:
                # Deduplicate on URI + endpoints to avoid duplicates when
                # the same BFO property resolves to different names
                key = f"{class_name}|{prop_uri}|{target_name}"
                if key not in seen:
                    seen.add(key)
                    relationships.append({
                        "uri": prop_uri,
                        "name": prop_name,
                        "label": prop_name,
                        "comment": "",
                        "type": "ObjectProperty",
                        "domain": class_name,
                        "range": target_name,
                    })

        # Scan both rdfs:subClassOf and owl:equivalentClass for restrictions
        for predicate in (RDFS.subClassOf, OWL.equivalentClass):
            for cls, _, obj in graph.triples((None, predicate, None)):
                if isinstance(cls, BNode):
                    continue
                cls_name = _extract_local_name(str(cls))

                # Direct restriction node
                IofImportService._process_restriction(graph, cls_name, obj, known_class_names,
                                                      _try_add)

                # Intersection inside equivalentClass:
                #   owl:equivalentClass [ owl:intersectionOf ( ... ) ]
                for int_list in graph.objects(obj, OWL.intersectionOf):
                    IofImportService._walk_rdf_list(graph, int_list,
                                                    lambda node: IofImportService._process_restriction(
                                                        graph, cls_name, node,
                                                        known_class_names, _try_add))

        return relationships

    @staticmethod
    def _process_restriction(graph, cls_name, node, known_class_names, callback):
        """If *node* is an owl:Restriction, extract the relationship."""
        if not isinstance(node, BNode):
            return
        if (node, RDF.type, OWL.Restriction) not in graph:
            return

        prop_uri = None
        for p in graph.objects(node, OWL.onProperty):
            prop_uri = str(p)
            break
        if not prop_uri:
            return

        prop_name = IofImportService._resolve_property_label(graph, prop_uri)

        for target in graph.objects(node, OWL.someValuesFrom):
            if not isinstance(target, BNode):
                callback(cls_name, prop_name, _extract_local_name(str(target)),
                         prop_uri)

        for target in graph.objects(node, OWL.allValuesFrom):
            if not isinstance(target, BNode):
                callback(cls_name, prop_name, _extract_local_name(str(target)),
                         prop_uri)

    @staticmethod
    def _walk_rdf_list(graph, node, visitor):
        """Walk an RDF list and call *visitor* on each element."""
        nil_uri = str(RDF.nil)
        current = node
        while current and str(current) != nil_uri:
            for first in graph.objects(current, RDF.first):
                visitor(first)
            rest = None
            for r in graph.objects(current, RDF.rest):
                rest = r
                break
            current = rest

    @staticmethod
    def fetch_and_parse_iof(
        domain_keys: List[str],
    ) -> Dict[str, Any]:
        """Fetch IOF modules for the selected domains, merge, and parse.

        Args:
            domain_keys: List of domain keys (e.g. ["CORE", "MAINTENANCE"])

        Returns:
            dict with keys:
                success (bool)
                turtle (str) – merged Turtle content
                stats (dict)  – classes, properties counts
                fetched (int) – number of modules successfully loaded
                failed (list)  – module labels that could not be fetched
                message (str)
        """
        start = time.time()
        modules = IofImportService._collect_modules(domain_keys)

        if not modules:
            raise ValidationError("No valid IOF domains selected.")

        logger.info("Fetching %d modules for domains: %s", len(modules), domain_keys)

        # Fetch modules concurrently
        merged_graph = Graph()
        fetched_count = 0
        failed_modules: list[str] = []

        with ThreadPoolExecutor(max_workers=IofImportService._MAX_WORKERS) as pool:
            futures = {
                pool.submit(IofImportService._fetch_single_module, mod): mod
                for mod in modules
            }
            for future in as_completed(futures):
                label, content, error = future.result()
                if content:
                    try:
                        fmt = "xml" if content.lstrip().startswith("<") else "turtle"
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
                "Could not reach the IOF GitHub repository. "
                "If you are running in Databricks Apps, outbound internet access "
                "may be restricted. Try downloading IOF RDF files manually from "
                "https://github.com/iofoundry/ontology and importing via 'Import OWL'."
            )
            raise InfrastructureError(hint, detail=", ".join(failed_modules) or None)

        # Serialize merged graph to Turtle
        turtle_content = merged_graph.serialize(format="turtle")

        # Parse with the OntoBricks OWL parser for structured output
        from back.objects.ontology import Ontology

        result = Ontology.parse_owl(turtle_content, extract_advanced=True)
        ontology_info, classes, properties, constraints, swrl_rules, axioms, expressions, _groups = result

        # ------------------------------------------------------------------
        # Post-processing: fix relationships for BFO-based ontologies
        #
        # IOF ontologies define most class-to-class relationships through
        # OWL restrictions (someValuesFrom / allValuesFrom) rather than
        # explicit rdfs:domain / rdfs:range on properties.  We therefore:
        #
        #   1. Build a set of known class names from the parsed classes.
        #   2. Filter out ObjectProperties whose domain/range reference BFO
        #      or other external classes not in the imported set.
        #   3. Extract additional relationships from OWL restrictions on
        #      classes (rdfs:subClassOf and owl:equivalentClass).
        #   4. Merge, deduplicate, and sort.
        # ------------------------------------------------------------------
        known_class_names: Set[str] = {cls["name"] for cls in classes}

        # Step 2: keep only properties referencing known classes
        original_prop_count = len(properties)
        cleaned_properties = []
        for prop in properties:
            if prop.get("type") == "ObjectProperty":
                domain = prop.get("domain", "")
                range_val = prop.get("range", "")
                if domain in known_class_names and range_val in known_class_names:
                    cleaned_properties.append(prop)
            else:
                domain = prop.get("domain", "")
                if not domain or domain in known_class_names:
                    cleaned_properties.append(prop)

        dropped = original_prop_count - len(cleaned_properties)
        if dropped:
            logger.info("Dropped %d properties with external domain/range", dropped)

        # Step 3: extract relationships from OWL restrictions
        restriction_rels = IofImportService._extract_relationships_from_restrictions(
            merged_graph, known_class_names
        )
        logger.info("Extracted %d relationships from OWL restrictions", len(restriction_rels))

        # Step 4: merge and deduplicate (keyed on URI + endpoints)
        existing_keys = set()
        for p in cleaned_properties:
            if p.get("type") == "ObjectProperty":
                existing_keys.add(
                    f"{p.get('domain')}|{p.get('uri', p.get('name'))}|{p.get('range')}"
                )

        for rel in restriction_rels:
            key = f"{rel['domain']}|{rel['uri']}|{rel['range']}"
            if key not in existing_keys:
                existing_keys.add(key)
                cleaned_properties.append(rel)

        properties = sorted(cleaned_properties, key=lambda x: x.get("name", ""))
        obj_count = sum(1 for p in properties if p.get('type') == 'ObjectProperty')
        logger.info("Final property count: %d (%d relationships)", len(properties), obj_count)

        stats = {
            "classes": len(classes),
            "properties": len(properties),
            "constraints": len(constraints),
            "modules_fetched": fetched_count,
            "modules_failed": len(failed_modules),
        }

        domain_names = ", ".join(
            IofImportService.IOF_DOMAINS[k]["name"] for k in domain_keys if k in IofImportService.IOF_DOMAINS
        )
        msg = (
            f"IOF imported: {stats['classes']} classes, {stats['properties']} "
            f"relationships from {domain_names} ({fetched_count} modules loaded"
        )
        if failed_modules:
            msg += f", {len(failed_modules)} modules unavailable"
        msg += f") in {elapsed:.1f}s"

        logger.info("%s", msg)

        return {
            "success": True,
            "message": msg,
            "turtle": turtle_content,
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
