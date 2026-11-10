"""HL7 FHIR R5 import service.

Fetches the HL7 FHIR R5 ontology (fhir.ttl) from the official HL7 specification
server, parses it with rdflib, and converts it to OntoBricks ontology structures.

FHIR's OWL/RDF structure differs from standard OWL ontologies:
  - All resource types are flat subclasses of fhir:Resource or fhir:DomainResource
    (no deep hierarchy beyond two levels).
  - Properties are polymorphic (same predicate name across different resources).
  - Primitive values are wrapped in blank nodes using fhir:v.

Because of the flat hierarchy, the FHIR ontology is loaded as a single Turtle file
and then filtered by the user's selected domain groups (CLINICAL, DIAGNOSTICS, etc.)
using a curated mapping of FHIR resource types to domain buckets.

Reference: https://hl7.org/fhir/R5/rdf.html
OWL file:  https://hl7.org/fhir/R5/fhir.ttl
"""

import time
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from rdflib import OWL, RDF, RDFS, Graph, Namespace, URIRef

from back.core.errors import InfrastructureError, ValidationError
from back.core.helpers import extract_local_name as _extract_local_name
from back.core.industry.constants import FHIR_NS
from back.core.logging import get_logger
from shared.config.constants import HTTP_USER_AGENT

logger = get_logger(__name__)

FHIR = Namespace(FHIR_NS)


class FhirImportService:
    # ---------------------------------------------------------------------------
    # Domain groupings — curated mapping of FHIR R5 resource types to domains
    # ---------------------------------------------------------------------------

    # Each resource key is the local name of the fhir: class (e.g. "Patient").
    _DOMAIN_RESOURCES: Dict[str, List[str]] = {
        "FOUNDATION": [
            # True FHIR root types (always needed for the tree to render)
            "Base", "Resource", "Element", "DataType", "BackboneType",
            "DomainResource", "BackboneElement",
            # Infrastructure resources
            "Bundle", "BundleEntry",
            "OperationOutcome", "Parameters", "Binary",
            "CapabilityStatement", "StructureDefinition",
            "ImplementationGuide", "MessageHeader",
            "OperationDefinition", "SearchParameter",
            "Subscription", "SubscriptionStatus", "SubscriptionTopic",
        ],
        "CLINICAL": [
            "Patient",
            "RelatedPerson",
            "Person",
            "Practitioner",
            "PractitionerRole",
            "Organization",
            "Location",
            "HealthcareService",
            "Encounter",
            "EpisodeOfCare",
            "Condition",
            "Procedure",
            "Observation",
            "AllergyIntolerance",
            "Immunization",
            "ImmunizationEvaluation",
            "ImmunizationRecommendation",
            "FamilyMemberHistory",
            "ClinicalImpression",
            "DetectedIssue",
            "RiskAssessment",
            "CarePlan",
            "CareTeam",
            "Goal",
            "ServiceRequest",
            "NutritionOrder",
            "VisionPrescription",
            "Flag",
            "Communication",
            "CommunicationRequest",
            "DeviceRequest",
            "DeviceUsage",
            "Device",
            "DeviceDefinition",
            "DeviceMetric",
        ],
        "DIAGNOSTICS": [
            "DiagnosticReport",
            "ImagingStudy",
            "ImagingSelection",
            "Specimen",
            "SpecimenDefinition",
            "BodyStructure",
            "Observation",
            "ObservationDefinition",
            "QuestionnaireResponse",
            "MolecularSequence",
            "GenomicStudy",
            "DocumentReference",
            "Composition",
        ],
        "MEDICATIONS": [
            "Medication",
            "MedicationRequest",
            "MedicationAdministration",
            "MedicationDispense",
            "MedicationStatement",
            "MedicationKnowledge",
            "Ingredient",
            "ManufacturedItemDefinition",
            "MedicinalProductDefinition",
            "PackagedProductDefinition",
            "ClinicalUseDefinition",
            "SubstanceDefinition",
            "Substance",
            "AdministrableProductDefinition",
            "RegulatedAuthorization",
        ],
        "WORKFLOW": [
            "Task",
            "Appointment",
            "AppointmentResponse",
            "Schedule",
            "Slot",
            "VerificationResult",
            "RequestOrchestration",
            "Transport",
            "InventoryItem",
            "InventoryReport",
            "SupplyRequest",
            "SupplyDelivery",
            "NutritionIntake",
        ],
        "FINANCIAL": [
            "Claim",
            "ClaimResponse",
            "Coverage",
            "CoverageEligibilityRequest",
            "CoverageEligibilityResponse",
            "EnrollmentRequest",
            "EnrollmentResponse",
            "ExplanationOfBenefit",
            "PaymentNotice",
            "PaymentReconciliation",
            "Account",
            "Invoice",
            "ChargeItem",
            "ChargeItemDefinition",
            "Contract",
        ],
    }

    FHIR_DOMAINS: Dict[str, Dict[str, Any]] = {
        "FOUNDATION": {
            "name": "Foundation",
            "description": "Core FHIR infrastructure resources: Resource, DomainResource, "
            "Bundle, CapabilityStatement, StructureDefinition, "
            "OperationOutcome, and messaging/subscription primitives.",
            "icon": "bi-layers",
            "color": "secondary",
            "required": True,
        },
        "CLINICAL": {
            "name": "Clinical",
            "description": "Clinical resources: Patient, Practitioner, Encounter, Condition, "
            "Procedure, Observation, AllergyIntolerance, Immunization, "
            "CarePlan, Goal, Device, and more.",
            "icon": "bi-person-heart",
            "color": "danger",
            "required": False,
        },
        "DIAGNOSTICS": {
            "name": "Diagnostics",
            "description": "Diagnostic resources: DiagnosticReport, ImagingStudy, Specimen, "
            "Observation, MolecularSequence, DocumentReference, Composition.",
            "icon": "bi-activity",
            "color": "info",
            "required": False,
        },
        "MEDICATIONS": {
            "name": "Medications",
            "description": "Medication resources: Medication, MedicationRequest, "
            "MedicationAdministration, MedicationDispense, "
            "MedicinalProductDefinition, Substance.",
            "icon": "bi-capsule",
            "color": "success",
            "required": False,
        },
        "WORKFLOW": {
            "name": "Workflow",
            "description": "Workflow resources: Task, Appointment, Schedule, "
            "RequestOrchestration, Transport, SupplyRequest.",
            "icon": "bi-diagram-3",
            "color": "primary",
            "required": False,
        },
        "FINANCIAL": {
            "name": "Financial",
            "description": "Financial resources: Claim, Coverage, ExplanationOfBenefit, "
            "Invoice, ChargeItem, Contract, Account.",
            "icon": "bi-currency-dollar",
            "color": "warning",
            "required": False,
        },
    }

    _REQUEST_TIMEOUT = 60  # FHIR fhir.ttl is ~3 MB — allow generous timeout

    # Supported FHIR versions and their publication base URLs.
    # The namespace URI is identical across all versions.
    FHIR_VERSIONS: Dict[str, Dict[str, str]] = {
        "R4":  {"label": "R4 (4.0.1)",  "base_url": "https://hl7.org/fhir/R4"},
        "R4B": {"label": "R4B (4.3.0)", "base_url": "https://hl7.org/fhir/R4B"},
        "R5":  {"label": "R5 (5.0.0)",  "base_url": "https://hl7.org/fhir/R5"},
    }
    DEFAULT_VERSION = "R5"

    # -------------------------------------------------------------------------
    # Catalog
    # -------------------------------------------------------------------------

    @staticmethod
    def get_fhir_catalog() -> List[Dict[str, Any]]:
        """Return the FHIR domain catalog and supported versions for the frontend."""
        catalog = []
        for key, domain in FhirImportService.FHIR_DOMAINS.items():
            resource_count = len(FhirImportService._DOMAIN_RESOURCES.get(key, []))
            catalog.append(
                {
                    "key": key,
                    "name": domain["name"],
                    "description": domain["description"],
                    "icon": domain["icon"],
                    "color": domain["color"],
                    "required": domain.get("required", False),
                    "module_count": resource_count,
                }
            )
        return catalog

    @staticmethod
    def get_fhir_versions() -> List[Dict[str, str]]:
        """Return supported FHIR versions as ``[{key, label, default}]``."""
        return [
            {
                "key": k,
                "label": v["label"],
                "default": k == FhirImportService.DEFAULT_VERSION,
            }
            for k, v in FhirImportService.FHIR_VERSIONS.items()
        ]

    # -------------------------------------------------------------------------
    # Fetch
    # -------------------------------------------------------------------------

    @staticmethod
    def _fetch_fhir_ttl(version: str = "R5") -> str:
        """Download the FHIR OWL/Turtle file for the given version from hl7.org.

        Args:
            version: FHIR release key — one of ``FHIR_VERSIONS`` (e.g. ``"R5"``).

        Returns:
            Raw Turtle content as a string.

        Raises:
            ValidationError: If the version key is unknown.
            InfrastructureError: If the download fails or returns non-Turtle content.
        """
        version_info = FhirImportService.FHIR_VERSIONS.get(version)
        if not version_info:
            supported = ", ".join(FhirImportService.FHIR_VERSIONS)
            raise ValidationError(
                f"Unknown FHIR version '{version}'. Supported: {supported}."
            )

        url = f"{version_info['base_url']}/fhir.ttl"
        logger.info("Fetching FHIR %s OWL from %s", version, url)
        try:
            resp = requests.get(
                url,
                timeout=FhirImportService._REQUEST_TIMEOUT,
                headers={
                    "User-Agent": HTTP_USER_AGENT,
                    "Accept": "text/turtle, application/x-turtle, */*",
                },
                allow_redirects=True,
            )
        except requests.exceptions.Timeout:
            raise InfrastructureError(
                f"Timeout fetching FHIR {version} ontology from {url}. "
                "The HL7 server may be slow. Try again or import manually "
                "via 'Import OWL' using a locally downloaded fhir.ttl."
            )
        except requests.exceptions.RequestException as exc:
            raise InfrastructureError(
                f"Network error fetching FHIR {version} ontology: {exc}",
                detail=str(exc),
            )

        if resp.status_code != 200:
            raise InfrastructureError(
                f"FHIR {version} ontology server returned HTTP {resp.status_code}.",
                detail=url,
            )

        text = resp.text.strip()
        if text.startswith("<!DOCTYPE") or text.startswith("<html"):
            raise InfrastructureError(
                f"FHIR {version} ontology URL returned an HTML page instead of Turtle. "
                "The HL7 server may be temporarily unavailable.",
                detail=url,
            )

        logger.info("Downloaded FHIR %s OWL (%d bytes)", version, len(resp.content))
        return text

    # FHIR primitive datatype local-names — owl:allValuesFrom pointing to these
    # means the restriction encodes a datatype property, not an object property.
    _FHIR_PRIMITIVE_TYPES: Set[str] = {
        "string", "boolean", "integer", "decimal", "date", "dateTime",
        "uri", "code", "id", "markdown", "base64Binary", "canonical",
        "instant", "time", "url", "uuid", "oid", "positiveInt",
        "unsignedInt", "integer64", "xhtml",
    }

    # FHIR complex/compound datatypes that appear as property range targets in
    # every domain. They are always auto-included so that object properties
    # referencing them can be resolved by the frontend (e.g. ontology-map.js
    # validates that both domain and range class exist before rendering a link).
    _FHIR_COMPLEX_TYPES: Set[str] = {
        "Address", "Age", "Annotation", "Attachment",
        "Availability", "BackboneElement", "BackboneType",
        "CodeableConcept", "CodeableReference", "Coding",
        "ContactDetail", "ContactPoint", "Count",
        "DataRequirement", "Distance", "Dosage", "Duration",
        "Element", "Expression", "ExtendedContactDetail",
        "HumanName", "Identifier", "MarketingStatus",
        "Meta", "Money", "MoneyQuantity", "Narrative",
        "ParameterDefinition", "Period", "ProductShelfLife",
        "Quantity", "Range", "Ratio", "RatioRange",
        "Reference", "RelatedArtifact", "SampledData",
        "Signature", "SimpleQuantity", "Timing",
        "TriggerDefinition", "UsageContext",
        "VirtualServiceDetail",
        # Backbone sub-types that appear as ranges
        "Prism",
    }

    # -------------------------------------------------------------------------
    # Transform
    # -------------------------------------------------------------------------

    @staticmethod
    def _build_allowed_resources(domain_keys: List[str]) -> Set[str]:
        """Collect the set of resource local-names included in the selection.

        Always includes FOUNDATION resources and all FHIR complex datatypes so
        that object-property range targets resolve correctly in the frontend.
        """
        allowed: Set[str] = set()
        # FOUNDATION is always auto-included
        if "FOUNDATION" not in domain_keys:
            domain_keys = ["FOUNDATION"] + list(domain_keys)
        for key in domain_keys:
            allowed.update(FhirImportService._DOMAIN_RESOURCES.get(key, []))
        # Complex/compound datatypes are always needed as property range targets
        allowed.update(FhirImportService._FHIR_COMPLEX_TYPES)
        return allowed

    @staticmethod
    def _extract_properties_from_restrictions(
        graph: Graph,
        class_uri: URIRef,
        class_local_name: str,
        known_class_names: Set[str],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Extract dataProperties and objectProperties from OWL restrictions.

        FHIR encodes ALL property bindings as::

            rdfs:subClassOf [
                a owl:Restriction ;
                owl:onProperty fhir:<propName> ;
                owl:allValuesFrom fhir:<rangeType>
            ]

        There are no standalone ``rdfs:domain`` declarations on the properties.
        This method walks the ``rdfs:subClassOf`` blank nodes and extracts
        ``(property, range)`` pairs.

        Returns:
            (data_properties, object_properties) — each a list of dicts
            compatible with the OntoBricks property schema.
        """
        from rdflib import BNode

        data_props: List[Dict[str, Any]] = []
        obj_props: List[Dict[str, Any]] = []
        seen_prop_names: Set[str] = set()

        primitives = FhirImportService._FHIR_PRIMITIVE_TYPES

        for restriction_node in graph.objects(class_uri, RDFS.subClassOf):
            if not isinstance(restriction_node, BNode):
                continue

            # Check it is an owl:Restriction
            if (restriction_node, RDF.type, OWL.Restriction) not in graph:
                continue

            # Get owl:onProperty
            prop_uri_node: Optional[URIRef] = None
            for p in graph.objects(restriction_node, OWL.onProperty):
                if isinstance(p, URIRef):
                    prop_uri_node = p
                    break
            if prop_uri_node is None:
                continue

            prop_local = _extract_local_name(str(prop_uri_node))
            if not prop_local or prop_local in seen_prop_names:
                continue

            # Get rdfs:label for the property (if available in graph)
            prop_label = ""
            for lbl in graph.objects(prop_uri_node, RDFS.label):
                prop_label = str(lbl).strip()
                break
            if not prop_label:
                prop_label = prop_local

            # Get rdfs:comment for the property
            prop_comment = ""
            for cmt in graph.objects(prop_uri_node, RDFS.comment):
                text = str(cmt).strip()
                # FHIR comments are often "ResourceName.propName: description"
                if ":" in text:
                    text = text.split(":", 1)[1].strip()
                prop_comment = text
                break

            # Get owl:allValuesFrom to determine the range
            range_local = ""
            for avf in graph.objects(restriction_node, OWL.allValuesFrom):
                if isinstance(avf, URIRef):
                    range_local = _extract_local_name(str(avf))
                    break

            seen_prop_names.add(prop_local)

            if range_local in primitives:
                data_props.append(
                    {
                        "name": prop_local,
                        "label": prop_label,
                        "comment": prop_comment,
                        "type": "string" if range_local == "string" else range_local,
                    }
                )
            else:
                # object property — only include if range is a known class
                if range_local and range_local not in known_class_names:
                    # Keep it anyway — FHIR complex types (Reference, CodeableConcept)
                    # are useful even if not in the selected domain subset
                    pass
                obj_props.append(
                    {
                        "uri": str(prop_uri_node),
                        "name": prop_local,
                        "label": prop_label,
                        "comment": prop_comment,
                        "type": "ObjectProperty",
                        "domain": class_local_name,
                        "range": range_local,
                    }
                )

        return data_props, obj_props

    @staticmethod
    def _transform_fhir_to_ontobricks(
        graph: Graph,
        domain_keys: List[str],
        version: str = "R5",
    ) -> Dict[str, Any]:
        """Convert a parsed FHIR OWL graph to OntoBricks classes and properties.

        FHIR's fhir.ttl encodes all property bindings as OWL restrictions on
        classes (``rdfs:subClassOf [ owl:Restriction … ]``), not via
        ``rdfs:domain`` on the properties. This method therefore:

        1. Enumerates all ``owl:Class`` subjects in the fhir: namespace and
           filters to the allowed domain set.
        2. For each class, extracts data- and object-properties by walking
           the restriction blank nodes.
        3. Returns the OntoBricks-compatible structure.

        Returns:
            dict with keys: classes, properties, ontology_info, stats.
        """
        allowed_local_names = FhirImportService._build_allowed_resources(domain_keys)

        classes: List[Dict[str, Any]] = []
        class_uris: Dict[str, URIRef] = {}   # local_name → URIRef for restriction pass

        # ----------------------------------------------------------------
        # Pass 1: collect classes
        # ----------------------------------------------------------------
        for subj in graph.subjects(RDF.type, OWL.Class):
            if not isinstance(subj, URIRef):
                continue
            uri_str = str(subj)
            if not uri_str.startswith(FHIR_NS):
                continue
            local_name = _extract_local_name(uri_str)
            if not local_name:
                continue
            if local_name not in allowed_local_names:
                continue

            label = ""
            for lbl in graph.objects(subj, RDFS.label):
                label = str(lbl).strip()
                break
            if not label:
                label = local_name

            comment = ""
            for cmt in graph.objects(subj, RDFS.comment):
                comment = str(cmt).strip()
                break

            # Determine parent: only consider fhir: namespace URIRefs.
            # Other namespaces (w5:, rim:, dc:, etc.) appear in rdfs:subClassOf
            # but are not meaningful for the OntoBricks class tree.
            parent = ""
            for par_node in graph.objects(subj, RDFS.subClassOf):
                if not isinstance(par_node, URIRef):
                    continue
                if not str(par_node).startswith(FHIR_NS):
                    continue
                par_local = _extract_local_name(str(par_node))
                if par_local and par_local != local_name:
                    parent = par_local
                    break

            classes.append(
                {
                    "uri": uri_str,
                    "name": local_name,
                    "label": label,
                    "comment": comment,
                    "emoji": "",
                    "parent": parent,
                    "dashboard": "",
                    "dashboardParams": {},
                    "dataProperties": [],
                }
            )
            class_uris[local_name] = subj

        # Ensure structural base types are always present so parent references
        # resolve correctly. These types define the FHIR class hierarchy skeleton:
        #   Base (root) → Resource → DomainResource → concrete resources
        #   Base (root) → Element → DataType → compound types (Address, etc.)
        #                         → BackboneElement → backbone components
        # We only synthesise a stub when the real class wasn't parsed from the graph.
        known_class_names: Set[str] = {c["name"] for c in classes}
        _BASE_HIERARCHY = [
            ("Base",            ""),
            ("Resource",        "Base"),
            ("Element",         "Base"),
            ("DomainResource",  "Resource"),
            ("DataType",        "Element"),
            ("BackboneType",    "Element"),
            ("BackboneElement", "Element"),
        ]
        for base, base_parent in _BASE_HIERARCHY:
            if base not in known_class_names:
                classes.append(
                    {
                        "uri": f"{FHIR_NS}{base}",
                        "name": base,
                        "label": base,
                        "comment": f"FHIR structural base type: {base}",
                        "emoji": "",
                        "parent": base_parent,
                        "dashboard": "",
                        "dashboardParams": {},
                        "dataProperties": [],
                    }
                )
                known_class_names.add(base)

        # ----------------------------------------------------------------
        # Pass 2: extract properties from OWL restrictions
        # ----------------------------------------------------------------
        all_obj_props: List[Dict[str, Any]] = []
        obj_prop_seen: Set[str] = set()  # "domain|name" dedup key

        for cls in classes:
            cls_local = cls["name"]
            cls_uri = class_uris.get(cls_local)
            if cls_uri is None:
                continue

            data_props, obj_props = FhirImportService._extract_properties_from_restrictions(
                graph, cls_uri, cls_local, known_class_names
            )

            # Attach data properties inline on the class
            cls["dataProperties"] = data_props

            # Collect object properties deduplicated globally
            for op in obj_props:
                key = f"{op['domain']}|{op['name']}"
                if key not in obj_prop_seen:
                    obj_prop_seen.add(key)
                    all_obj_props.append(op)

        all_obj_props.sort(key=lambda p: p.get("name", ""))

        domain_names = ", ".join(
            FhirImportService.FHIR_DOMAINS[k]["name"]
            for k in domain_keys
            if k in FhirImportService.FHIR_DOMAINS
        )

        # Total data-property count for stats
        total_data_props = sum(len(c.get("dataProperties", [])) for c in classes)

        ver_info = FhirImportService.FHIR_VERSIONS.get(version, FhirImportService.FHIR_VERSIONS["R5"])
        ttl_url = f"{ver_info['base_url']}/fhir.ttl"
        return {
            "classes": classes,
            "properties": all_obj_props,
            "ontology_info": {
                "name": f"HL7 FHIR {version} — {domain_names}",
                "base_uri": FHIR_NS,
                "description": (
                    f"HL7 FHIR {version} ontology, domains: {domain_names}. "
                    f"Source: {ttl_url}"
                ),
                "version": version,
            },
            "stats": {
                "classes": len(classes),
                "properties": len(all_obj_props) + total_data_props,
            },
        }

    # -------------------------------------------------------------------------
    # Public entry point
    # -------------------------------------------------------------------------

    @staticmethod
    def fetch_and_parse_fhir(
        domain_keys: List[str],
        version: str = "R5",
    ) -> Dict[str, Any]:
        """Fetch FHIR OWL for the given version, parse it, filter by domain.

        Args:
            domain_keys: List of domain keys (e.g. ["FOUNDATION", "CLINICAL"]).
            version: FHIR release — one of ``FHIR_VERSIONS`` keys (default ``"R5"``).

        Returns:
            dict with keys: success, message, ontology_info, classes, properties,
            constraints, if_then_rules, axioms, expressions, stats, failed.
        """
        if not domain_keys:
            raise ValidationError("No FHIR domains selected.")

        version = version.upper() if version else FhirImportService.DEFAULT_VERSION
        if version not in FhirImportService.FHIR_VERSIONS:
            supported = ", ".join(FhirImportService.FHIR_VERSIONS)
            raise ValidationError(
                f"Unknown FHIR version '{version}'. Supported: {supported}."
            )

        # FOUNDATION always auto-included
        if "FOUNDATION" not in domain_keys:
            domain_keys = ["FOUNDATION"] + list(domain_keys)

        start = time.time()

        ttl_content = FhirImportService._fetch_fhir_ttl(version)

        graph = Graph()
        try:
            graph.parse(data=ttl_content, format="turtle")
        except Exception as exc:
            raise ValidationError(
                f"Failed to parse FHIR {version} Turtle ontology.", detail=str(exc)
            ) from exc

        logger.info("Parsed FHIR %s graph: %d triples", version, len(graph))

        mapped = FhirImportService._transform_fhir_to_ontobricks(graph, domain_keys, version)

        elapsed = time.time() - start
        domain_names = ", ".join(
            FhirImportService.FHIR_DOMAINS[k]["name"]
            for k in domain_keys
            if k in FhirImportService.FHIR_DOMAINS
        )
        stats = mapped["stats"]
        msg = (
            f"FHIR {version} imported: {stats['classes']} classes, "
            f"{stats['properties']} properties from {domain_names} "
            f"in {elapsed:.1f}s"
        )
        logger.info("%s", msg)

        return {
            "success": True,
            "message": msg,
            "turtle": ttl_content,
            "ontology_info": mapped["ontology_info"],
            "classes": mapped["classes"],
            "properties": mapped["properties"],
            "constraints": [],
            "if_then_rules": [],
            "axioms": [],
            "expressions": [],
            "stats": {**stats, "modules_fetched": 1, "modules_failed": 0},
            "fetched": 1,
            "failed": [],
        }
