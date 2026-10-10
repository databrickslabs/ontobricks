"""OWL/RDFS ingest, merge, and industry import extracted from :class:`Ontology`.

Fowler Extract Class. ``Ontology`` keeps one-line delegators.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional

from back.core.errors import InfrastructureError, OntoBricksError, ValidationError
from back.core.industry import (
    fetch_and_parse_cdisc,
    fetch_and_parse_fibo,
    fetch_and_parse_fhir,
    fetch_and_parse_iof,
)
from back.core.logging import get_logger
from back.core.w3c.owl import ConflictReport, OntologyConflictDetector
from shared.config.constants import DEFAULT_BASE_URI
from back.objects.ontology.OntologyClassModel import OntologyClassModel
from back.objects.ontology.OntologyOwl import OntologyOwl

if TYPE_CHECKING:
    from back.objects.session.DomainSession import DomainSession

IndustryKind = Literal["fibo", "cdisc", "iof", "fhir"]

logger = get_logger(__name__)

_INDUSTRY_EMPTY_MESSAGE: Dict[IndustryKind, str] = {
    "fibo": "No FIBO domains selected.",
    "cdisc": "No CDISC domains selected.",
    "iof": "No IOF domains selected.",
    "fhir": "No FHIR domains selected.",
}

_INDUSTRY_LOG_LABEL: Dict[IndustryKind, str] = {
    "fibo": "FIBO",
    "cdisc": "CDISC",
    "iof": "IOF",
    "fhir": "FHIR",
}

_INDUSTRY_FETCH = {
    "fibo": fetch_and_parse_fibo,
    "cdisc": fetch_and_parse_cdisc,
    "iof": fetch_and_parse_iof,
    "fhir": fetch_and_parse_fhir,
}


class OntologyImport:
    """Parse and merge vocabularies into the current domain session."""

    def __init__(self, session: "DomainSession") -> None:
        self._domain = session

    def ingest_owl(
        self,
        owl_content: str,
        *,
        name_fallback_to_domain: bool = True,
        outcome: str = "import",
    ) -> Dict[str, Any]:
        """Parse OWL content, apply to project, return the appropriate success payload.

        ``outcome`` controls the response shape:

        - ``"import"`` → :meth:`build_import_owl_success_payload`
        - ``"parse"`` → :meth:`build_parse_owl_success_payload`
        - ``"load_file"`` → :meth:`build_load_owl_file_success_payload`
        """
        result = OntologyOwl.parse_owl(owl_content, extract_advanced=True)
        (
            ontology_info,
            classes,
            properties,
            constraints,
            swrl_rules,
            axioms,
            expressions,
            groups,
        ) = result

        # Auto-fallback: OWL parser found nothing → try RDFS/SKOS
        if not classes and not properties:
            # SHACL files → data quality, not ontology classes
            shacl = self._try_import_as_shacl(owl_content)
            if shacl is not None:
                return shacl
            try:
                rdfs_info, rdfs_classes, rdfs_props = OntologyOwl.parse_rdfs(owl_content)
                if rdfs_classes or rdfs_props:
                    logger.info(
                        "ingest_owl: OWL parse empty — falling back to RDFS/SKOS parser "
                        "(classes=%d properties=%d)", len(rdfs_classes), len(rdfs_props)
                    )
                    ontology_info = {
                        "name": rdfs_info.get("label", ""),
                        "uri": rdfs_info.get("uri", ""),
                    }
                    classes, properties = rdfs_classes, rdfs_props
                    constraints, swrl_rules, axioms, expressions, groups = [], [], [], [], []
            except Exception:
                pass

        resolved_name = self.apply_parsed_owl_to_domain(
            ontology_info,
            classes,
            properties,
            constraints,
            swrl_rules,
            axioms,
            expressions,
            groups=groups,
            name_fallback_to_domain=name_fallback_to_domain,
        )

        if outcome == "parse":
            return self.build_parse_owl_success_payload(
                ontology_info,
                classes,
                properties,
                constraints,
                swrl_rules,
                axioms,
                expressions,
                resolved_name,
            )
        if outcome == "load_file":
            return self.build_load_owl_file_success_payload(
                classes,
                properties,
                constraints,
                swrl_rules,
                axioms,
                expressions,
            )
        return self.build_import_owl_success_payload(classes, properties, constraints)

    def apply_parsed_rdfs_to_domain(
        self,
        rdfs_content: str,
    ) -> Dict[str, Any]:
        """Parse RDFS/SKOS/SHACL content, apply to project, return success payload.

        SHACL files (containing ``sh:NodeShape``) are automatically redirected
        to the data-quality store instead of the ontology class list.
        """
        # Auto-detect SHACL and route to dataquality
        shacl_result = self._try_import_as_shacl(rdfs_content)
        if shacl_result is not None:
            return shacl_result

        ontology_info, classes, properties = OntologyOwl.parse_rdfs(rdfs_content)
        self._domain.ontology.update(
            {
                "name": ontology_info.get("label", "Imported Vocabulary"),
                "base_uri": ontology_info.get(
                    "namespace", ontology_info.get("uri", "")
                ),
                "classes": classes,
                "properties": properties,
            }
        )
        OntologyClassModel.sync_class_data_properties(self._domain.ontology)
        self._domain.save()
        return {
            "success": True,
            "ontology": {
                "info": ontology_info,
                "classes": classes,
                "properties": properties,
            },
            "config": self._domain.ontology,
            "stats": {"classes": len(classes), "properties": len(properties)},
        }

    def _try_import_as_shacl(self, content: str) -> Optional[Dict[str, Any]]:
        """Return a dataquality import payload if *content* is a SHACL file, else None."""
        try:
            from rdflib import Graph, RDF, Namespace as NS
            _SH = NS("http://www.w3.org/ns/shacl#")
            g = Graph()
            g.parse(data=content, format="turtle")
            if not any(True for _ in g.subjects(RDF.type, _SH.NodeShape)):
                return None
            from back.core.w3c import SHACLService
            svc = SHACLService()
            imported = svc.import_shapes(content)
            if not imported:
                return None
            existing = list(self._domain.shacl_shapes or [])
            # Merge: replace shapes with same id, append new ones
            existing_ids = {s.get("id") for s in existing}
            for shape in imported:
                if shape.get("id") in existing_ids:
                    existing = [s if s.get("id") != shape.get("id") else shape for s in existing]
                else:
                    existing.append(shape)
            self._domain.shacl_shapes = existing
            self._domain.save()
            logger.info(
                "_try_import_as_shacl: detected SHACL — imported %d shape(s) to data quality",
                len(imported),
            )
            return {
                "success": True,
                "shacl": True,
                "message": f"Imported {len(imported)} SHACL shape(s) to Data Quality rules",
                "imported_count": len(imported),
                "stats": {"classes": 0, "properties": 0, "shacl_shapes": len(imported)},
            }
        except Exception:
            return None

    def analyze_import(
        self,
        owl_content: str,
        *,
        format: str = "owl",
    ) -> Dict[str, Any]:
        """Parse *owl_content* and compare against the current session ontology.

        Returns a JSON-serialisable :class:`~back.core.w3c.owl.ConflictReport`
        dict.  The session is **not** mutated.

        Parameters
        ----------
        owl_content:
            Raw OWL (Turtle/RDF/XML) or RDFS content.
        format:
            ``"owl"`` (default) or ``"rdfs"``.
        """
        content_len = len(owl_content)
        logger.info("analyze_import: format=%s content_len=%d", format, content_len)

        if format == "rdfs":
            logger.debug("analyze_import: parsing as RDFS")
            # SHACL files are data-quality rules, not ontology vocabulary
            if self._try_import_as_shacl(owl_content) is not None:
                raise ValidationError(
                    "This file contains SHACL shapes (data quality rules). "
                    "It has been automatically imported into Data Quality. "
                    "Use the Data Quality tab to view the imported shapes."
                )
            ontology_info, classes, properties = OntologyOwl.parse_rdfs(owl_content)
            logger.debug(
                "analyze_import: RDFS parsed — classes=%d properties=%d",
                len(classes), len(properties),
            )
            if not classes and not properties:
                raise ValidationError(
                    "No classes or properties found in the uploaded file. "
                    "Supported vocabularies: RDFS (rdfs:Class), OWL (owl:Class), "
                    "SKOS (skos:Concept). Please verify the file format."
                )
            incoming = {
                "classes": classes,
                "properties": properties,
                "constraints": [],
                "swrl_rules": [],
                "axioms": [],
                "expressions": [],
                "groups": [],
            }
        else:
            logger.debug("analyze_import: parsing as OWL")
            result = OntologyOwl.parse_owl(owl_content, extract_advanced=True)
            (
                _ontology_info,
                classes,
                properties,
                constraints,
                swrl_rules,
                axioms,
                expressions,
                groups,
            ) = result
            logger.debug(
                "analyze_import: OWL parsed — classes=%d properties=%d "
                "constraints=%d swrl_rules=%d axioms=%d expressions=%d groups=%d",
                len(classes), len(properties), len(constraints),
                len(swrl_rules), len(axioms), len(expressions or []), len(groups or []),
            )
            # Auto-fallback: OWL parser found nothing → try RDFS/SKOS
            if not classes and not properties:
                try:
                    rdfs_info, rdfs_classes, rdfs_props = OntologyOwl.parse_rdfs(owl_content)
                    if rdfs_classes or rdfs_props:
                        logger.info(
                            "analyze_import: OWL parse empty — falling back to RDFS/SKOS "
                            "(classes=%d properties=%d)", len(rdfs_classes), len(rdfs_props)
                        )
                        classes, properties = rdfs_classes, rdfs_props
                        constraints, swrl_rules, axioms, expressions, groups = [], [], [], [], []
                except Exception:
                    pass
            incoming = {
                "classes": classes,
                "properties": properties,
                "constraints": constraints,
                "swrl_rules": swrl_rules,
                "axioms": axioms,
                "expressions": expressions or [],
                "groups": groups or [],
            }

        existing_classes = len(self._domain.ontology.get("classes") or [])
        existing_props   = len(self._domain.ontology.get("properties") or [])
        logger.debug(
            "analyze_import: existing ontology — classes=%d properties=%d",
            existing_classes, existing_props,
        )

        detector = OntologyConflictDetector()
        report = detector.analyze(self._domain.ontology, incoming)
        s = report.to_dict()["summary"]
        logger.info(
            "analyze_import: conflict report — new=%d duplicates=%d conflicts=%d",
            s["new"], s["duplicates"], s["conflicts"],
        )
        return {"success": True, "report": report.to_dict()}

    def merge_parsed_owl_to_domain(
        self,
        owl_content: str,
        resolutions: Dict[str, str],
        *,
        format: str = "owl",
        name_fallback_to_domain: bool = True,
    ) -> Dict[str, Any]:
        """Parse *owl_content* and merge it into the current session ontology.

        Only entities classified as ``new`` are appended automatically.
        Entities with ``uri_conflict`` or ``name_conflict`` are handled
        according to the *resolutions* map.

        Parameters
        ----------
        owl_content:
            Raw OWL/RDFS content.
        resolutions:
            Mapping of entity URI (or name for nameless items) to one of:
            ``"skip"`` — keep existing, discard incoming.
            ``"overwrite"`` — replace existing with incoming.
            ``"rename:<new_name>"`` — add incoming with a new name.
        format:
            ``"owl"`` (default) or ``"rdfs"``.
        name_fallback_to_domain:
            Whether to fall back to the domain name when the ontology has
            no declared name.
        """
        if format == "rdfs":
            return self._merge_rdfs(owl_content, resolutions)

        logger.info(
            "merge_parsed_owl_to_domain: format=owl content_len=%d resolutions=%d key(s)",
            len(owl_content), len(resolutions),
        )
        result = OntologyOwl.parse_owl(owl_content, extract_advanced=True)
        (
            ontology_info,
            classes,
            properties,
            constraints,
            swrl_rules,
            axioms,
            expressions,
            groups,
        ) = result

        # Auto-fallback: OWL parser found nothing → try RDFS/SKOS
        if not classes and not properties:
            try:
                rdfs_info, rdfs_classes, rdfs_props = OntologyOwl.parse_rdfs(owl_content)
                if rdfs_classes or rdfs_props:
                    logger.info(
                        "merge_parsed_owl_to_domain: OWL parse empty — falling back to RDFS/SKOS "
                        "(classes=%d properties=%d)", len(rdfs_classes), len(rdfs_props)
                    )
                    classes, properties = rdfs_classes, rdfs_props
                    constraints, swrl_rules, axioms, expressions, groups = [], [], [], [], []
            except Exception:
                pass

        incoming = {
                "classes": classes,
                "properties": properties,
                "constraints": constraints,
                "swrl_rules": swrl_rules,
                "axioms": axioms,
                "expressions": expressions or [],
                "groups": groups or [],
            }

        logger.debug(
            "merge_parsed_owl_to_domain: OWL parsed — classes=%d properties=%d "
            "constraints=%d swrl_rules=%d axioms=%d groups=%d",
            len(classes), len(properties), len(constraints),
            len(swrl_rules), len(axioms), len(groups or []),
        )

        detector = OntologyConflictDetector()
        report = detector.analyze(self._domain.ontology, incoming)
        s = report.to_dict()["summary"]
        logger.info(
            "merge_parsed_owl_to_domain: conflict analysis — new=%d duplicates=%d conflicts=%d",
            s["new"], s["duplicates"], s["conflicts"],
        )

        ont = self._domain.ontology
        ont["classes"] = self._apply_resolutions(
            "class", ont.get("classes") or [], report, resolutions
        )
        ont["properties"] = self._apply_resolutions(
            "property", ont.get("properties") or [], report, resolutions
        )
        ont["constraints"] = self._apply_resolutions(
            "constraint", ont.get("constraints") or [], report, resolutions
        )
        ont["swrl_rules"] = self._apply_resolutions(
            "swrl_rule", ont.get("swrl_rules") or [], report, resolutions
        )
        ont["groups"] = self._apply_resolutions(
            "group", ont.get("groups") or [], report, resolutions
        )
        ont["axioms"] = self._apply_resolutions(
            "axiom", ont.get("axioms") or [], report, resolutions
        )
        ont["expressions"] = self._apply_resolutions(
            "expression", ont.get("expressions") or [], report, resolutions
        )

        OntologyClassModel.sync_class_data_properties(ont)
        OntologyClassModel.finalize_class_attributes(ont)
        self._domain.clear_generated_content()
        self._domain.save()

        all_classes = ont.get("classes") or []
        all_props = ont.get("properties") or []
        added = len([i for i in report.new_items])
        logger.info(
            "merge_parsed_owl_to_domain: saved — total classes=%d properties=%d "
            "new=%d duplicates_skipped=%d conflicts_resolved=%d",
            len(all_classes), len(all_props),
            added, len(report.duplicates), len(report.conflicts),
        )
        return {
            "success": True,
            "message": f"Merged: {added} new item(s) added",
            "config": ont,
            "stats": {
                "classes": len(all_classes),
                "properties": len(all_props),
                "new": added,
                "duplicates_skipped": len(report.duplicates),
                "conflicts_resolved": len(report.conflicts),
            },
        }

    def _merge_rdfs(
        self,
        rdfs_content: str,
        resolutions: Dict[str, str],
    ) -> Dict[str, Any]:
        """Append-mode merge for RDFS content."""
        logger.info(
            "_merge_rdfs: content_len=%d resolutions=%d key(s)",
            len(rdfs_content), len(resolutions),
        )
        # SHACL files belong in data quality, not the ontology
        if self._try_import_as_shacl(rdfs_content) is not None:
            raise ValidationError(
                "This file contains SHACL shapes (data quality rules). "
                "It has been automatically imported into Data Quality. "
                "Use the Data Quality tab to view the imported shapes."
            )
        ontology_info, classes, properties = OntologyOwl.parse_rdfs(rdfs_content)
        logger.debug(
            "_merge_rdfs: parsed — classes=%d properties=%d",
            len(classes), len(properties),
        )
        if not classes and not properties:
            raise ValidationError(
                "No classes or properties found in the uploaded file. "
                "Supported vocabularies: RDFS (rdfs:Class), OWL (owl:Class), "
                "SKOS (skos:Concept). Please verify the file format."
            )
        incoming = {
            "classes": classes,
            "properties": properties,
            "constraints": [],
            "swrl_rules": [],
            "axioms": [],
            "expressions": [],
            "groups": [],
        }

        detector = OntologyConflictDetector()
        ont = self._domain.ontology
        report = detector.analyze(ont, incoming)
        s = report.to_dict()["summary"]
        logger.info(
            "_merge_rdfs: conflict analysis — new=%d duplicates=%d conflicts=%d",
            s["new"], s["duplicates"], s["conflicts"],
        )

        ont["classes"] = self._apply_resolutions(
            "class", ont.get("classes") or [], report, resolutions
        )
        ont["properties"] = self._apply_resolutions(
            "property", ont.get("properties") or [], report, resolutions
        )

        self._domain.clear_generated_content()
        self._domain.save()

        all_classes = ont.get("classes") or []
        added = len([i for i in report.new_items])
        logger.info(
            "_merge_rdfs: saved — total classes=%d properties=%d "
            "new=%d duplicates_skipped=%d conflicts_resolved=%d",
            len(all_classes), len(ont.get("properties") or []),
            added, len(report.duplicates), len(report.conflicts),
        )
        return {
            "success": True,
            "message": f"Merged: {added} new item(s) added",
            "config": ont,
            "stats": {
                "classes": len(all_classes),
                "properties": len(ont.get("properties") or []),
                "new": added,
                "duplicates_skipped": len(report.duplicates),
                "conflicts_resolved": len(report.conflicts),
            },
        }

    @staticmethod
    def _apply_resolutions(
        entity_type: str,
        existing: list,
        report: ConflictReport,
        resolutions: Dict[str, str],
    ) -> list:
        """Return the merged list for one entity type after applying resolutions.

        Resolution keys are the incoming item's URI; for name-only items
        the key is the name.  Actions:

        - ``"skip"``            — keep existing, drop incoming.
        - ``"overwrite"``       — replace existing entry with incoming.
        - ``"rename:<name>"``   — append incoming with a patched name.
        """

        # Build a mutable copy of existing indexed by URI and by name.
        result = list(existing)
        uri_index: Dict[str, int] = {}
        name_index: Dict[str, int] = {}
        for idx, item in enumerate(result):
            u = (item.get("uri") or "").strip()
            n = (item.get("name") or item.get("label") or "").strip().lower()
            if u:
                uri_index[u] = idx
            if n:
                name_index[n] = idx

        type_items = [i for i in report.new_items + report.conflicts if i.entity_type == entity_type]

        n_appended = 0
        n_overwritten = 0
        n_renamed = 0
        n_skipped = 0

        for item in type_items:
            if item.conflict_type == "new":
                result.append(item.incoming)
                n_appended += 1
                continue

            # Determine resolution key: prefer URI, fall back to name.
            res_key = item.uri or item.name
            action = resolutions.get(res_key, "skip")

            if action == "overwrite":
                if item.uri and item.uri in uri_index:
                    result[uri_index[item.uri]] = item.incoming
                elif item.name and item.name in name_index:
                    result[name_index[item.name]] = item.incoming
                else:
                    result.append(item.incoming)
                n_overwritten += 1
            elif action.startswith("rename:"):
                new_name = action[7:].strip()
                patched = dict(item.incoming)
                patched["name"] = new_name
                result.append(patched)
                n_renamed += 1
            else:
                n_skipped += 1
            # else "skip" — do nothing (keep existing)

        logger.debug(
            "_apply_resolutions: entity_type=%s appended=%d overwritten=%d renamed=%d skipped=%d",
            entity_type, n_appended, n_overwritten, n_renamed, n_skipped,
        )
        return result

    def import_industry_ontology(
        self,
        kind: IndustryKind,
        domain_keys: List[str],
        version: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fetch industry modules, merge, parse, persist to project session.

        Args:
            kind: Industry standard identifier (fibo, cdisc, iof, fhir).
            domain_keys: Domain bucket keys to import.
            version: Version string for importers that support it (currently FHIR only).

        Returns the same dict shape as the former /import-fibo|cdisc|iof handlers.
        """
        if not domain_keys:
            raise ValidationError(_INDUSTRY_EMPTY_MESSAGE[kind])

        try:
            if kind == "fhir":
                from back.core.industry.fhir import FhirImportService
                fhir_version = version or FhirImportService.DEFAULT_VERSION
                result = _INDUSTRY_FETCH[kind](domain_keys, version=fhir_version)
            else:
                result = _INDUSTRY_FETCH[kind](domain_keys)

            info = result["ontology_info"]
            if kind == "cdisc":
                ont_name = info.get("label", "CDISC")
                base_uri = info.get("uri", "http://rdf.cdisc.org/")
                desc_prefix = "CDISC Foundational Standards in RDF – "
            elif kind == "fibo":
                ont_name = info.get("name", "FIBO")
                base_uri = info.get("uri", "https://spec.edmcouncil.org/fibo/ontology/")
                desc_prefix = "Financial Industry Business Ontology (FIBO) – "
            elif kind == "fhir":
                fhir_ver = info.get("version", fhir_version)
                ont_name = info.get("name", f"HL7 FHIR {fhir_ver}")
                base_uri = info.get("base_uri", "http://hl7.org/fhir/")
                desc_prefix = f"HL7 FHIR {fhir_ver} – "
            else:
                ont_name = info.get("name", "IOF")
                base_uri = info.get(
                    "uri", "https://spec.industrialontologies.org/ontology/"
                )
                desc_prefix = "Industrial Ontologies Foundry (IOF) – "

            self._domain.ontology.update(
                {
                    "name": ont_name,
                    "base_uri": base_uri,
                    "description": desc_prefix + ", ".join(domain_keys),
                    "classes": result["classes"],
                    "properties": result["properties"],
                    "constraints": result["constraints"],
                    "swrl_rules": result["swrl_rules"],
                    "axioms": result["axioms"],
                    "expressions": result["expressions"],
                }
            )
            self._domain.save()

            return {
                "success": True,
                "message": result["message"],
                "stats": result["stats"],
                "failed": result["failed"],
            }
        except OntoBricksError:
            raise
        except Exception as exc:
            logger.exception("%s import failed: %s", _INDUSTRY_LOG_LABEL[kind], exc)
            raise InfrastructureError(
                f"{_INDUSTRY_LOG_LABEL[kind]} import failed",
                detail=str(exc),
            ) from exc

    def apply_parsed_owl_to_domain(
        self,
        ontology_info: Dict[str, Any],
        classes: list,
        properties: list,
        constraints: list,
        swrl_rules: list,
        axioms: list,
        expressions: list = None,
        *,
        groups: list = None,
        name_fallback_to_domain: bool = True,
    ) -> str:
        """Write parse result into self._domain.ontology and save. Returns resolved ontology name."""
        file_name = ontology_info.get("label") or ontology_info.get("name") or ""
        if name_fallback_to_domain:
            default_name = self._domain.info.get("name", "")
            resolved_name = file_name or default_name
        else:
            resolved_name = file_name

        self._domain.ontology.update(
            {
                "name": resolved_name,
                # Prefer the (normalized, #/-terminated) namespace over the raw
                # ontology IRI, and never persist an empty/sentinel value — mirror
                # the RDFS import path so a file without an owl:Ontology header
                # gets DEFAULT_BASE_URI instead of "Unknown".
                "base_uri": (
                    ontology_info.get("namespace")
                    or ontology_info.get("uri")
                    or DEFAULT_BASE_URI
                ),
                "classes": classes,
                "properties": properties,
                "constraints": constraints,
                "swrl_rules": swrl_rules,
                "axioms": axioms,
                "expressions": expressions or [],
                "groups": groups or [],
            }
        )
        OntologyClassModel.sync_class_data_properties(self._domain.ontology)
        OntologyClassModel.finalize_class_attributes(self._domain.ontology)
        self._domain.save()
        return resolved_name

    def build_import_owl_success_payload(
        self,
        classes: list,
        properties: list,
        constraints: list,
    ) -> Dict[str, Any]:
        return {
            "success": True,
            "config": self._domain.ontology,
            "stats": {
                "classes": len(classes),
                "properties": len(properties),
                "constraints": len(constraints),
            },
        }

    def build_parse_owl_success_payload(
        self,
        ontology_info: Dict[str, Any],
        classes: list,
        properties: list,
        constraints: list,
        swrl_rules: list,
        axioms: list,
        expressions: list = None,
        resolved_name: str = "",
    ) -> Dict[str, Any]:
        return {
            "success": True,
            "ontology": {
                "info": {
                    "label": resolved_name,
                    "namespace": ontology_info.get("namespace")
                    or ontology_info.get("uri", ""),
                    "uri": ontology_info.get("uri", ""),
                },
                "classes": classes,
                "properties": properties,
            },
            "config": self._domain.ontology,
            "stats": {
                "classes": len(classes),
                "properties": len(properties),
                "constraints": len(constraints),
                "swrl_rules": len(swrl_rules),
                "axioms": len(axioms),
                "expressions": len(expressions or []),
            },
        }

    def build_load_owl_file_success_payload(
        self,
        classes: list,
        properties: list,
        constraints: list,
        swrl_rules: list,
        axioms: list,
        expressions: list = None,
    ) -> Dict[str, Any]:
        return {
            "success": True,
            "ontology": self._domain.ontology,
            "stats": {
                "classes": len(classes),
                "properties": len(properties),
                "constraints": len(constraints),
                "swrl_rules": len(swrl_rules),
                "axioms": len(axioms),
                "expressions": len(expressions or []),
            },
        }
