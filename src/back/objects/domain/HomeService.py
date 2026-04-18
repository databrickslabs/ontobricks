"""Home page validation and status orchestration (routes call this class directly)."""

from __future__ import annotations

import asyncio
from typing import Any, Dict

from back.core.helpers import run_blocking
from back.core.logging import get_logger

logger = get_logger(__name__)


class HomeService:
    """Session status, ontology/mapping validation, navbar state, detailed validation."""

    @staticmethod
    def get_session_status(domain) -> Dict[str, Any]:
        """Get current session status for navbar indicators.

        Delegates to :meth:`DomainSession.get_session_status`.
        """
        return domain.get_session_status()

    @staticmethod
    def _active_classes_and_properties(domain):
        """Return (active_classes, active_object_properties, all_object_properties, excluded_names).

        Excludes entities/relationships marked as excluded.
        The ``excluded`` flag lives on each entry in ``entities``
        and ``relationships``.  Relationships are also excluded
        implicitly when their domain or range class is excluded.
        """
        all_classes = domain.get_classes()
        all_properties = domain.get_properties()
        assignment = domain.assignment or {}

        excluded_entity_uris = {
            m.get("ontology_class")
            for m in assignment.get("entities", [])
            if m.get("excluded")
        }
        excluded_rel_uris = {
            m.get("property")
            for m in assignment.get("relationships", [])
            if m.get("excluded")
        }

        active_classes = [
            c for c in all_classes if c.get("uri") not in excluded_entity_uris
        ]
        excluded_names = set()
        for c in all_classes:
            if c.get("uri") in excluded_entity_uris:
                if c.get("name"):
                    excluded_names.add(c["name"])
                if c.get("localName"):
                    excluded_names.add(c["localName"])

        object_properties = [
            p for p in all_properties if p.get("type") == "ObjectProperty"
        ]
        active_props = [
            p
            for p in object_properties
            if p.get("uri") not in excluded_rel_uris
            and p.get("domain") not in excluded_names
            and p.get("range") not in excluded_names
        ]
        return active_classes, active_props, object_properties, excluded_names

    @staticmethod
    def validate_status(domain) -> Dict[str, Any]:
        """Get validation status for navbar indicators (computed on each call).

        Args:
            domain: DomainSession instance

        Returns:
            dict: Complete validation status
        """
        return HomeService._compute_validation(domain)

    @staticmethod
    def _validate_ontology_classes(all_classes) -> tuple:
        """Delegates to :meth:`Ontology.validate_classes`."""
        from back.objects.ontology import Ontology

        return Ontology.validate_classes(all_classes)

    @staticmethod
    def _compute_mapping_gaps(
        active_classes, active_props, active_entity_mappings, active_rel_mappings
    ):
        """Delegates to :meth:`Mapping.compute_mapping_gaps`."""
        from back.objects.mapping import Mapping

        return Mapping.compute_mapping_gaps(
            active_classes,
            active_props,
            active_entity_mappings,
            active_rel_mappings,
        )

    @staticmethod
    def _build_mapping_issues(
        active_classes,
        active_props,
        active_entity_mappings,
        active_rel_mappings,
        unmapped_entity_count,
        unmapped_rel_count,
        unmapped_attr_count,
    ):
        """Delegates to :meth:`Mapping.build_mapping_issues`."""
        from back.objects.mapping import Mapping

        return Mapping.build_mapping_issues(
            active_classes,
            active_props,
            active_entity_mappings,
            active_rel_mappings,
            unmapped_entity_count,
            unmapped_rel_count,
            unmapped_attr_count,
        )

    @staticmethod
    def _compute_validation(domain) -> Dict[str, Any]:
        """Run the full ontology + mapping validation rules."""
        all_classes = domain.get_classes()
        all_properties = domain.get_properties()
        active_classes, active_props, all_obj_props, _ = (
            HomeService._active_classes_and_properties(domain)
        )
        entity_mappings = domain.get_entity_mappings()
        relationship_mappings = domain.get_relationship_mappings()

        active_entity_mappings = [m for m in entity_mappings if not m.get("excluded")]
        active_rel_mappings = [
            m for m in relationship_mappings if not m.get("excluded")
        ]
        ignored_entity_count = len(all_classes) - len(active_classes)
        ignored_rel_count = len(all_obj_props) - len(active_props)

        ontology_valid, ontology_issues = HomeService._validate_ontology_classes(
            all_classes
        )

        unmapped_ent, unmapped_rel, unmapped_attr, *_ = (
            HomeService._compute_mapping_gaps(
                active_classes,
                active_props,
                active_entity_mappings,
                active_rel_mappings,
            )
        )
        mapping_issues = HomeService._build_mapping_issues(
            active_classes,
            active_props,
            active_entity_mappings,
            active_rel_mappings,
            len(unmapped_ent),
            len(unmapped_rel),
            len(unmapped_attr),
        )

        mapping_valid = (
            len(active_classes) > 0
            and not unmapped_ent
            and not unmapped_rel
            and not unmapped_attr
        )
        mapped_entity_count = len(active_classes) - len(unmapped_ent)
        mapped_rel_count = len(active_props) - len(unmapped_rel)

        datatype_props = [
            p for p in all_properties if p.get("type") != "ObjectProperty"
        ]
        object_props = [p for p in all_properties if p.get("type") == "ObjectProperty"]

        return {
            "ontology_valid": ontology_valid,
            "ontology_issues": ontology_issues,
            "ontology_stats": {
                "classes": len(all_classes),
                "properties": len(all_properties),
                "attributes": len(datatype_props),
                "object_properties": len(object_props),
            },
            "ontology_changed": domain.ontology_changed,
            "mapping_valid": mapping_valid,
            "mapping_issues": mapping_issues,
            "mapping_stats": {
                "entities": mapped_entity_count,
                "relationships": mapped_rel_count,
                "total_classes": len(active_classes),
                "total_properties": len(active_props),
                "ignored_entities": ignored_entity_count,
                "ignored_relationships": ignored_rel_count,
            },
            "assignment_changed": domain.assignment_changed,
        }

    @staticmethod
    def validate_ontology(domain) -> Dict[str, Any]:
        """Validate current ontology.

        Args:
            domain: DomainSession instance

        Returns:
            dict: Validation result with errors
        """
        classes = domain.get_classes()
        properties = domain.get_properties()

        if not classes:
            return {
                "valid": False,
                "message": "No ontology loaded",
                "errors": ["No classes defined"],
            }

        errors = []
        for cls in classes:
            # Lenient: accept uri, name, or localName
            if not cls.get("uri") and not cls.get("name") and not cls.get("localName"):
                label = cls.get("label", cls.get("name", "Unknown"))
                errors.append(f"Entity '{label}' has no URI")

        for prop in properties:
            if (
                not prop.get("uri")
                and not prop.get("name")
                and not prop.get("localName")
            ):
                label = prop.get("label", prop.get("name", "Unknown"))
                errors.append(f"Relationship '{label}' has no URI")

        return {
            "valid": len(errors) == 0,
            "message": "Valid" if not errors else f"{len(errors)} issues found",
            "errors": errors,
        }

    @staticmethod
    async def get_detailed_validation(
        domain, settings, warehouse_id: str = ""
    ) -> Dict[str, Any]:
        """Get detailed validation status.

        Digital Twin graph status and artefact existence are served from the
        session cache when available (populated after each successful build).
        On a cache miss the values are fetched live and then cached.

        Args:
            domain: DomainSession instance
            settings: App settings (Databricks / warehouse resolution)
            warehouse_id: resolved warehouse ID (empty string if none)

        Returns:
            dict: Comprehensive validation details
        """
        from back.objects.domain.Domain import Domain
        from back.objects.digitaltwin import DigitalTwin

        dt = DigitalTwin(domain)
        await run_blocking(dt.sync_last_build_from_schedule, settings)
        ts_status, dt_exist, document_count = await asyncio.gather(
            dt.get_or_fetch_graph_status(settings),
            dt.get_or_fetch_dt_existence(settings),
            run_blocking(Domain(domain).count_documents_in_volume, settings),
        )

        all_classes = domain.get_classes()
        all_properties = domain.get_properties()
        active_classes, active_props, all_obj_props, _ = (
            HomeService._active_classes_and_properties(domain)
        )
        entity_mappings = domain.get_entity_mappings()
        relationship_mappings = domain.get_relationship_mappings()
        design_layout = domain.design_layout

        active_entity_mappings = [m for m in entity_mappings if not m.get("excluded")]
        active_relationship_mappings = [
            m for m in relationship_mappings if not m.get("excluded")
        ]
        ignored_entity_count = len(all_classes) - len(active_classes)
        ignored_rel_count = len(all_obj_props) - len(active_props)

        ontology_valid, ontology_errors = HomeService._validate_ontology_classes(
            all_classes
        )

        unmapped_entities, unmapped_relationships, unmapped_attributes, *_ = (
            HomeService._compute_mapping_gaps(
                active_classes,
                active_props,
                active_entity_mappings,
                active_relationship_mappings,
            )
        )

        has_design = bool(design_layout and design_layout.get("views"))

        mapping_issues = HomeService._build_mapping_issues(
            active_classes,
            active_props,
            active_entity_mappings,
            active_relationship_mappings,
            len(unmapped_entities),
            len(unmapped_relationships),
            len(unmapped_attributes),
        )
        mapping_warnings = []

        # Mapping is complete ONLY if ALL active classes, ObjectProperties, and attributes are mapped
        all_classes_mapped = len(unmapped_entities) == 0 and len(active_classes) > 0
        all_properties_mapped = len(unmapped_relationships) == 0
        all_attributes_mapped = len(unmapped_attributes) == 0
        mapping_complete = (
            all_classes_mapped and all_properties_mapped and all_attributes_mapped
        )
        mapping_valid = mapping_complete and len(mapping_issues) == 0

        mapped_entity_count = len(active_classes) - len(unmapped_entities)
        mapped_rel_count = len(active_props) - len(unmapped_relationships)

        # Ontology warnings
        ontology_warnings = []
        ontology_name = domain.ontology.get("name", "")
        if not ontology_name or ontology_name == "MyOntology":
            ontology_warnings.append("Ontology name is still set to default")
        object_props = [p for p in active_props if p.get("type") != "DatatypeProperty"]
        if len(object_props) == 0:
            ontology_warnings.append("No relationships defined")
        props_without_domain = [p for p in object_props if not p.get("domain")]
        if props_without_domain:
            names = ", ".join(
                p.get("label") or p.get("name", "(unnamed)")
                for p in props_without_domain
            )
            ontology_warnings.append(
                f"{len(props_without_domain)} relationship(s) without domain: {names}"
            )

        dtwin = HomeService._compute_dtwin_indicator(domain, ts_status, dt_exist)

        dt_existence = dt_exist or {}
        ts_st = ts_status or {}

        last_update = domain.last_update
        last_build = domain.last_build
        needs_rebuild = bool(last_update and last_build and last_update > last_build)

        dtwin_detail = {
            **dtwin,
            "view_exists": dt_existence.get("view_exists"),
            "view_table": dt_existence.get("view_table", ""),
            "graph_name": dt_existence.get("graph_name", ""),
            "local_lbug_exists": dt_existence.get("local_lbug_exists", False),
            "registry_lbug_exists": dt_existence.get("registry_lbug_exists"),
            "snapshot_table": dt_existence.get("snapshot_table", ""),
            "snapshot_exists": dt_existence.get("snapshot_exists"),
            "last_built": last_build,
            "last_update": last_update,
            "has_data": ts_st.get("has_data", False),
            "triple_count": ts_st.get("count", 0),
            "needs_rebuild": needs_rebuild,
        }

        # Metadata table count (in-session, no I/O)
        _dom = domain._data.get("domain") or domain._data.get("project") or {}
        metadata = _dom.get("metadata", {})
        metadata_table_count = len(metadata.get("tables", [])) if metadata else 0

        # Document count: live list of UC documents path (same rules as domain documents list)

        domain_info = domain.info or {}
        dname = domain_info.get("name", "NewDomain")
        dversion = domain.current_version or "1"

        detail_datatype_props = [
            p for p in all_properties if p.get("type") != "ObjectProperty"
        ]

        return {
            "success": True,
            # Flat format expected by the JavaScript
            "ontology_valid": ontology_valid,
            "mapping_valid": mapping_valid,
            "mapping_complete": mapping_complete,
            "ontology_stats": {
                "classes": len(all_classes),
                "properties": len(active_props),
                "attributes": len(detail_datatype_props),
                "object_properties": len(active_props),
            },
            "mapping_stats": {
                "entities": mapped_entity_count,
                "relationships": mapped_rel_count,
                "total_classes": len(active_classes),
                "total_properties": len(active_props),
                "ignored_entities": ignored_entity_count,
                "ignored_relationships": ignored_rel_count,
            },
            "ontology_issues": ontology_errors,
            "ontology_warnings": ontology_warnings,
            "mapping_issues": mapping_issues,
            "mapping_warnings": mapping_warnings,
            "unmapped_entities": unmapped_entities,
            "unmapped_relationships": unmapped_relationships,
            "unmapped_attributes": unmapped_attributes,
            # Digital Twin status (summary + detail)
            "dtwin": dtwin_detail,
            # Warehouse status
            "warehouse": {"warehouse_id": warehouse_id},
            "metadata_table_count": metadata_table_count,
            "document_count": document_count,
            "domain_name": dname,
            "domain_version": dversion,
            # Also include nested format for other consumers
            "ontology": {
                "valid": ontology_valid,
                "entity_count": len(all_classes),
                "relationship_count": len(active_props),
                "errors": ontology_errors,
            },
            "mapping": {
                "valid": mapping_valid,
                "complete": mapping_complete,
                "entity_count": mapped_entity_count,
                "relationship_count": mapped_rel_count,
            },
            "design": {"valid": has_design, "has_layout": has_design},
            "ready_for_query": ontology_valid and mapping_complete,
        }

    @staticmethod
    def _compute_dtwin_indicator(
        domain,
        ts_status: Dict[str, Any],
        dt_exist: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Derive a three-state Digital Twin indicator.

        Delegates to :meth:`DigitalTwin.compute_dtwin_indicator`.
        """
        from back.objects.digitaltwin import DigitalTwin

        return DigitalTwin.compute_dtwin_indicator(domain, ts_status, dt_exist)

    @staticmethod
    async def get_navbar_state(
        domain,
        settings,
        warehouse_id: str = "",
    ) -> Dict[str, Any]:
        """Build the navbar state in a single call.

        The navbar only displays domain identity and SQL Warehouse status,
        so this intentionally skips the expensive Digital Twin and validation
        I/O that used to run here.  Those checks are available on the
        validation page (``/validate/detailed``) and the DT Sync
        page (``/dtwin/sync/info``).
        """
        from back.objects.domain.Domain import Domain

        logger.debug("Building navbar state")
        domain_data = Domain(domain, settings).get_domain_info()

        return {
            "domain": domain_data,
            "warehouse": {"warehouse_id": warehouse_id},
        }
