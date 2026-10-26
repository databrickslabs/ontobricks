"""Ontology management (non-HTTP).

Use :class:`Ontology` with a :class:`~back.objects.session.DomainSession` for
operations that persist to the session; use static methods for pure transforms.

Capability classes (Fowler Extract Class) own the bodies; this facade keeps
one-line delegators so ``Ontology.parse_owl`` / ``Ontology.add_class`` stay.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple

from back.core.w3c.shacl.constants import QUALITY_CATEGORIES
from shared.config.constants import DEFAULT_BASE_URI
from back.objects.ontology.ClassModel import ClassModel
from back.objects.ontology.Editor import Editor
from back.objects.ontology.EntityImport import EntityImport
from back.objects.ontology.Groups import Groups
from back.objects.ontology.Import import IndustryKind, Import
from back.objects.ontology.Owl import Owl
from back.objects.ontology.Rules import Rules

if TYPE_CHECKING:
    from agents.agent_auto_icon_assign.engine import (
        AgentResult as IconAssignAgentResult,
    )
    from agents.agent_business_rules_generator.engine import (
        AgentResult as BusinessRulesAgentResult,
    )
    from back.objects.session.DomainSession import DomainSession


class Ontology:
    """Ontology operations for the current domain session or as static helpers."""

    def __init__(self, session: "DomainSession") -> None:
        self._domain = session

    def generate_rules_with_agent(
        self,
        *,
        host: str,
        token: str,
        endpoint_name: str,
        options: Optional[Dict[str, Any]] = None,
        guidelines: str = "",
        selected_docs: Optional[List[str]] = None,
        warehouse_id: str = "",
        on_step: Optional[Callable[[str], None]] = None,
    ) -> "BusinessRulesAgentResult":
        """Run ``agent_business_rules_generator`` for this project (blocking).

        Feeds the live ontology design (classes/attributes + relationships) and
        the domain's uploaded documents to the agent, which proposes SWRL,
        decision-table, SPARQL, and aggregate rules for the user to review.

        Typical use: call from a background thread; poll task status from HTTP.
        """
        from agents.agent_business_rules_generator import run_agent

        s = self._domain
        ont = s.ontology
        base_uri = (
            ont.get("base_uri")
            or ont.get("info", {}).get("base_uri")
            or DEFAULT_BASE_URI
        )
        return run_agent(
            host=host,
            token=token,
            endpoint_name=endpoint_name,
            registry=dict(s.registry),
            ontology_design=self.agent_ontology_context(connected_only=True),
            base_uri=base_uri,
            options=options or {},
            guidelines=guidelines or "",
            domain_name=s.info.get("name", ""),
            domain_folder=s.domain_folder,
            domain_version=s.current_version,
            selected_docs=list(selected_docs or []),
            warehouse_id=warehouse_id or "",
            on_step=on_step,
        )

    def agent_ontology_context(
        self, connected_only: bool = False
    ) -> Dict[str, Any]:
        """Ontology snapshot for agents: entities (classes + attributes) + object-property rels.

        Args:
            connected_only: When True, drop entities that do not participate in
                any business relationship (object property) as domain or range.
                Entities related only through inheritance — or not at all — are
                excluded so the consuming agent never references them.
        """
        s = self._domain
        classes = s.get_classes()
        properties = s.get_properties()

        relationships = [
            {
                "name": p.get("name", ""),
                "domain": p.get("domain", ""),
                "range": p.get("range", ""),
            }
            for p in properties
            if p.get("type") in ("ObjectProperty", "owl:ObjectProperty", None)
        ]

        def _local(ref: str) -> str:
            return ref.rsplit("#", 1)[-1].rsplit("/", 1)[-1] if ref else ""

        entities = [
            {
                "name": c.get("name", ""),
                "uri": c.get("uri", ""),
                "attributes": [
                    dp.get("name", "") for dp in c.get("dataProperties", [])
                ],
            }
            for c in classes
        ]

        if connected_only:
            endpoints: Set[str] = set()
            for rel in relationships:
                for ref in (rel["domain"], rel["range"]):
                    if ref:
                        endpoints.add(ref.lower())
                        endpoints.add(_local(ref).lower())
            entities = [
                e
                for e in entities
                if (e["name"] and e["name"].lower() in endpoints)
                or (e["uri"] and e["uri"].lower() in endpoints)
                or (e["uri"] and _local(e["uri"]).lower() in endpoints)
            ]

        return {"entities": entities, "relationships": relationships}

    def assign_icons_with_agent(
        self,
        *,
        host: str,
        token: str,
        endpoint_name: str,
        entity_names: List[str],
        on_step: Optional[Callable[[str], None]] = None,
    ) -> "IconAssignAgentResult":
        """Run ``agent_auto_icon_assign`` for this project (blocking).

        Uses session ontology classes/properties and ``catalog_metadata`` as agent context.
        """
        from agents.agent_auto_icon_assign import run_agent

        return run_agent(
            host=host,
            token=token,
            endpoint_name=endpoint_name,
            entity_names=entity_names,
            metadata=self._domain.catalog_metadata,
            ontology=self.agent_ontology_context(),
            on_step=on_step,
        )

    @staticmethod
    def ensure_uris(config: Dict[str, Any]) -> Dict[str, Any]:
        return ClassModel.ensure_uris(config)

    @staticmethod
    def prune_orphaned_datatype_properties(config: Dict[str, Any]) -> int:
        return ClassModel.prune_orphaned_datatype_properties(config)

    @staticmethod
    def sync_class_data_properties(config: Dict[str, Any]) -> None:
        return ClassModel.sync_class_data_properties(config)

    @staticmethod
    def finalize_class_attributes(config: Dict[str, Any]) -> None:
        return ClassModel.finalize_class_attributes(config)

    @staticmethod
    def get_ontology_stats(config: Dict[str, Any]) -> Dict[str, int]:
        return Owl.get_ontology_stats(config)

    @staticmethod
    def normalize_property_domain_range(
        ontology_config: Dict[str, Any],
        *,
        on_replace: Optional[Callable[[Dict[str, Any], str, Any, Any], None]] = None,
    ) -> bool:
        return ClassModel.normalize_property_domain_range(ontology_config, on_replace=on_replace)

    def prune_mappings_to_ontology_uris(
        self,
        class_uris: Set[str],
        property_uris: Set[str],
    ) -> Dict[str, int]:
        return Editor(self._domain).prune_mappings_to_ontology_uris(class_uris, property_uris)

    @staticmethod
    def _diff_by_uri(
        old_list: Optional[List[Dict[str, Any]]],
        new_list: Optional[List[Dict[str, Any]]],
    ) -> Tuple[list, list, list]:
        return Editor._diff_by_uri(old_list, new_list)

    def _record_ontology_diff(
        self,
        old_classes: Optional[List[Dict[str, Any]]],
        new_classes: Optional[List[Dict[str, Any]]],
        old_props: Optional[List[Dict[str, Any]]],
        new_props: Optional[List[Dict[str, Any]]],
        *,
        source: str = "user",
    ) -> None:
        return Editor(self._domain)._record_ontology_diff(old_classes, new_classes, old_props, new_props, source=source)

    def save_ontology_config_from_editor(
        self, raw_body: Dict[str, Any]
    ) -> Dict[str, Any]:
        return Editor(self._domain).save_ontology_config_from_editor(raw_body)

    def _sync_design_layout_with_ontology(self) -> None:
        return Editor(self._domain)._sync_design_layout_with_ontology()

    def delete_class_by_uri(self, class_uri: Optional[str]) -> Dict[str, Any]:
        return Editor(self._domain).delete_class_by_uri(class_uri)

    def delete_property_by_uri(self, property_uri: Optional[str]) -> Dict[str, Any]:
        return Editor(self._domain).delete_property_by_uri(property_uri)

    def add_class(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return Editor(self._domain).add_class(data)

    def update_class(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return Editor(self._domain).update_class(data)

    def add_property(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return Editor(self._domain).add_property(data)

    def update_property(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return Editor(self._domain).update_property(data)

    def ingest_owl(
        self,
        owl_content: str,
        *,
        name_fallback_to_domain: bool = True,
        outcome: str = "import",
    ) -> Dict[str, Any]:
        return Import(self._domain).ingest_owl(owl_content, name_fallback_to_domain=name_fallback_to_domain, outcome=outcome)

    def apply_parsed_rdfs_to_domain(
        self,
        rdfs_content: str,
    ) -> Dict[str, Any]:
        return Import(self._domain).apply_parsed_rdfs_to_domain(rdfs_content)

    def _try_import_as_shacl(self, content: str) -> Optional[Dict[str, Any]]:
        return Import(self._domain)._try_import_as_shacl(content)

    def analyze_import(
        self,
        owl_content: str,
        *,
        format: str = "owl",
    ) -> Dict[str, Any]:
        return Import(self._domain).analyze_import(owl_content, format=format)

    def merge_parsed_owl_to_domain(
        self,
        owl_content: str,
        resolutions: Dict[str, str],
        *,
        format: str = "owl",
        name_fallback_to_domain: bool = True,
    ) -> Dict[str, Any]:
        return Import(self._domain).merge_parsed_owl_to_domain(owl_content, resolutions, format=format, name_fallback_to_domain=name_fallback_to_domain)

    def _merge_rdfs(
        self,
        rdfs_content: str,
        resolutions: Dict[str, str],
    ) -> Dict[str, Any]:
        return Import(self._domain)._merge_rdfs(rdfs_content, resolutions)

    @staticmethod
    def _apply_resolutions(
        entity_type: str,
        existing: list,
        report: ConflictReport,
        resolutions: Dict[str, str],
    ) -> list:
        return Import._apply_resolutions(entity_type, existing, report, resolutions)

    def rename_relationship_references(
        self, old_name: str, new_name: str
    ) -> Dict[str, int]:
        return Editor(self._domain).rename_relationship_references(old_name, new_name)

    def apply_agent_ontology_changes(
        self,
        classes: List[Dict[str, Any]],
        properties: List[Dict[str, Any]],
        *,
        prune_orphan_mappings: bool = True,
    ) -> Dict[str, Any]:
        """Normalize + persist ontology from an assistant agent result.

        Returns the config dict suitable for ``response["config"]``.
        """
        s = self._domain
        old_classes = list(s.get_classes())
        old_props = list(s.get_properties())
        base_uri = s.ontology.get("base_uri") or DEFAULT_BASE_URI
        ontology_config = {
            "name": s.ontology.get("name", ""),
            "base_uri": base_uri,
            "description": s.ontology.get("description", ""),
            "classes": classes,
            "properties": properties,
        }
        ontology_config = Ontology.ensure_uris(ontology_config)

        if prune_orphan_mappings:
            new_class_uris = {
                c.get("uri") for c in ontology_config["classes"] if c.get("uri")
            }
            new_property_uris = {
                p.get("uri") for p in ontology_config["properties"] if p.get("uri")
            }
            self.prune_mappings_to_ontology_uris(new_class_uris, new_property_uris)

        s.clear_generated_content()
        s.ontology.update(
            {
                "classes": ontology_config["classes"],
                "properties": ontology_config["properties"],
            }
        )
        self._record_ontology_diff(
            old_classes,
            ontology_config["classes"],
            old_props,
            ontology_config["properties"],
            source="agent",
        )
        s.save()

        return {
            "name": s.ontology.get("name", ""),
            "base_uri": base_uri,
            "description": s.ontology.get("description", ""),
            "classes": ontology_config["classes"],
            "properties": ontology_config["properties"],
        }

    @staticmethod
    def validate_swrl_rule(rule: Dict[str, Any]) -> List[str]:
        return Rules.validate_swrl_rule(rule)

    _SWRL_ATOM_RE = Rules._SWRL_ATOM_RE

    _SWRL_BUILTIN_PREFIXES = Rules._SWRL_BUILTIN_PREFIXES

    @staticmethod
    def swrl_reference_errors(
        rule: Dict[str, Any],
        class_names: Set[str],
        property_names: Set[str],
    ) -> List[str]:
        return Rules.swrl_reference_errors(rule, class_names, property_names)

    @staticmethod
    def _ref_local_name(term: str):
        return Rules._ref_local_name(term)

    @staticmethod
    def decision_table_reference_errors(
        rule: Dict[str, Any], class_names: Set[str], property_names: Set[str]
    ) -> List[str]:
        return Rules.decision_table_reference_errors(rule, class_names, property_names)

    @staticmethod
    def aggregate_reference_errors(
        rule: Dict[str, Any], class_names: Set[str], property_names: Set[str]
    ) -> List[str]:
        return Rules.aggregate_reference_errors(rule, class_names, property_names)

    @staticmethod
    def sparql_reference_errors(
        rule: Dict[str, Any], class_names: Set[str], property_names: Set[str]
    ) -> List[str]:
        return Rules.sparql_reference_errors(rule, class_names, property_names)

    @staticmethod
    def rule_reference_errors(
        key: str,
        rule: Dict[str, Any],
        class_names: Set[str],
        property_names: Set[str],
    ) -> List[str]:
        return Rules.rule_reference_errors(key, rule, class_names, property_names)

    @staticmethod
    def merge_icon_suggestions(
        entity_names: List[str], icons: Dict[str, str]
    ) -> Dict[str, str]:
        """Case-insensitive merge of agent icon suggestions into a final map."""
        normalized: Dict[str, str] = {}
        for key, emoji in icons.items():
            normalized[key] = emoji
            normalized[key.lower()] = emoji
        return {
            name: emoji
            for name in entity_names
            if (emoji := normalized.get(name) or normalized.get(name.lower()))
        }

    @staticmethod
    def postprocess_generated_owl(content: str) -> tuple:
        return Owl.postprocess_generated_owl(content)

    @staticmethod
    def build_class_from_data(
        data: Dict[str, Any], existing: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        return ClassModel.build_class_from_data(data, existing)

    @staticmethod
    def build_property_from_data(
        data: Dict[str, Any], existing: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        return ClassModel.build_property_from_data(data, existing)

    @staticmethod
    def validate_constraint(constraint: Dict[str, Any]) -> Optional[str]:
        return Rules.validate_constraint(constraint)

    @staticmethod
    def validate_shape(shape: Dict[str, Any]) -> Optional[str]:
        return Rules.validate_shape(shape)

    @staticmethod
    def validate_classes(classes: List[Dict[str, Any]]) -> tuple:
        return ClassModel.validate_classes(classes)

    @staticmethod
    def generate_shacl(shapes: list, base_uri: str = "") -> str:
        return Rules.generate_shacl(shapes, base_uri)

    @staticmethod
    def generate_owl(
        data,
        constraints=None,
        swrl_rules=None,
        axioms=None,
        expressions=None,
        groups=None,
    ):
        return Owl.generate_owl(data, constraints, swrl_rules, axioms, expressions, groups)

    @staticmethod
    def parse_owl(content, extract_advanced=True):
        return Owl.parse_owl(content, extract_advanced)

    @staticmethod
    def parse_rdfs(content):
        return Owl.parse_rdfs(content)

    def import_industry_ontology(
        self,
        kind: IndustryKind,
        domain_keys: List[str],
        version: Optional[str] = None,
    ) -> Dict[str, Any]:
        return Import(self._domain).import_industry_ontology(kind, domain_keys, version)

    def import_entities_from_domain(
        self,
        source: Dict[str, Any],
        names: List[str],
        renames: Dict[str, str],
        *,
        source_domain: str,
        source_version: str,
    ) -> Dict[str, Any]:
        return EntityImport(self._domain).import_entities(source, names, renames, source_domain=source_domain, source_version=source_version)

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
        return Import(self._domain).apply_parsed_owl_to_domain(ontology_info, classes, properties, constraints, swrl_rules, axioms, expressions, groups=groups, name_fallback_to_domain=name_fallback_to_domain)

    def build_import_owl_success_payload(
        self,
        classes: list,
        properties: list,
        constraints: list,
    ) -> Dict[str, Any]:
        return Import(self._domain).build_import_owl_success_payload(classes, properties, constraints)

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
        return Import(self._domain).build_parse_owl_success_payload(ontology_info, classes, properties, constraints, swrl_rules, axioms, expressions, resolved_name)

    def build_load_owl_file_success_payload(
        self,
        classes: list,
        properties: list,
        constraints: list,
        swrl_rules: list,
        axioms: list,
        expressions: list = None,
    ) -> Dict[str, Any]:
        return Import(self._domain).build_load_owl_file_success_payload(classes, properties, constraints, swrl_rules, axioms, expressions)

    @staticmethod
    def _turtle_to_camel(words: list, is_pascal: bool) -> str:
        return Owl._turtle_to_camel(words, is_pascal)

    @staticmethod
    def _fix_snake_kebab_local_names(content: str) -> str:
        return Owl._fix_snake_kebab_local_names(content)

    @staticmethod
    def _fix_spaced_local_names(content: str) -> str:
        return Owl._fix_spaced_local_names(content)

    @staticmethod
    def _fix_local_names(content: str) -> str:
        return Owl._fix_local_names(content)

    @staticmethod
    def clean_owl_output(content: str) -> str:
        return Owl.clean_owl_output(content)

    def save_group(self, group: Dict, index: int = -1) -> List[Dict]:
        return Groups(self._domain).save_group(group, index)

    def delete_group(self, *, index: int = -1, name: str = "") -> List[Dict]:
        return Groups(self._domain).delete_group(index=index, name=name)

    def update_group_members(
        self, group_name: str, *, add: List[str] = None, remove: List[str] = None
    ) -> List[Dict]:
        return Groups(self._domain).update_group_members(group_name, add=add, remove=remove)

    @staticmethod
    def _enforce_exclusive_membership(
        groups: List[Dict], authoritative_group_name: str
    ) -> None:
        return Groups._enforce_exclusive_membership(groups, authoritative_group_name)

    def _sync_class_group_field(self, groups: List[Dict]) -> None:
        return Groups(self._domain)._sync_class_group_field(groups)

    @staticmethod
    def calculate_owl_stats(owl_content: str) -> Dict:
        return Owl.calculate_owl_stats(owl_content)
