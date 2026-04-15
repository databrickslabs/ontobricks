"""
Internal API -- Ontology JSON endpoints.

Moved from app/frontend/ontology/routes.py during the front/back split.
"""
from fastapi import APIRouter, Request, Depends

from back.core.errors import ValidationError, InfrastructureError
from back.objects.session import SessionManager, get_session_manager
from shared.config.settings import get_settings, Settings
from back.objects.ontology import Ontology
from back.core.databricks import VolumeFileService
from back.objects.session import get_domain
from back.core.task_manager import get_task_manager
from back.core.helpers import get_databricks_host_and_token, require_serving_llm
from agents.serialization import serialize_agent_steps
from back.core.industry import (
    get_fibo_catalog,
    get_cdisc_catalog,
    get_iof_catalog,
)
from back.core.logging import get_logger
from back.core.w3c import SHACLService
from shared.config.constants import DEFAULT_BASE_URI, DEFAULT_GRAPH_NAME

router = APIRouter(prefix="/ontology", tags=["Ontology"])
logger = get_logger(__name__)


# ===========================================
# Ontology CRUD API Routes
# ===========================================

@router.get("/load")
async def load_ontology(session_mgr: SessionManager = Depends(get_session_manager)):
    """Load ontology from session."""
    domain = get_domain(session_mgr)
    classes = domain.get_classes()
    properties = domain.get_properties()
    logger.debug("/ontology/load: ontology name=%s, classes=%s, properties=%s", domain.ontology.get('name', ''), len(classes), len(properties))
    if classes:
        logger.debug("/ontology/load: first class=%s", classes[0].get('name', 'unknown'))

    if Ontology.normalize_property_domain_range({"classes": classes, "properties": properties}):
        logger.info("/ontology/load: fixed domain/range case mismatches in properties")
        domain.ontology["properties"] = properties
        domain.save()

    ontology_name = domain.info.get('name', '').lower() or domain.ontology.get('name', '')
    return {
        'success': True,
        'config': {
            'name': ontology_name,
            'base_uri': domain.ontology.get('base_uri', ''),
            'description': domain.ontology.get('description', ''),
            'classes': classes,
            'properties': properties
        }
    }


@router.post("/save")
async def save_ontology(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Save ontology to session and clean up orphaned mappings."""
    data = await request.json()
    domain = get_domain(session_mgr)
    return Ontology(domain).save_ontology_config_from_editor(data)


@router.post("/reset")
async def reset_ontology(session_mgr: SessionManager = Depends(get_session_manager)):
    """Reset ontology, associated mappings, and design layout."""
    domain = get_domain(session_mgr)
    domain.reset_ontology()
    logger.info("Ontology, mappings, and design layout reset")
    return {'success': True, 'message': 'Ontology, mappings, and layout reset'}


# ===========================================
# Class (Entity) Management
# ===========================================

@router.post("/class/add")
async def add_class(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Add a class to the ontology."""
    data = await request.json()
    return Ontology(get_domain(session_mgr)).add_class(data)


@router.post("/class/update")
async def update_class(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Update a class in the ontology."""
    data = await request.json()
    return Ontology(get_domain(session_mgr)).update_class(data)


@router.post("/class/delete")
async def delete_class(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Delete a class from the ontology and its associated mappings."""
    data = await request.json()
    domain = get_domain(session_mgr)
    return Ontology(domain).delete_class_by_uri(data.get("uri"))


# ===========================================
# Property (Relationship) Management
# ===========================================

@router.post("/property/add")
async def add_property(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Add a property (relationship) to the ontology."""
    data = await request.json()
    return Ontology(get_domain(session_mgr)).add_property(data)


@router.post("/property/update")
async def update_property(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Update a property in the ontology."""
    data = await request.json()
    return Ontology(get_domain(session_mgr)).update_property(data)


@router.post("/property/delete")
async def delete_property(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Delete a property from the ontology and its associated mappings."""
    data = await request.json()
    domain = get_domain(session_mgr)
    return Ontology(domain).delete_property_by_uri(data.get("uri"))


# ===========================================
# OWL Generation & Import/Export
# ===========================================

@router.post("/generate-owl")
async def generate_owl_endpoint(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Generate OWL from ontology configuration."""
    data = await request.json()
    if not data:
        return {'success': False, 'message': 'No data provided'}
    
    try:
        domain = get_domain(session_mgr)
        constraints = data.get('constraints') or domain.constraints
        swrl_rules = data.get('swrl_rules') or domain.swrl_rules
        axioms = data.get('axioms') or domain.axioms
        expressions = data.get('expressions') or domain.expressions
        groups = data.get('groups') or domain.groups
        
        owl_content = Ontology.generate_owl(data, constraints, swrl_rules, axioms, expressions, groups)
        domain.generated['owl'] = owl_content
        domain.save()
        
        return {'success': True, 'owl': owl_content, 'format': 'turtle'}
    except Exception as e:
        logger.exception("OWL generation failed: %s", e)
        return {'success': False, 'message': str(e)}


@router.post("/import-owl")
async def import_owl(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Import ontology from OWL/TTL content."""
    data = await request.json()
    owl_content = data.get('content', '')
    if not owl_content:
        return {'success': False, 'message': 'No OWL content provided'}
    try:
        return Ontology(get_domain(session_mgr)).ingest_owl(
            owl_content, name_fallback_to_domain=True, outcome="import",
        )
    except Exception as e:
        logger.exception("OWL import failed: %s", e)
        return {'success': False, 'message': str(e)}


@router.get("/export-owl")
async def export_owl(session_mgr: SessionManager = Depends(get_session_manager)):
    """Export ontology to OWL/TTL format."""
    domain = get_domain(session_mgr)
    
    if not domain.get_classes():
        return {'success': False, 'message': 'No ontology to export'}
    
    try:
        owl_content = Ontology.generate_owl(
            domain.ontology,
            domain.constraints,
            domain.swrl_rules,
            domain.axioms,
            domain.expressions,
            domain.groups,
        )
        return {'success': True, 'owl_content': owl_content, 'format': 'turtle'}
    except Exception as e:
        logger.exception("OWL export failed: %s", e)
        return {'success': False, 'message': str(e)}


@router.get("/get-loaded-ontology")
async def get_loaded_ontology(session_mgr: SessionManager = Depends(get_session_manager)):
    """Get currently loaded ontology from session."""
    domain = get_domain(session_mgr)
    if domain.get_classes():
        return {'success': True, 'ontology': domain.ontology}
    return {'success': False, 'message': 'No ontology loaded'}


@router.post("/parse-owl")
async def parse_owl_content(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Parse OWL content and store in session."""
    data = await request.json()
    owl_content = data.get('content', '')
    if not owl_content:
        return {'success': False, 'message': 'No OWL content provided'}
    try:
        return Ontology(get_domain(session_mgr)).ingest_owl(
            owl_content, name_fallback_to_domain=True, outcome="parse",
        )
    except Exception as e:
        logger.exception("Parse OWL content failed: %s", e)
        return {'success': False, 'message': str(e)}


@router.post("/parse-rdfs")
async def parse_rdfs_content(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Parse RDFS content and store in session."""
    data = await request.json()
    rdfs_content = data.get('content', '')
    if not rdfs_content:
        return {'success': False, 'message': 'No RDFS content provided'}
    try:
        return Ontology(get_domain(session_mgr)).apply_parsed_rdfs_to_domain(rdfs_content)
    except Exception as e:
        logger.exception("Parse RDFS content failed: %s", e)
        return {'success': False, 'message': str(e)}


# ===========================================
# Industry Ontology Import (FIBO, CDISC, IOF)
# ===========================================

_INDUSTRY_CATALOGS = {
    "fibo": get_fibo_catalog,
    "cdisc": get_cdisc_catalog,
    "iof": get_iof_catalog,
}


@router.get("/{kind}-catalog")
async def industry_catalog(kind: str):
    """Return a domain catalog for the given industry standard."""
    catalog_fn = _INDUSTRY_CATALOGS.get(kind)
    if not catalog_fn:
        return {"success": False, "message": f"Unknown industry kind: {kind}"}
    return {"success": True, "catalog": catalog_fn()}


@router.post("/import-{kind}")
async def import_industry(kind: str, request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Fetch industry domain modules, merge, parse, and store in session.

    Expects JSON body: ``{ "domains": ["FND", "BE", ...] }``
    """
    if kind not in _INDUSTRY_CATALOGS:
        raise ValidationError(f"Unknown industry kind: {kind}")
    data = await request.json()
    domain_keys = data.get("domains", [])
    domain = get_domain(session_mgr)
    return Ontology(domain).import_industry_ontology(kind, domain_keys)


# ===========================================
# Legacy Constraints (kept for backward-compat)
# ===========================================

@router.get("/constraints/list")
async def list_constraints(session_mgr: SessionManager = Depends(get_session_manager)):
    """Get list of legacy constraints from session."""
    domain = get_domain(session_mgr)
    return {'success': True, 'constraints': domain.constraints}


# ===========================================
# Data Quality (SHACL Shapes)
# ===========================================

@router.get("/dataquality/list")
async def list_shapes(
    session_mgr: SessionManager = Depends(get_session_manager),
    category: str = "",
):
    """List all SHACL data-quality shapes, optionally filtered by category."""
    domain = get_domain(session_mgr)
    domain.deduplicate_shacl_shapes()
    shapes = domain.shacl_shapes
    if category:
        shapes = [s for s in shapes if s.get("category") == category]
    return {"success": True, "shapes": shapes}


@router.post("/dataquality/save")
async def save_shape(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Add or update a SHACL data-quality shape."""
    try:
        data = await request.json()
        shape_data = data.get("shape", {})
        shape_id = shape_data.get("id", "")

        error = Ontology.validate_shape(shape_data)
        if error:
            return {"success": False, "message": error}

        domain = get_domain(session_mgr)
        shapes = list(domain.shacl_shapes)

        if shape_id and any(s["id"] == shape_id for s in shapes):
            shapes = SHACLService.update_shape(shapes, shape_id, shape_data)
        else:
            new_shape = SHACLService.create_shape(
                category=shape_data.get("category", "conformance"),
                target_class=shape_data.get("target_class", ""),
                target_class_uri=shape_data.get("target_class_uri", ""),
                property_path=shape_data.get("property_path", ""),
                property_uri=shape_data.get("property_uri", ""),
                shacl_type=shape_data.get("shacl_type", "sh:minCount"),
                parameters=shape_data.get("parameters", {}),
                severity=shape_data.get("severity", "sh:Violation"),
                message=shape_data.get("message", ""),
                label=shape_data.get("label", ""),
                enabled=shape_data.get("enabled", True),
            )
            shapes.append(new_shape)

        domain.shacl_shapes = shapes
        domain.save()
        return {"success": True, "message": "Shape saved", "shapes": shapes}
    except Exception as e:
        logger.exception("Save shape failed: %s", e)
        return {"success": False, "message": str(e)}


@router.post("/dataquality/delete")
async def delete_shape(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Delete a SHACL data-quality shape by id."""
    try:
        data = await request.json()
        shape_id = data.get("id", "")
        if not shape_id:
            return {"success": False, "message": "Shape id is required"}

        domain = get_domain(session_mgr)
        shapes = SHACLService.delete_shape(domain.shacl_shapes, shape_id)
        domain.shacl_shapes = shapes
        domain.save()
        return {"success": True, "message": "Shape deleted", "shapes": shapes}
    except Exception as e:
        logger.exception("Delete shape failed: %s", e)
        return {"success": False, "message": str(e)}


@router.get("/dataquality/turtle")
async def get_shacl_turtle(session_mgr: SessionManager = Depends(get_session_manager)):
    """Generate and return the SHACL Turtle for all shapes."""
    try:
        domain = get_domain(session_mgr)
        turtle = Ontology.generate_shacl(
            domain.shacl_shapes,
            domain.ontology.get("base_uri", ""),
        )
        return {"success": True, "turtle": turtle}
    except Exception as e:
        logger.exception("Generate SHACL turtle failed: %s", e)
        return {"success": False, "message": str(e)}


@router.post("/dataquality/import")
async def import_shacl(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Import SHACL shapes from Turtle content."""
    try:
        data = await request.json()
        turtle_content = data.get("turtle", "")
        if not turtle_content:
            return {"success": False, "message": "No Turtle content provided"}

        svc = SHACLService()
        imported = svc.import_shapes(turtle_content)
        if not imported:
            return {"success": False, "message": "No valid shapes found in the provided Turtle"}

        domain = get_domain(session_mgr)
        shapes = list(domain.shacl_shapes)
        shapes.extend(imported)
        domain.shacl_shapes = shapes
        domain.save()
        return {
            "success": True,
            "message": f"Imported {len(imported)} shapes",
            "shapes": shapes,
            "imported_count": len(imported),
        }
    except Exception as e:
        logger.exception("Import SHACL failed: %s", e)
        return {"success": False, "message": str(e)}


@router.get("/dataquality/export")
async def export_shacl(session_mgr: SessionManager = Depends(get_session_manager)):
    """Download SHACL shapes as a Turtle file."""
    from fastapi.responses import Response

    domain = get_domain(session_mgr)
    turtle = Ontology.generate_shacl(
        domain.shacl_shapes,
        domain.ontology.get("base_uri", ""),
    )
    export_name = domain._data.get("domain", domain._data.get("project", {})).get("info", {}).get("name", DEFAULT_GRAPH_NAME)
    filename = f"{export_name}_shacl_shapes.ttl"
    return Response(
        content=turtle,
        media_type="text/turtle",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/dataquality/migrate")
async def migrate_constraints(session_mgr: SessionManager = Depends(get_session_manager)):
    """One-time migration: convert legacy constraints to SHACL shapes."""
    try:
        domain = get_domain(session_mgr)
        legacy = domain.constraints
        if not legacy:
            return {"success": True, "message": "No legacy constraints to migrate", "shapes": domain.shacl_shapes}

        svc = SHACLService(base_uri=domain.ontology.get("base_uri", ""))
        migrated = svc.migrate_legacy_constraints(legacy, base_uri=domain.ontology.get("base_uri", ""))
        migrated_ids = {s["id"] for s in migrated}
        existing = domain.shacl_shapes or []
        manual = [s for s in existing if s.get("id", "") not in migrated_ids]
        domain.shacl_shapes = manual + migrated
        domain.save()
        return {
            "success": True,
            "message": f"Migrated {len(migrated)} constraints to SHACL shapes",
            "shapes": domain.shacl_shapes,
            "migrated_count": len(migrated),
        }
    except Exception as e:
        logger.exception("Migration failed: %s", e)
        raise InfrastructureError("SHACL migration failed", detail=str(e)) from e


# ===========================================
# SWRL Rules Management
# ===========================================

@router.get("/swrl/list")
async def list_swrl_rules(session_mgr: SessionManager = Depends(get_session_manager)):
    """Get list of SWRL rules from session."""
    domain = get_domain(session_mgr)
    return {'success': True, 'rules': domain.swrl_rules}


@router.post("/swrl/save")
async def save_swrl_rule(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Save a SWRL rule (add or update)."""
    try:
        data = await request.json()
        rule = data.get('rule', {})
        index = data.get('index', -1)
        
        if not rule.get('name'):
            return {'success': False, 'message': 'Rule name is required'}
        if not rule.get('antecedent') or not rule.get('consequent'):
            return {'success': False, 'message': 'Rule antecedent and consequent are required'}
        
        domain = get_domain(session_mgr)
        rules = domain.swrl_rules
        
        if 0 <= index < len(rules):
            rules[index] = rule
        else:
            rules.append(rule)
        
        domain.swrl_rules = rules
        domain.save()
        return {'success': True, 'message': 'SWRL rule saved successfully', 'rules': rules}
    except Exception as e:
        logger.exception("Save SWRL rule failed: %s", e)
        return {'success': False, 'message': str(e)}


@router.post("/swrl/delete")
async def delete_swrl_rule(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Delete a SWRL rule by index."""
    try:
        data = await request.json()
        index = data.get('index', -1)
        domain = get_domain(session_mgr)
        rules = domain.swrl_rules
        
        if not (0 <= index < len(rules)):
            return {'success': False, 'message': 'Invalid rule index'}
        
        rules.pop(index)
        domain.swrl_rules = rules
        domain.save()
        return {'success': True, 'message': 'SWRL rule deleted', 'rules': rules}
    except Exception as e:
        logger.exception("Delete SWRL rule failed: %s", e)
        return {'success': False, 'message': str(e)}


@router.post("/swrl/validate")
async def validate_swrl_rule(request: Request):
    """Validate a SWRL rule syntax."""
    data = await request.json()
    errors = Ontology.validate_swrl_rule(data.get('rule', {}))
    if errors:
        return {'success': False, 'valid': False, 'errors': errors}
    return {'success': True, 'valid': True, 'message': 'Rule syntax is valid'}


# ===========================================
# Business Rules — Generic CRUD for new rule types
# (decision_tables, sparql_rules, aggregate_rules)
# ===========================================

_RULE_TYPES = {
    "decision_tables": "decision_tables",
    "sparql_rules": "sparql_rules",
    "aggregate_rules": "aggregate_rules",
}


@router.get("/rules/{rule_type}/list")
async def list_rules(
    rule_type: str,
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """List rules of a given type from the ontology session."""
    key = _RULE_TYPES.get(rule_type)
    if not key:
        return {"success": False, "message": f"Unknown rule type: {rule_type}"}
    domain = get_domain(session_mgr)
    rules = (domain.ontology or {}).get(key, [])
    return {"success": True, "rules": rules}


@router.post("/rules/{rule_type}/save")
async def save_rule(
    rule_type: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Add or update a rule by index."""
    key = _RULE_TYPES.get(rule_type)
    if not key:
        return {"success": False, "message": f"Unknown rule type: {rule_type}"}
    try:
        data = await request.json()
        rule = data.get("rule", {})
        index = data.get("index", -1)

        if not rule.get("name"):
            return {"success": False, "message": "Rule name is required"}

        domain = get_domain(session_mgr)
        rules = list((domain.ontology or {}).get(key, []))

        if 0 <= index < len(rules):
            rules[index] = rule
        else:
            rules.append(rule)

        domain._data["ontology"][key] = rules
        domain.save()
        return {"success": True, "message": "Rule saved", "rules": rules}
    except Exception as e:
        logger.exception("Save rule (%s) failed: %s", rule_type, e)
        return {"success": False, "message": str(e)}


@router.post("/rules/{rule_type}/delete")
async def delete_rule(
    rule_type: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Delete a rule by index."""
    key = _RULE_TYPES.get(rule_type)
    if not key:
        return {"success": False, "message": f"Unknown rule type: {rule_type}"}
    try:
        data = await request.json()
        index = data.get("index", -1)

        domain = get_domain(session_mgr)
        rules = list((domain.ontology or {}).get(key, []))

        if not (0 <= index < len(rules)):
            return {"success": False, "message": "Invalid rule index"}

        rules.pop(index)
        domain._data["ontology"][key] = rules
        domain.save()
        return {"success": True, "message": "Rule deleted", "rules": rules}
    except Exception as e:
        logger.exception("Delete rule (%s) failed: %s", rule_type, e)
        return {"success": False, "message": str(e)}


@router.post("/rules/{rule_type}/validate")
async def validate_rule(rule_type: str, request: Request):
    """Validate a rule using the corresponding engine validator."""
    key = _RULE_TYPES.get(rule_type)
    if not key:
        return {"success": False, "message": f"Unknown rule type: {rule_type}"}

    data = await request.json()
    rule = data.get("rule", {})
    errors: list = []

    try:
        if key == "decision_tables":
            from back.core.reasoning.DecisionTableEngine import DecisionTableEngine
            errors = DecisionTableEngine.validate_table(rule)
        elif key == "sparql_rules":
            from back.core.reasoning.SPARQLRuleEngine import SPARQLRuleEngine
            errors = SPARQLRuleEngine.validate_rule(rule)
        elif key == "aggregate_rules":
            from back.core.reasoning.AggregateRuleEngine import AggregateRuleEngine
            errors = AggregateRuleEngine.validate_rule(rule)
    except Exception as e:
        errors = [str(e)]

    if errors:
        return {"success": False, "valid": False, "errors": errors}
    return {"success": True, "valid": True, "message": "Rule is valid"}


# ===========================================
# Axioms Management
# ===========================================

@router.get("/axioms/list")
async def list_axioms(session_mgr: SessionManager = Depends(get_session_manager)):
    """Get list of axioms and expressions from session."""
    domain = get_domain(session_mgr)
    return {'success': True, 'axioms': domain.axioms, 'expressions': domain.expressions}


@router.post("/axioms/save")
async def save_axiom(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Save an axiom or expression (add or update).

    Accepts ``collection``: ``"expressions"`` or ``"axioms"`` (default).
    """
    try:
        data = await request.json()
        axiom = data.get('axiom', {})
        index = data.get('index', -1)
        collection = data.get('collection', 'axioms')
        
        if not axiom.get('type'):
            return {'success': False, 'message': 'Axiom type is required'}
        
        domain = get_domain(session_mgr)
        if collection == 'expressions':
            items = domain.expressions
        else:
            items = domain.axioms
        
        if 0 <= index < len(items):
            items[index] = axiom
        else:
            items.append(axiom)
        
        if collection == 'expressions':
            domain.expressions = items
        else:
            domain.axioms = items
        domain.save()
        return {
            'success': True,
            'message': 'Axiom saved successfully',
            'axioms': domain.axioms,
            'expressions': domain.expressions,
        }
    except Exception as e:
        logger.exception("Save axiom failed: %s", e)
        return {'success': False, 'message': str(e)}


@router.post("/axioms/delete")
async def delete_axiom(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Delete an axiom or expression by index.

    Accepts ``collection``: ``"expressions"`` or ``"axioms"`` (default).
    """
    try:
        data = await request.json()
        index = data.get('index', -1)
        collection = data.get('collection', 'axioms')
        domain = get_domain(session_mgr)

        if collection == 'expressions':
            items = domain.expressions
        else:
            items = domain.axioms
        
        if not (0 <= index < len(items)):
            return {'success': False, 'message': 'Invalid axiom index'}
        
        items.pop(index)
        if collection == 'expressions':
            domain.expressions = items
        else:
            domain.axioms = items
        domain.save()
        return {
            'success': True,
            'message': 'Axiom deleted',
            'axioms': domain.axioms,
            'expressions': domain.expressions,
        }
    except Exception as e:
        logger.exception("Delete axiom failed: %s", e)
        return {'success': False, 'message': str(e)}


@router.get("/axioms/get-by-class/{class_uri:path}")
async def get_axioms_by_class(class_uri: str, session_mgr: SessionManager = Depends(get_session_manager)):
    """Get all axioms and expressions for a specific class."""
    domain = get_domain(session_mgr)

    def _matches(a):
        return class_uri in (a.get('class1'), a.get('class2'), a.get('className'), a.get('subject'))

    return {
        'success': True,
        'axioms': [a for a in domain.axioms if _matches(a)],
        'expressions': [a for a in domain.expressions if _matches(a)],
    }


@router.get("/axioms/get-by-type/{axiom_type}")
async def get_axioms_by_type(axiom_type: str, session_mgr: SessionManager = Depends(get_session_manager)):
    """Get all axioms or expressions of a specific type."""
    domain = get_domain(session_mgr)
    both = list(domain.axioms) + list(domain.expressions)
    return {'success': True, 'axioms': [a for a in both if a.get('type') == axiom_type]}


# ===========================================
# Group Management (entity groups via owl:unionOf)
# ===========================================

@router.get("/groups/list")
async def list_groups(session_mgr: SessionManager = Depends(get_session_manager)):
    """Get list of entity groups from session."""
    domain = get_domain(session_mgr)
    return {'success': True, 'groups': domain.groups}


@router.post("/groups/save")
async def save_group(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Create or update an entity group.

    Body: ``{ "group": { "name", "label", "description", "color", "icon", "members": [...] }, "index": -1 }``
    When ``index >= 0`` the group at that position is replaced; otherwise a new
    group is appended (duplicating a name is prevented).
    """
    data = await request.json()
    domain = get_domain(session_mgr)
    groups = Ontology(domain).save_group(data.get('group', {}), data.get('index', -1))
    return {'success': True, 'message': 'Group saved', 'groups': groups}


@router.post("/groups/delete")
async def delete_group(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Delete an entity group by index or name."""
    data = await request.json()
    domain = get_domain(session_mgr)
    groups = Ontology(domain).delete_group(index=data.get('index', -1), name=data.get('name', ''))
    return {'success': True, 'message': 'Group deleted', 'groups': groups}


@router.post("/groups/members")
async def update_group_members(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Add or remove members from a group.

    Body: ``{ "name": "G1", "add": ["D"], "remove": ["A"] }``
    """
    data = await request.json()
    domain = get_domain(session_mgr)
    groups = Ontology(domain).update_group_members(
        data.get('name', ''),
        add=data.get('add', []),
        remove=data.get('remove', []),
    )
    return {'success': True, 'message': 'Members updated', 'groups': groups}


# ===========================================
# Dashboard Management
# ===========================================

@router.get("/dashboards/list")
async def list_dashboards(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Get list of available Databricks dashboards."""
    try:
        from back.core.databricks import DatabricksClient

        domain = get_domain(session_mgr)
        host, token = get_databricks_host_and_token(domain, settings)
        
        if not host:
            return {'success': False, 'message': 'Databricks host not configured', 'dashboards': []}
        
        from back.core.helpers import run_blocking
        client = DatabricksClient(host=host, token=token)
        dashboards = await run_blocking(client.get_dashboards)
        
        return {'success': True, 'dashboards': dashboards}
    except Exception as e:
        logger.exception("Error listing dashboards: %s", e)
        return {'success': False, 'message': str(e), 'dashboards': []}


@router.get("/dashboards/{dashboard_id}/parameters")
async def get_dashboard_parameters(
    dashboard_id: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Get dashboard parameters for mapping to ontology attributes."""
    try:
        from back.core.databricks import DatabricksClient

        domain = get_domain(session_mgr)
        host, token = get_databricks_host_and_token(domain, settings)
        
        if not host:
            return {'success': False, 'message': 'Databricks host not configured', 'parameters': []}
        
        from back.core.helpers import run_blocking
        client = DatabricksClient(host=host, token=token)
        result = await run_blocking(client.get_dashboard_parameters, dashboard_id)
        
        if 'error' in result and result['error']:
            return {'success': False, 'message': result['error'], 'parameters': []}
        
        return {'success': True, **result}
    except Exception as e:
        logger.exception("Error getting dashboard parameters: %s", e)
        return {'success': False, 'message': str(e), 'parameters': []}


# ===========================================
# Cross-Domain Bridges
# ===========================================

@router.get("/bridges/domains")
async def list_bridge_domains(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List registry domains available as bridge targets (excludes the current domain)."""
    try:
        from back.objects.registry import RegistryService
        from back.core.helpers import run_blocking

        domain = get_domain(session_mgr)
        svc = RegistryService.from_context(domain, settings)
        ok, details, msg = await run_blocking(svc.list_domain_details_cached)
        if not ok:
            return {'success': False, 'message': msg, 'domains': []}

        current_name = (domain.info.get("name") or "").strip().lower()
        current_folder = (domain.domain_folder or "").strip().lower()

        domains = [
            p for p in details
            if p["name"].strip().lower() not in (current_name, current_folder)
        ]
        return {'success': True, 'domains': domains, 'projects': domains}
    except Exception as e:
        logger.exception("Error listing bridge domains: %s", e)
        return {'success': False, 'message': str(e), 'domains': [], 'projects': []}


@router.get("/bridges/domains/{domain_name}/classes")
async def list_bridge_domain_classes(
    domain_name: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Load a target domain's ontology classes for bridge selection."""
    try:
        from back.objects.registry import RegistryService
        from back.core.helpers import run_blocking

        domain = get_domain(session_mgr)
        svc = RegistryService.from_context(domain, settings)
        ok, data, _version, msg = await run_blocking(
            svc.load_latest_domain_data, domain_name,
        )
        if not ok:
            return {'success': False, 'message': msg, 'classes': []}

        ontology = svc._extract_latest_ontology(data)
        raw_classes = ontology.get("classes", [])

        classes = [
            {
                "name": c.get("name", ""),
                "uri": c.get("uri", ""),
                "label": c.get("label", c.get("name", "")),
                "emoji": c.get("emoji", "📦"),
                "description": c.get("description", c.get("comment", "")),
            }
            for c in raw_classes
            if c.get("name")
        ]
        return {
            'success': True,
            'domain': domain_name,
            'project': domain_name,
            'base_uri': ontology.get("base_uri", ""),
            'classes': classes,
        }
    except Exception as e:
        logger.exception("Error loading classes for domain %s: %s", domain_name, e)
        return {'success': False, 'message': str(e), 'classes': []}


# ===========================================
# OWL File Operations (Unity Catalog)
# ===========================================

@router.get("/list-owl-files")
async def list_owl_files(
    catalog: str = None, schema: str = None, volume: str = None,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """List OWL files from Unity Catalog Volume."""
    if not all([catalog, schema, volume]):
        return {'success': False, 'message': 'Missing required parameters', 'files': []}
    
    try:
        domain = get_domain(session_mgr)
        host, token = get_databricks_host_and_token(domain, settings)
        uc_service = VolumeFileService(host=host, token=token)
        success, files, message = uc_service.list_files(catalog, schema, volume, extensions=['.ttl', '.owl', '.rdf'])
        return {'success': success, 'files': files} if success else {'success': False, 'message': message, 'files': []}
    except Exception as e:
        logger.exception("List OWL files failed: %s", e)
        return {'success': False, 'message': str(e), 'files': []}


@router.post("/load-owl-file")
async def load_owl_file(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Load and parse an OWL file from Unity Catalog."""
    data = await request.json()
    catalog, schema, volume, filename = data.get('catalog'), data.get('schema'), data.get('volume'), data.get('filename')
    
    if not all([catalog, schema, volume, filename]):
        return {'success': False, 'message': 'Missing required fields'}
    
    try:
        domain = get_domain(session_mgr)
        host, token = get_databricks_host_and_token(domain, settings)
        uc_service = VolumeFileService(host=host, token=token)
        file_path = f"/Volumes/{catalog}/{schema}/{volume}/{filename}"
        
        success, owl_content, message = uc_service.read_file(file_path)
        if not success:
            return {'success': False, 'message': message}
        
        return Ontology(domain).ingest_owl(
            owl_content, name_fallback_to_domain=False, outcome="load_file",
        )
    except Exception as e:
        logger.exception("Load OWL file failed: %s", e)
        return {'success': False, 'message': str(e)}


@router.post("/save-to-uc")
async def save_ontology_to_uc(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Save OWL ontology to Unity Catalog volume."""
    from api.routers.internal._helpers import save_content_to_uc
    return await save_content_to_uc(request, session_mgr, settings, log_context="ontology")


@router.post("/update-relationship-references")
async def update_relationship_references(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Update references when a relationship is renamed."""
    data = await request.json()
    old_name, new_name = data.get('old_name'), data.get('new_name')
    if not old_name or not new_name:
        return {'success': False, 'message': 'Both old_name and new_name are required'}
    updates = Ontology(get_domain(session_mgr)).rename_relationship_references(old_name, new_name)
    total = sum(updates.values())
    return {'success': True, 'message': f'Updated {total} references', 'updates': updates}


# ===========================================
# Wizard — ontology generation (agent_owl_generator, async task)
# ===========================================

@router.get("/wizard/templates")
async def get_wizard_templates():
    """Return the predefined wizard quick-templates."""
    from shared.config.constants import WIZARD_TEMPLATES
    return {"success": True, "templates": WIZARD_TEMPLATES}


@router.post("/wizard/generate-async")
async def generate_ontology_async(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Start ontology generation via ``agent_owl_generator`` (background task).

    Poll ``GET /tasks/{task_id}`` for ``owl_content``, ``stats``, and agent trace fields.
    There is no synchronous generate endpoint; this is the only LLM wizard entry point.
    """
    import threading

    data = await request.json()
    metadata = data.get('metadata', {})
    guidelines = data.get('guidelines', '')
    options = data.get('options', {})
    documents = data.get('documents', [])

    tables_count = len(metadata.get('tables', []))

    domain = get_domain(session_mgr)
    err, llm_ctx = require_serving_llm(domain, settings)
    if err:
        return err
    host, token, llm_endpoint = llm_ctx

    tm = get_task_manager()
    task = tm.create_task(
        name=f"Generate Ontology ({tables_count} tables)" if tables_count else "Generate Ontology (guidelines only)",
        task_type="ontology_generation",
        steps=[
            {'name': 'init', 'description': 'Initializing agent'},
            {'name': 'gather', 'description': 'Gathering context (metadata & documents)'},
            {'name': 'generate', 'description': 'Generating ontology with AI'},
            {'name': 'process', 'description': 'Processing results'},
            {'name': 'finalize', 'description': 'Finalizing'},
        ]
    )

    def run_generation():
        try:
            tm.start_task(task.id, "Starting agent…")

            def on_step(msg: str):
                tm.update_progress(task.id, task.progress, msg)

            agent_result = Ontology(domain).generate_with_agent(
                host=host,
                token=token,
                endpoint_name=llm_endpoint,
                metadata=metadata,
                guidelines=guidelines,
                options=options,
                selected_docs=documents,
                on_step=on_step,
            )

            if not agent_result.success:
                tm.fail_task(task.id, agent_result.error or "Agent did not produce output")
                return

            tm.advance_step(task.id, "Processing results…")
            owl_content, stats = Ontology.postprocess_generated_owl(agent_result.owl_content)

            tm.advance_step(task.id, "Finalizing…")
            tm.complete_task(
                task.id,
                result={
                    'owl_content': owl_content,
                    'stats': stats,
                    'agent_steps': serialize_agent_steps(agent_result.steps),
                    'agent_iterations': agent_result.iterations,
                    'agent_usage': agent_result.usage,
                },
                message=(
                    f"Generated {stats.get('classes', 0)} classes, "
                    f"{stats.get('properties', 0)} properties "
                    f"({agent_result.iterations} agent iterations)"
                ),
            )

        except Exception as e:
            logger.exception("Wizard async: Ontology generation failed: %s", e)
            tm.fail_task(task.id, str(e))

    thread = threading.Thread(target=run_generation, daemon=True)
    thread.start()

    return {
        'success': True,
        'task_id': task.id,
        'message': 'Agent task started'
    }


# ===========================================
# Auto-map Icons via Agent
# ===========================================

@router.post("/auto-assign-icons")
async def auto_assign_icons(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Use the auto-icon-assign agent to suggest emoji icons for entity names."""
    try:
        data = await request.json()
        entity_names = data.get('entity_names', [])

        if not entity_names:
            return {'success': False, 'message': 'No entity names provided'}

        domain = get_domain(session_mgr)
        err, llm_ctx = require_serving_llm(domain, settings)
        if err:
            return err
        host, token, llm_endpoint = llm_ctx

        logger.info("AutoIcons: launching agent for %d entities", len(entity_names))

        agent_result = Ontology(domain).assign_icons_with_agent(
            host=host,
            token=token,
            endpoint_name=llm_endpoint,
            entity_names=entity_names,
        )

        if not agent_result.success:
            logger.warning("AutoIcons: agent failed — %s", agent_result.error)
            return {'success': False, 'message': agent_result.error or 'Agent failed to assign icons'}

        final_map = Ontology.merge_icon_suggestions(entity_names, agent_result.icons)
        missing = [n for n in entity_names if n not in final_map]
        if missing:
            logger.warning("AutoIcons: No icon for: %s", missing)

        logger.info(
            "AutoIcons: agent completed — %d/%d icons assigned in %d iterations",
            len(final_map), len(entity_names), agent_result.iterations,
        )

        return {
            'success': True,
            'icons': final_map,
            'agent_iterations': agent_result.iterations,
        }

    except Exception as e:
        logger.exception("AutoIcons: Failed: %s", e)
        return {'success': False, 'message': str(e)}


# ===========================================
# Ontology Assistant (AI Chat)
# ===========================================

@router.post("/assistant/chat")
async def ontology_assistant_chat(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Process a single chat turn with the ontology assistant agent.

    Expects JSON body:
        {
            "message": "Remove the entity Customer",
            "history": [...]   // optional prior conversation messages
        }

    Returns:
        {
            "success": true/false,
            "reply": "...",
            "ontology_changed": true/false,
            "config": { classes, properties, ... }   // returned when ontology was modified
        }
    """
    from agents.agent_ontology_assistant import run_agent as run_assistant

    data = await request.json()
    user_message = data.get("message", "").strip()
    history = data.get("history", [])

    if not user_message:
        return {"success": False, "message": "No message provided"}

    domain = get_domain(session_mgr)
    err, llm_ctx = require_serving_llm(domain, settings)
    if err:
        return err
    host, token, llm_endpoint = llm_ctx

    classes = list(domain.get_classes())
    properties = list(domain.get_properties())
    base_uri = domain.ontology.get("base_uri") or DEFAULT_BASE_URI

    logger.info("OntologyAssistant: user_message=%s, classes=%d, properties=%d",
                user_message[:80], len(classes), len(properties))

    try:
        agent_result = run_assistant(
            host=host,
            token=token,
            endpoint_name=llm_endpoint,
            classes=classes,
            properties=properties,
            base_uri=base_uri,
            user_message=user_message,
            conversation_history=history,
        )
    except Exception as exc:
        logger.exception("OntologyAssistant: agent failed: %s", exc)
        return {"success": False, "message": f"Assistant error: {exc}"}

    if not agent_result.success:
        return {"success": False, "message": agent_result.error or "Assistant failed"}

    response = {
        "success": True,
        "reply": agent_result.reply,
        "ontology_changed": agent_result.ontology_changed,
    }

    if agent_result.ontology_changed:
        config = Ontology(domain).apply_agent_ontology_changes(
            agent_result.classes, agent_result.properties, prune_orphan_mappings=True,
        )
        response["config"] = config
        logger.info("OntologyAssistant: ontology saved — classes=%d, properties=%d",
                     len(config["classes"]), len(config["properties"]))

    return response


# ===========================================
# Ontology Assistant — ResponsesAgent API
# ===========================================

@router.post("/assistant/invoke")
async def ontology_assistant_invoke(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Invoke the Ontology Assistant via the MLflow ResponsesAgent interface.

    Accepts the OpenAI Responses-compatible schema used by the Databricks
    Agent Framework.  The caller can either supply Databricks credentials in
    ``custom_inputs`` or let the route fill them from the active session.

    Expects JSON body (ResponsesAgentRequest)::

        {
            "input": [
                {"role": "user", "content": "Add an entity called Vehicle"}
            ],
            "custom_inputs": {          // optional overrides
                "host": "...",
                "token": "...",
                "endpoint_name": "..."
            }
        }

    Returns a ``ResponsesAgentResponse`` with ``custom_outputs`` containing
    the mutated ontology when changes were made.
    """
    from agents.agent_ontology_assistant import OntologyAssistantResponsesAgent
    from mlflow.types.responses import ResponsesAgentRequest as RAReq

    data = await request.json()

    domain = get_domain(session_mgr)
    host, token = get_databricks_host_and_token(domain, settings)
    llm_endpoint = domain.info.get("llm_endpoint", "")
    base_uri = domain.ontology.get("base_uri") or DEFAULT_BASE_URI

    custom_inputs = data.get("custom_inputs", {})
    custom_inputs.setdefault("host", host)
    custom_inputs.setdefault("token", token)
    custom_inputs.setdefault("endpoint_name", llm_endpoint)
    custom_inputs.setdefault("base_uri", base_uri)
    custom_inputs.setdefault("classes", list(domain.get_classes()))
    custom_inputs.setdefault("properties", list(domain.get_properties()))
    data["custom_inputs"] = custom_inputs

    if not custom_inputs.get("host") or not custom_inputs.get("token"):
        return {"error": "Databricks credentials not configured"}
    if not custom_inputs.get("endpoint_name"):
        return {"error": "No LLM serving endpoint configured."}

    try:
        agent = OntologyAssistantResponsesAgent()
        ra_request = RAReq(**data)
        response = agent.predict(ra_request)

        co = response.custom_outputs or {}
        if co.get("ontology_changed"):
            Ontology(domain).apply_agent_ontology_changes(
                co.get("classes", []), co.get("properties", []),
                prune_orphan_mappings=False,
            )

        return response.model_dump()
    except Exception as exc:
        logger.exception("ontology_assistant_invoke failed: %s", exc)
        return {"error": str(exc)}


