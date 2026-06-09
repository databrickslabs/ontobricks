"""Persona registry for the OntoBricks UAT suite.

Five job-function personas cover the four RBAC roles (two builders,
differentiated by job focus), plus two thin "edge" personas that drive the
access-denied paths. A persona is turned into a session by attaching its
:pyattr:`Persona.headers` to a Playwright browser context — the test-auth
seam in ``PermissionMiddleware`` reads them and runs the *real* gates.
"""

from __future__ import annotations

from dataclasses import dataclass

# RBAC role constants — mirror back.objects.registry.PermissionService so the
# UAT suite never imports app code just to name a role.
ROLE_ADMIN = "admin"
ROLE_BUILDER = "builder"
ROLE_EDITOR = "editor"
ROLE_VIEWER = "viewer"
ROLE_APP_USER = "app_user"
ROLE_NONE = "none"

ROLE_LEVEL = {
    ROLE_NONE: 0,
    ROLE_VIEWER: 1,
    ROLE_EDITOR: 2,
    ROLE_BUILDER: 3,
    ROLE_ADMIN: 4,
}


@dataclass(frozen=True)
class Persona:
    """A UAT persona: a job archetype bound to an app role + domain role."""

    key: str
    name: str
    email: str
    app_role: str
    domain_role: str
    description: str

    @property
    def headers(self) -> dict:
        """Seam headers that make every request act as this persona."""
        return {
            "x-forwarded-email": self.email,
            "x-ontobricks-test-role": self.app_role,
            "x-ontobricks-test-domain-role": self.domain_role,
        }

    @property
    def effective_domain_level(self) -> int:
        """Domain authority level (admins bypass the domain gate entirely)."""
        if self.app_role == ROLE_ADMIN:
            return ROLE_LEVEL[ROLE_ADMIN]
        return ROLE_LEVEL.get(self.domain_role, 0)

    @property
    def is_admin(self) -> bool:
        return self.app_role == ROLE_ADMIN


# --- The five primary personas ---------------------------------------------

ADMIN = Persona(
    key="admin",
    name="Priya — Platform Admin",
    email="priya.admin@uat.ontobricks.test",
    app_role=ROLE_ADMIN,
    domain_role=ROLE_ADMIN,
    description=(
        "Owns Databricks/global settings, app permissions, the Teams matrix, "
        "registry config, health, scheduler; publishes/reverts any version."
    ),
)

ONTOLOGY_ENGINEER = Persona(
    key="ontology_engineer",
    name="Olu — Ontology Engineer",
    email="olu.onteng@uat.ontobricks.test",
    app_role=ROLE_APP_USER,
    domain_role=ROLE_BUILDER,
    description=(
        "Designs the ontology (classes, properties, axioms, SWRL, SHACL/DQ "
        "shapes), generates/imports/exports OWL, submits for review, builds."
    ),
)

DATA_ENGINEER = Persona(
    key="data_engineer",
    name="Dana — Data Engineer",
    email="dana.dataeng@uat.ontobricks.test",
    app_role=ROLE_APP_USER,
    domain_role=ROLE_BUILDER,
    description=(
        "Wires data sources (UC metadata), authors mappings/R2RML "
        "(manual + auto-map + diagnostics), builds/syncs the digital twin."
    ),
)

DATA_STEWARD = Persona(
    key="data_steward",
    name="Sam — Data Steward",
    email="sam.steward@uat.ontobricks.test",
    app_role=ROLE_APP_USER,
    domain_role=ROLE_EDITOR,
    description=(
        "Edits ontology/mappings, runs data quality, reviews validation and "
        "signs off — but cannot build or publish."
    ),
)

CONSUMER = Persona(
    key="consumer",
    name="Cory — Business Consumer",
    email="cory.consumer@uat.ontobricks.test",
    app_role=ROLE_APP_USER,
    domain_role=ROLE_VIEWER,
    description=(
        "Read-only: browses ontology/mapping, explores the knowledge graph "
        "(SPARQL/GraphQL), views cohorts/DQ/inference, consumes API/MCP."
    ),
)

# --- Edge personas (access-denied paths only) ------------------------------

NO_DOMAIN = Persona(
    key="no_domain",
    name="Nina — No-Domain",
    email="nina.nodomain@uat.ontobricks.test",
    app_role=ROLE_APP_USER,
    domain_role=ROLE_NONE,
    description="App user with no team entry on the loaded domain.",
)

NO_APP = Persona(
    key="no_app",
    name="Nora — No-App",
    email="nora.noapp@uat.ontobricks.test",
    app_role=ROLE_NONE,
    domain_role=ROLE_NONE,
    description="Authenticated user absent from the app ACL.",
)


PERSONAS = {
    p.key: p for p in (ADMIN, ONTOLOGY_ENGINEER, DATA_ENGINEER, DATA_STEWARD, CONSUMER)
}
ALL_PERSONAS = list(PERSONAS.values())

EDGE_PERSONAS = {p.key: p for p in (NO_DOMAIN, NO_APP)}

# Personas with at least some write capability on a domain.
WRITE_PERSONAS = [ADMIN, ONTOLOGY_ENGINEER, DATA_ENGINEER, DATA_STEWARD]
# Personas who may build/publish (domain-role builder or app admin).
BUILDER_PERSONAS = [ADMIN, ONTOLOGY_ENGINEER, DATA_ENGINEER]
# Read-only personas.
READ_ONLY_PERSONAS = [CONSUMER]


def ids(persona: Persona) -> str:
    """pytest id helper."""
    return persona.key
