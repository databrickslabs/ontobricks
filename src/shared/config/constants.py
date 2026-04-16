"""
OntoBricks - Global Configuration Constants

Centralised, read-only constants and defaults that are shared across the
application.  Environment-specific settings (credentials, hosts, feature
flags) belong in ``shared.config.settings.Settings``; this module is for
values that rarely change and are used by more than one package.
"""

import os
from rdflib import Namespace


def _read_version() -> str:
    """Read version from pyproject.toml (single source of truth)."""
    try:
        toml_path = os.path.join(
            os.path.dirname(__file__), os.pardir, os.pardir, os.pardir, "pyproject.toml",
        )
        with open(os.path.normpath(toml_path), encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("version"):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    except Exception:
        pass
    return "0.0.0"


# =====================================================
# Application Identity
# =====================================================
APP_NAME: str = "OntoBricks"
APP_VERSION: str = _read_version()
APP_DESCRIPTION: str = "Ontology-driven data integration for Databricks"
HTTP_USER_AGENT: str = f"{APP_NAME}/{APP_VERSION}"

# =====================================================
# Logging
# =====================================================
APP_LOGGER_NAME: str = "ontobricks"
DEFAULT_LOG_LEVEL: str = "DEBUG"        # DEBUG | INFO | WARNING | ERROR | CRITICAL
DEFAULT_LOG_FILE: str = "ontobricks.log"
LOG_MAX_BYTES: int = 10 * 1024 * 1024  # 10 MB per file
LOG_BACKUP_COUNT: int = 5

# =====================================================
# OWL / RDF Namespaces
# =====================================================
ONTOBRICKS_NS = Namespace("http://ontobricks.com/schema#")
DEFAULT_BASE_URI: str = "https://databricks-ontology.com/"

# =====================================================
# Domain / graph defaults
# =====================================================
DEFAULT_GRAPH_NAME: str = "ontobricks"
DEFAULT_GRAPH_VERSION: str = "1"
DEFAULT_LADYBUG_PATH: str = "/tmp/ontobricks"

# =====================================================
# Validation Messages (reusable across modules)
# =====================================================
MSG_TABLE_NAME_REQUIRED: str = "table_name cannot be empty"
MSG_WAREHOUSE_ID_REQUIRED: str = "SQL Warehouse ID is required"

# =====================================================
# Session Keys
# =====================================================
SESSION_COOKIE_NAME: str = "ontobricks_session"

# =====================================================
# LLM / Wizard Defaults
# =====================================================
LLM_DEFAULT_MAX_TOKENS: int = 4096
LLM_DEFAULT_TEMPERATURE: float = 0.1

# =====================================================
# Mapping Defaults
# =====================================================
MAPPING_SQL_LIMIT: int = 100

# =====================================================
# Auto-Mapping Chunking
# =====================================================
AUTO_ASSIGN_CHUNK_SIZE: int = 5       # max entities + relationships per agent run
AUTO_ASSIGN_CHUNK_COOLDOWN: int = 15  # seconds to wait between chunks

# =====================================================
# UI Defaults
# =====================================================
MAX_PREVIEW_ROWS: int = 50
MAX_NOTIFICATIONS: int = 10

# =====================================================
# Ontology Wizard – Quick Templates
# =====================================================
# Each entry is a dict with:
#   key   – unique identifier (used in API and JS)
#   label – button text shown in the UI
#   icon  – Bootstrap Icons class (without "bi-" prefix)
#   guidelines – the prompt text injected into the wizard textarea
WIZARD_TEMPLATES: dict = {
    "crm": {
        "label": "CRM",
        "icon": "people",
        "guidelines": (
            "Generate an ontology for a Customer Relationship Management (CRM) domain:\n"
            "\n"
            "- Create entities for customers, contacts, accounts, opportunities, and activities\n"
            "- Include relationships like 'hasContact', 'belongsToAccount', 'relatedTo'\n"
            "- Consider that customers can have multiple contacts and opportunities\n"
            "- Include properties for status, dates, and monetary values\n"
            "- Focus on the sales pipeline and customer journey"
        ),
    },
    "ecommerce": {
        "label": "E-Commerce",
        "icon": "cart",
        "guidelines": (
            "Generate an ontology for an E-Commerce platform:\n"
            "\n"
            "- Create entities for products, categories, orders, customers, and payments\n"
            "- Include relationships like 'belongsToCategory', 'containsItem', 'placedBy'\n"
            "- Model the shopping cart and checkout process\n"
            "- Include properties for prices, quantities, and statuses\n"
            "- Consider inventory and shipping aspects"
        ),
    },
    "iot": {
        "label": "IoT",
        "icon": "cpu",
        "guidelines": (
            "Generate an ontology for an Internet of Things (IoT) system:\n"
            "\n"
            "- Create entities for devices, sensors, measurements, and locations\n"
            "- Include relationships like 'hasSensor', 'locatedAt', 'measures'\n"
            "- Model the device hierarchy and sensor readings\n"
            "- Include properties for timestamps, values, and units\n"
            "- Consider device states and alerts"
        ),
    },
    "healthcare": {
        "label": "Healthcare",
        "icon": "heart-pulse",
        "guidelines": (
            "Generate an ontology for a Healthcare system:\n"
            "\n"
            "- Create entities for patients, providers, appointments, diagnoses, and treatments\n"
            "- Include relationships like 'hasAppointment', 'diagnosedWith', 'treatedBy'\n"
            "- Model the patient care journey\n"
            "- Include properties for dates, codes, and descriptions\n"
            "- Consider medical records and prescriptions"
        ),
    },
    "energy": {
        "label": "Energy",
        "icon": "lightning-charge",
        "guidelines": (
            "Generate an ontology for a Customer Relationship Management (CRM) domain in the energy sector:\n"
            "\n"
            "- Create entities for customers, contacts, interactions, invoices, and meter informations\n"
            "- Include relationships like 'hasContact', 'belongsToAccount', 'relatedTo'\n"
            "- Consider that customers can have multiple contacts and interactions\n"
            "- Include properties for status, dates, and monetary values\n"
            "- Focus on the customer journey"
        ),
    },
}
