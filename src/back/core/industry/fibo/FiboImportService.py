"""FIBO import service.

Fetches FIBO (Financial Industry Business Ontology) domain modules from the
EDM Council specification server, merges them using rdflib, and converts
them to OntoBricks ontology structures via the existing OWL parser.

Reference: https://spec.edmcouncil.org/fibo/
"""

import time
from typing import Dict, List, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from rdflib import Graph

from back.core.errors import InfrastructureError, ValidationError
from back.core.logging import get_logger
from back.core.industry.constants import FIBO_BASE_URL
from shared.config.constants import HTTP_USER_AGENT

logger = get_logger(__name__)


class FiboImportService:
    # ---------------------------------------------------------------------------
    # FIBO Catalog – curated list of domain modules
    # ---------------------------------------------------------------------------

    # Base URL for fetching individual FIBO ontology files (latest production).
    # The spec server also hosts versioned releases (e.g. .../master/2025Q4/...).

    FIBO_DOMAINS: Dict[str, Dict[str, Any]] = {
        "FND": {
            "name": "Foundations (FND)",
            "description": "Core building blocks: parties, agreements, relations, dates, "
            "organizations, and accounting concepts. Required by all other "
            "FIBO domains.",
            "icon": "bi-bricks",
            "color": "secondary",
            "modules": [
                "FND/AgentsAndPeople/Agents",
                "FND/AgentsAndPeople/People",
                "FND/Parties/Parties",
                "FND/Relations/Relations",
                "FND/Agreements/Agreements",
                "FND/Agreements/Contracts",
                "FND/DatesAndTimes/FinancialDates",
                "FND/DatesAndTimes/BusinessDates",
                "FND/Organizations/FormalOrganizations",
                "FND/Places/Addresses",
                "FND/Places/Facilities",
                "FND/Places/VirtualPlaces",
                "FND/Accounting/CurrencyAmount",
                "FND/Law/LegalCapacity",
                "FND/Law/LegalCore",
            ],
        },
        "BE": {
            "name": "Business Entities (BE)",
            "description": "Legal entities, corporations, partnerships, government bodies, "
            "and ownership structures.",
            "icon": "bi-building",
            "color": "primary",
            "modules": [
                "BE/LegalEntities/LegalPersons",
                "BE/LegalEntities/CorporateBodies",
                "BE/LegalEntities/FormalBusinessOrganizations",
                "BE/FunctionalEntities/FunctionalEntities",
                "BE/FunctionalEntities/Publishers",
                "BE/GovernmentEntities/GovernmentEntities",
                "BE/OwnershipAndControl/OwnershipParties",
                "BE/OwnershipAndControl/ControlParties",
                "BE/OwnershipAndControl/CorporateOwnership",
            ],
        },
        "FBC": {
            "name": "Financial Business & Commerce (FBC)",
            "description": "Financial products, services, intermediaries, markets, "
            "and financial instruments.",
            "icon": "bi-bank",
            "color": "success",
            "modules": [
                "FBC/ProductsAndServices/FinancialProductsAndServices",
                "FBC/ProductsAndServices/ClientsAndAccounts",
                "FBC/FunctionalEntities/FinancialServicesEntities",
                "FBC/FunctionalEntities/Markets",
                "FBC/FunctionalEntities/RegulatoryAgencies",
                "FBC/FunctionalEntities/BusinessRegistries",
                "FBC/FinancialInstruments/FinancialInstruments",
                "FBC/FinancialInstruments/InstrumentPricing",
            ],
        },
        "LOAN": {
            "name": "Loans (LOAN)",
            "description": "Loan products, applications, mortgage concepts, "
            "and real estate lending.",
            "icon": "bi-cash-coin",
            "color": "warning",
            "modules": [
                "LOAN/LoansGeneral/Loans",
                "LOAN/LoansGeneral/LoanApplications",
                "LOAN/RealEstateLoans/Mortgages",
            ],
        },
        "SEC": {
            "name": "Securities (SEC)",
            "description": "Securities listings, equities, bonds, debt instruments, "
            "and investment funds.",
            "icon": "bi-graph-up-arrow",
            "color": "info",
            "modules": [
                "SEC/Securities/SecuritiesListings",
                "SEC/Securities/SecuritiesClassification",
                "SEC/Securities/SecuritiesIssuance",
                "SEC/Equities/EquityInstruments",
                "SEC/Debt/Bonds",
                "SEC/Debt/DebtInstruments",
                "SEC/Funds/Funds",
                "SEC/Funds/CollectiveInvestmentVehicles",
            ],
        },
        "DER": {
            "name": "Derivatives (DER)",
            "description": "Options, futures, swaps, and other derivative "
            "contracts and instruments.",
            "icon": "bi-arrow-left-right",
            "color": "danger",
            "modules": [
                "DER/DerivativesContracts/DerivativesBasics",
                "DER/DerivativesContracts/Options",
                "DER/DerivativesContracts/FuturesAndForwards",
                "DER/DerivativesContracts/Swaps",
                "DER/DerivativesContracts/CommoditiesContracts",
                "DER/DerivativesContracts/CurrencyContracts",
            ],
        },
    }

    _REQUEST_TIMEOUT = 20  # seconds per module
    _MAX_WORKERS = 5  # concurrent download threads

    @staticmethod
    def get_fibo_catalog() -> List[Dict[str, Any]]:
        """Return the FIBO domain catalog for the frontend.

        Returns:
            list[dict]: Each entry has key, name, description, icon, color,
                        and module_count.
        """
        catalog = []
        for key, domain in FiboImportService.FIBO_DOMAINS.items():
            catalog.append(
                {
                    "key": key,
                    "name": domain["name"],
                    "description": domain["description"],
                    "icon": domain["icon"],
                    "color": domain["color"],
                    "module_count": len(domain["modules"]),
                }
            )
        return catalog

    @staticmethod
    def _fetch_single_module(
        module_path: str,
    ) -> Tuple[str, Optional[str], Optional[str]]:
        """Fetch a single FIBO module, trying multiple URL patterns.

        Args:
            module_path: e.g. "FND/Parties/Parties"

        Returns:
            (module_path, content_or_None, error_or_None)
        """
        strategies = [
            (f"{FIBO_BASE_URL}/{module_path}.rdf", "xml"),
            (f"{FIBO_BASE_URL}/{module_path}.ttl", "turtle"),
        ]

        last_error = ""
        for url, fmt in strategies:
            try:
                resp = requests.get(
                    url,
                    timeout=FiboImportService._REQUEST_TIMEOUT,
                    headers={"User-Agent": HTTP_USER_AGENT, "Accept": "*/*"},
                    allow_redirects=True,
                )
                if resp.status_code == 200:
                    text = resp.text.strip()
                    # Reject HTML responses (some servers return 200 with an HTML page)
                    if text.startswith("<!DOCTYPE") or text.startswith("<html"):
                        last_error = f"{url} returned HTML"
                        continue
                    return module_path, text, None
                last_error = f"{url} -> HTTP {resp.status_code}"
            except requests.exceptions.Timeout:
                last_error = f"{url} -> timeout"
            except requests.exceptions.RequestException as exc:
                last_error = f"{url} -> {exc}"

        return module_path, None, last_error

    @staticmethod
    def _collect_module_paths(domain_keys: List[str]) -> List[str]:
        """Build a deduplicated list of FIBO module paths for the selected domains.

        FND is always included when any other domain is selected because other
        FIBO domains depend on foundational concepts.
        """
        paths: list[str] = []
        seen: set[str] = set()

        # Always include FND if any non-FND domain is requested
        keys_to_fetch = list(domain_keys)
        if any(k != "FND" for k in keys_to_fetch) and "FND" not in keys_to_fetch:
            keys_to_fetch.insert(0, "FND")

        for key in keys_to_fetch:
            domain = FiboImportService.FIBO_DOMAINS.get(key)
            if not domain:
                continue
            for mod in domain["modules"]:
                if mod not in seen:
                    seen.add(mod)
                    paths.append(mod)

        return paths

    @staticmethod
    def fetch_and_parse_fibo(
        domain_keys: List[str],
    ) -> Dict[str, Any]:
        """Fetch FIBO modules for the selected domains, merge, and parse.

        Args:
            domain_keys: List of domain keys (e.g. ["FND", "BE"])

        Returns:
            dict with keys:
                success (bool)
                turtle (str) – merged Turtle content
                stats (dict)  – classes, properties counts
                fetched (int) – number of modules successfully loaded
                failed (list)  – module paths that could not be fetched
                message (str)
        """
        start = time.time()
        module_paths = FiboImportService._collect_module_paths(domain_keys)

        if not module_paths:
            raise ValidationError("No valid FIBO domains selected.")

        logger.info(
            "Fetching %d modules for domains: %s", len(module_paths), domain_keys
        )

        # Fetch modules concurrently
        merged_graph = Graph()
        fetched_count = 0
        failed_modules: list[str] = []

        with ThreadPoolExecutor(max_workers=FiboImportService._MAX_WORKERS) as pool:
            futures = {
                pool.submit(FiboImportService._fetch_single_module, path): path
                for path in module_paths
            }
            for future in as_completed(futures):
                mod_path, content, error = future.result()
                if content:
                    try:
                        # Detect format from content
                        fmt = "xml" if content.lstrip().startswith("<") else "turtle"
                        merged_graph.parse(data=content, format=fmt)
                        fetched_count += 1
                        logger.info("OK  %s", mod_path)
                    except Exception as parse_err:
                        failed_modules.append(mod_path)
                        logger.warning("PARSE_ERR  %s: %s", mod_path, parse_err)
                else:
                    failed_modules.append(mod_path)
                    logger.warning("FAIL %s: %s", mod_path, error)

        elapsed = time.time() - start
        logger.info(
            "Fetched %d/%d modules in %.1fs", fetched_count, len(module_paths), elapsed
        )

        if fetched_count == 0:
            hint = (
                "Could not reach the FIBO specification server. "
                "If you are running in Databricks Apps, outbound internet access "
                "may be restricted. Try downloading FIBO Turtle files manually "
                "and importing via 'Import OWL'."
            )
            raise InfrastructureError(hint, detail=", ".join(failed_modules) or None)

        # Serialize merged graph to Turtle
        turtle_content = merged_graph.serialize(format="turtle")

        # Parse with the OntoBricks OWL parser for structured output
        from back.objects.ontology import Ontology

        result = Ontology.parse_owl(turtle_content, extract_advanced=True)
        (
            ontology_info,
            classes,
            properties,
            constraints,
            swrl_rules,
            axioms,
            expressions,
            _groups,
        ) = result

        stats = {
            "classes": len(classes),
            "properties": len(properties),
            "constraints": len(constraints),
            "modules_fetched": fetched_count,
            "modules_failed": len(failed_modules),
        }

        domain_names = ", ".join(
            FiboImportService.FIBO_DOMAINS[k]["name"]
            for k in domain_keys
            if k in FiboImportService.FIBO_DOMAINS
        )
        msg = (
            f"FIBO imported: {stats['classes']} classes, {stats['properties']} "
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
