"""Unit tests for ``agents.agent_cohort.tools`` (Stage 2 — NL → CohortRule).

Each tool is exercised in isolation by swapping in an
``httpx.MockTransport`` so no network I/O happens. We assert:

* happy-path JSON shape (compact payloads, capped sizes),
* input validation (missing class_uri, etc.),
* deterministic offline behaviour of ``propose_rule`` (parks the rule on
  ``ToolContext.metadata`` and round-trips through ``CohortRule``).
"""

from __future__ import annotations

import json

import httpx
import pytest

from agents.agent_cohort import tools as cohort_tools
from agents.tools.context import ToolContext


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _ctx() -> ToolContext:
    return ToolContext(
        host="https://test.databricks.com",
        token="test-token",
        dtwin_base_url="http://loopback.invalid",
        dtwin_domain_name="consulting_demo",
        dtwin_registry_params={
            "registry_catalog": "main",
            "registry_schema": "ontobricks",
            "registry_volume": "documents",
        },
    )


@pytest.fixture
def patch_client(monkeypatch):
    """Install an httpx MockTransport in cohort_tools._client."""

    def _install(handler):
        def _factory(ctx):
            transport = httpx.MockTransport(handler)
            return httpx.Client(
                base_url=ctx.dtwin_base_url or "http://loopback.invalid",
                transport=transport,
                timeout=5,
                follow_redirects=False,
            )

        monkeypatch.setattr(cohort_tools, "_client", _factory)

    return _install


# Minimal ontology payload reused across multiple tests.
_ONT_PAYLOAD = {
    "success": True,
    "ontology": {
        "classes": [
            {
                "uri": "https://ex.com/onto/Consultant",
                "name": "Consultant",
                "label": "Consultant",
                "dataProperties": [
                    {
                        "uri": "https://ex.com/onto/status",
                        "name": "status",
                        "label": "status",
                        "range": "xsd:string",
                    },
                    {
                        "uri": "https://ex.com/onto/level",
                        "name": "level",
                        "label": "level",
                        "range": "xsd:integer",
                    },
                ],
            },
            {
                "uri": "https://ex.com/onto/Project",
                "name": "Project",
                "label": "Project",
                "dataProperties": [],
            },
        ],
        "properties": [
            {
                "uri": "https://ex.com/onto/staffedOn",
                "name": "staffedOn",
                "label": "staffed on",
                "type": "ObjectProperty",
                "domain": "https://ex.com/onto/Consultant",
                "range": "https://ex.com/onto/Project",
            },
            {
                "uri": "https://ex.com/onto/owns",
                "name": "owns",
                "label": "owns",
                "type": "ObjectProperty",
                "domain": "https://ex.com/onto/Person",
                "range": "https://ex.com/onto/Asset",
            },
        ],
    },
}


# ---------------------------------------------------------------------------
# list_classes
# ---------------------------------------------------------------------------


class TestListClasses:
    def test_happy_path(self, patch_client):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/ontology/get-loaded-ontology"
            return httpx.Response(200, json=_ONT_PAYLOAD)

        patch_client(handler)
        out = json.loads(cohort_tools.tool_list_classes(_ctx()))
        assert out["count"] == 2
        uris = [c["uri"] for c in out["classes"]]
        assert "https://ex.com/onto/Consultant" in uris
        assert "https://ex.com/onto/Project" in uris
        consultant = next(c for c in out["classes"] if c["uri"].endswith("Consultant"))
        assert consultant["data_property_count"] == 2

    def test_empty_ontology(self, patch_client):
        patch_client(lambda _r: httpx.Response(
            200,
            json={"success": True, "ontology": {"classes": [], "properties": []}},
        ))
        out = json.loads(cohort_tools.tool_list_classes(_ctx()))
        assert out["count"] == 0
        assert out["classes"] == []

    def test_http_error_is_returned_as_error_field(self, patch_client):
        patch_client(lambda _r: httpx.Response(500, text="boom"))
        out = json.loads(cohort_tools.tool_list_classes(_ctx()))
        assert "error" in out
        assert "500" in out["error"]


# ---------------------------------------------------------------------------
# list_properties_of
# ---------------------------------------------------------------------------


class TestListPropertiesOf:
    def test_happy_path(self, patch_client):
        patch_client(lambda _r: httpx.Response(200, json=_ONT_PAYLOAD))
        out = json.loads(
            cohort_tools.tool_list_properties_of(
                _ctx(), class_uri="https://ex.com/onto/Consultant"
            )
        )
        assert out["class_label"] == "Consultant"
        prop_uris = [p["uri"] for p in out["data_properties"]]
        assert "https://ex.com/onto/status" in prop_uris
        assert "https://ex.com/onto/level" in prop_uris
        # Object properties are filtered by domain — only ``staffedOn`` matches.
        op_uris = [p["uri"] for p in out["object_properties"]]
        assert op_uris == ["https://ex.com/onto/staffedOn"]

    def test_unknown_class_uri(self, patch_client):
        patch_client(lambda _r: httpx.Response(200, json=_ONT_PAYLOAD))
        out = json.loads(
            cohort_tools.tool_list_properties_of(
                _ctx(), class_uri="https://ex.com/onto/Unknown"
            )
        )
        assert "error" in out
        assert "Unknown" in out["error"]

    def test_missing_class_uri(self):
        out = json.loads(cohort_tools.tool_list_properties_of(_ctx()))
        assert out.get("error", "").startswith("Missing")

    def test_owl_parser_local_name_domain_is_matched(self, patch_client):
        """Regression: ``OntologyParser`` strips ``rdfs:domain`` to a local
        name. The agent must still match the property to the class URI."""
        ont = {
            "success": True,
            "ontology": {
                "base_uri": "https://ex.com/onto/",
                "classes": [
                    {
                        "uri": "https://ex.com/onto/Consultant",
                        "name": "Consultant",
                        "label": "Consultant",
                        "dataProperties": [
                            {
                                "uri": "https://ex.com/onto/status",
                                "name": "status",
                                "label": "status",
                            }
                        ],
                    }
                ],
                "properties": [
                    {
                        "uri": "https://ex.com/onto/staffedOn",
                        "name": "staffedOn",
                        "type": "ObjectProperty",
                        "domain": "Consultant",  # local name, not full URI
                        "range": "Project",
                    },
                ],
            },
        }
        patch_client(lambda _r: httpx.Response(200, json=ont))
        out = json.loads(
            cohort_tools.tool_list_properties_of(
                _ctx(), class_uri="https://ex.com/onto/Consultant"
            )
        )
        op_uris = [p["uri"] for p in out["object_properties"]]
        assert "https://ex.com/onto/staffedOn" in op_uris
        assert out["object_properties_domain_unknown"] is False

    def test_lowercased_domain_is_matched(self, patch_client):
        """Wizard-imported properties may store ``domain`` as a lowercased
        local name -- still match it case-insensitively."""
        ont = {
            "success": True,
            "ontology": {
                "base_uri": "https://ex.com/onto/",
                "classes": [
                    {
                        "uri": "https://ex.com/onto/Consultant",
                        "name": "Consultant",
                        "label": "Consultant",
                        "dataProperties": [],
                    }
                ],
                "properties": [
                    {
                        "uri": "https://ex.com/onto/staffedOn",
                        "name": "staffedOn",
                        "type": "ObjectProperty",
                        "domain": "consultant",  # lowercased
                    },
                ],
            },
        }
        patch_client(lambda _r: httpx.Response(200, json=ont))
        out = json.loads(
            cohort_tools.tool_list_properties_of(
                _ctx(), class_uri="https://ex.com/onto/Consultant"
            )
        )
        op_uris = [p["uri"] for p in out["object_properties"]]
        assert "https://ex.com/onto/staffedOn" in op_uris

    def test_missing_uri_is_synthesised_from_base(self, patch_client):
        """Regression: ``dataProperties`` entries created by wizard / RDFS
        flows often lack a ``uri``. Synthesise from base_uri + name so the
        LLM gets a usable identifier."""
        ont = {
            "success": True,
            "ontology": {
                "base_uri": "https://ex.com/onto",
                "classes": [
                    {
                        "uri": "https://ex.com/onto/Consultant",
                        "name": "Consultant",
                        "label": "Consultant",
                        "dataProperties": [
                            {"name": "status", "label": "status"},  # no uri
                            {"localName": "level"},  # no uri or name
                        ],
                    }
                ],
                "properties": [],
            },
        }
        patch_client(lambda _r: httpx.Response(200, json=ont))
        out = json.loads(
            cohort_tools.tool_list_properties_of(
                _ctx(), class_uri="https://ex.com/onto/Consultant"
            )
        )
        uris = [p["uri"] for p in out["data_properties"]]
        assert "https://ex.com/onto/status" in uris
        assert "https://ex.com/onto/level" in uris
        for entry in out["data_properties"]:
            assert entry["uri_synthesised"] is True

    def test_no_matching_domain_falls_back_to_all_object_properties(
        self, patch_client
    ):
        """When no property's domain matches the class, return ALL object
        properties (mirroring the form's permissive picker) flagged with
        ``domain_unknown`` so the LLM knows it's a guess."""
        ont = {
            "success": True,
            "ontology": {
                "base_uri": "https://ex.com/onto/",
                "classes": [
                    {
                        "uri": "https://ex.com/onto/Consultant",
                        "name": "Consultant",
                        "dataProperties": [],
                    }
                ],
                "properties": [
                    {
                        "uri": "https://ex.com/onto/staffedOn",
                        "name": "staffedOn",
                        "type": "ObjectProperty",
                        "domain": "",  # missing
                        "range": "Project",
                    },
                    {
                        "uri": "https://ex.com/onto/owns",
                        "name": "owns",
                        "type": "ObjectProperty",
                        "domain": "",
                        "range": "Asset",
                    },
                ],
            },
        }
        patch_client(lambda _r: httpx.Response(200, json=ont))
        out = json.loads(
            cohort_tools.tool_list_properties_of(
                _ctx(), class_uri="https://ex.com/onto/Consultant"
            )
        )
        assert out["object_properties_domain_unknown"] is True
        op_uris = [p["uri"] for p in out["object_properties"]]
        assert "https://ex.com/onto/staffedOn" in op_uris
        assert "https://ex.com/onto/owns" in op_uris
        for entry in out["object_properties"]:
            assert entry["domain_unknown"] is True

    def test_global_datatype_props_supplement_class_block(self, patch_client):
        """Datatype properties declared in the global ``properties`` list
        with a matching domain are added to ``data_properties`` even when
        absent from the class's own ``dataProperties`` block."""
        ont = {
            "success": True,
            "ontology": {
                "base_uri": "https://ex.com/onto/",
                "classes": [
                    {
                        "uri": "https://ex.com/onto/Consultant",
                        "name": "Consultant",
                        "dataProperties": [],
                    }
                ],
                "properties": [
                    {
                        "uri": "https://ex.com/onto/status",
                        "name": "status",
                        "type": "DatatypeProperty",
                        "domain": "Consultant",
                        "range": "xsd:string",
                    },
                ],
            },
        }
        patch_client(lambda _r: httpx.Response(200, json=ont))
        out = json.loads(
            cohort_tools.tool_list_properties_of(
                _ctx(), class_uri="https://ex.com/onto/Consultant"
            )
        )
        uris = [p["uri"] for p in out["data_properties"]]
        assert "https://ex.com/onto/status" in uris


# ---------------------------------------------------------------------------
# count_class_members
# ---------------------------------------------------------------------------


class TestCountClassMembers:
    def test_happy_path(self, patch_client):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/dtwin/cohorts/preview/class-stats"
            assert request.url.params.get("class_uri") == "https://ex.com/onto/Consultant"
            assert request.url.params.get("registry_catalog") == "main"
            return httpx.Response(200, json={"success": True, "count": 137})

        patch_client(handler)
        out = json.loads(
            cohort_tools.tool_count_class_members(
                _ctx(), class_uri="https://ex.com/onto/Consultant"
            )
        )
        assert out == {
            "class_uri": "https://ex.com/onto/Consultant",
            "instance_count": 137,
        }

    def test_missing_class_uri(self):
        out = json.loads(cohort_tools.tool_count_class_members(_ctx()))
        assert "error" in out

    def test_backend_failure(self, patch_client):
        patch_client(
            lambda _r: httpx.Response(
                200, json={"success": False, "message": "domain not loaded"}
            )
        )
        out = json.loads(
            cohort_tools.tool_count_class_members(
                _ctx(), class_uri="https://ex.com/onto/X"
            )
        )
        assert "error" in out
        assert "domain not loaded" in out["error"]


# ---------------------------------------------------------------------------
# sample_values_of
# ---------------------------------------------------------------------------


class TestSampleValuesOf:
    def test_happy_path(self, patch_client):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/dtwin/cohorts/sample-values"
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "values": ["Exempt", "NonExempt", "Contractor"],
                    "truncated": False,
                },
            )

        patch_client(handler)
        out = json.loads(
            cohort_tools.tool_sample_values_of(
                _ctx(),
                class_uri="https://ex.com/onto/Consultant",
                property_uri="https://ex.com/onto/status",
                limit=15,
            )
        )
        assert captured["body"] == {
            "class_uri": "https://ex.com/onto/Consultant",
            "property": "https://ex.com/onto/status",
            "limit": 15,
        }
        assert out["values"] == ["Exempt", "NonExempt", "Contractor"]
        assert out["truncated"] is False

    def test_limit_clamping(self, patch_client):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"success": True, "values": []})

        patch_client(handler)
        cohort_tools.tool_sample_values_of(
            _ctx(),
            class_uri="https://ex.com/onto/Consultant",
            property_uri="https://ex.com/onto/status",
            limit=999,
        )
        assert captured["body"]["limit"] == 30

    def test_missing_args(self):
        out = json.loads(
            cohort_tools.tool_sample_values_of(
                _ctx(), class_uri="https://ex.com/onto/X"
            )
        )
        assert "error" in out
        assert "required" in out["error"]


# ---------------------------------------------------------------------------
# propose_rule (no HTTP — pure validation + side-effect on ctx.metadata)
# ---------------------------------------------------------------------------


class TestProposeRule:
    def _good_rule(self) -> dict:
        return {
            "id": "exempt-pool",
            "label": "Exempt staffing pool",
            "class_uri": "https://ex.com/onto/Consultant",
            "links": [
                {
                    "shared_class": "https://ex.com/onto/Project",
                    "via": "https://ex.com/onto/staffedOn",
                }
            ],
            "links_combine": "any",
            "compatibility": [
                {
                    "type": "value_equals",
                    "property": "https://ex.com/onto/status",
                    "value": "Exempt",
                }
            ],
            "group_type": "connected",
            "min_size": 2,
            "output": {"graph": True},
        }

    def test_valid_rule_is_parked_on_ctx(self):
        ctx = _ctx()
        out = json.loads(cohort_tools.tool_propose_rule(ctx, rule=self._good_rule()))
        assert out["valid"] is True
        assert out["rule_id"] == "exempt-pool"
        assert out["class_uri"] == "https://ex.com/onto/Consultant"
        assert out["links"] == 1
        assert out["compatibility"] == 1
        assert ctx.metadata["proposed_rule"]["id"] == "exempt-pool"

    def test_invalid_rule_returns_errors_and_does_not_park(self):
        ctx = _ctx()
        bad = self._good_rule()
        bad["class_uri"] = ""
        bad["compatibility"][0]["type"] = "samee_value"  # typo
        out = json.loads(cohort_tools.tool_propose_rule(ctx, rule=bad))
        assert out["valid"] is False
        assert isinstance(out["errors"], list) and out["errors"]
        assert "proposed_rule" not in ctx.metadata

    def test_non_dict_rule(self):
        out = json.loads(cohort_tools.tool_propose_rule(_ctx(), rule="not a dict"))
        assert "error" in out

    def test_unparseable_rule_is_reported(self):
        ctx = _ctx()
        out = json.loads(
            cohort_tools.tool_propose_rule(
                ctx,
                rule={"links": "not a list"},
            )
        )
        # CohortRule.from_dict raises on bad shapes — surfaced as error.
        assert ("error" in out) or (out.get("valid") is False)


# ---------------------------------------------------------------------------
# dry_run
# ---------------------------------------------------------------------------


class TestDryRun:
    def test_happy_path_truncates_clusters(self, patch_client):
        clusters = [
            {"id": f"c-{i}", "size": 10 - i, "members": [f"u{j}" for j in range(10 - i)], "idx": i}
            for i in range(8)
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/dtwin/cohorts/dry-run"
            body = json.loads(request.content.decode())
            assert body["id"] == "demo"
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "stats": {"matched_members": 42, "filtered_edges": 73},
                    "clusters": clusters,
                },
            )

        patch_client(handler)
        out = json.loads(
            cohort_tools.tool_dry_run(
                _ctx(),
                rule={"id": "demo", "label": "Demo", "class_uri": "x"},
            )
        )
        assert out["cluster_count"] == 8
        assert len(out["head"]) == 5  # truncated to 5
        assert all("members" not in h for h in out["head"])
        assert out["stats"]["matched_members"] == 42

    def test_validation_failure_propagates(self, patch_client):
        patch_client(
            lambda _r: httpx.Response(400, text='{"detail":{"error":"Cohort rule is invalid"}}')
        )
        out = json.loads(cohort_tools.tool_dry_run(_ctx(), rule={"id": "x"}))
        assert "error" in out
        assert "400" in out["error"]

    def test_non_dict_rule(self):
        out = json.loads(cohort_tools.tool_dry_run(_ctx(), rule=None))
        assert "error" in out
