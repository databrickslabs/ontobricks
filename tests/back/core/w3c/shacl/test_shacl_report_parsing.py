"""An unparseable PySHACL report must fail loudly, never read as 'conforms'."""

from __future__ import annotations

import sys
import types

import pytest

from back.core.errors import InfrastructureError
from back.core.w3c.shacl.SHACLService import SHACLService

_VALID_REPORT = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://test.org/data/> .

[] a sh:ValidationReport ;
    sh:conforms false ;
    sh:result [
        a sh:ValidationResult ;
        sh:focusNode ex:alice ;
        sh:resultPath ex:email ;
        sh:resultMessage "Missing email" ;
        sh:resultSeverity sh:Violation
    ] .
"""


@pytest.mark.unit
class TestExtractViolations:
    def test_parses_validation_results(self):
        violations = SHACLService._extract_violations(_VALID_REPORT)
        assert violations == [
            {
                "focus_node": "http://test.org/data/alice",
                "result_path": "http://test.org/data/email",
                "message": "Missing email",
                "severity": "sh:Violation",
            }
        ]

    def test_raises_infrastructure_error_when_report_unparseable(self):
        with pytest.raises(InfrastructureError):
            SHACLService._extract_violations("this is @@@ not turtle <<<")

    def test_validate_graph_surfaces_unparseable_report(self, monkeypatch):
        fake = types.ModuleType("pyshacl")
        fake.validate = lambda **_kw: (False, "garbage @@@ <<<", "report")
        monkeypatch.setitem(sys.modules, "pyshacl", fake)

        with pytest.raises(InfrastructureError):
            SHACLService(base_uri="http://test.org/ontology#").validate_graph(
                "", shapes=[]
            )
