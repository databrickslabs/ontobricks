"""Tests for back.core.w3c.rdf_utils – RDF parsing utilities."""

import pytest

from back.core.w3c.rdf_utils import uri_local_name, parse_rdf_flexible


class TestUriLocalName:
    def test_fragment(self):
        assert uri_local_name("http://example.org/ontology#Customer") == "Customer"

    def test_path_segment(self):
        assert uri_local_name("http://example.org/ontology/Customer") == "Customer"

    def test_no_fragment_no_slash(self):
        assert uri_local_name("urn:uuid:abc") == "urn:uuid:abc"

    def test_empty_string(self):
        assert uri_local_name("") == ""

    def test_trailing_hash(self):
        assert uri_local_name("http://example.org/ns#") == ""

    def test_multiple_fragments(self):
        assert uri_local_name("http://example.org#ns#local") == "local"


class TestParseRdfFlexible:
    def test_turtle_format(self):
        turtle = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        <http://test.org/ont> a owl:Ontology .
        """
        graph = parse_rdf_flexible(turtle)
        assert len(graph) > 0

    def test_xml_format(self):
        xml = """<?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                 xmlns:owl="http://www.w3.org/2002/07/owl#">
            <owl:Ontology rdf:about="http://test.org/ont"/>
        </rdf:RDF>
        """
        graph = parse_rdf_flexible(xml)
        assert len(graph) > 0

    def test_invalid_content_raises(self):
        with pytest.raises(ValueError, match="Could not parse RDF"):
            parse_rdf_flexible("this is not RDF at all")

    def test_custom_formats_tuple(self):
        turtle = "@prefix owl: <http://www.w3.org/2002/07/owl#> . <http://t.org> a owl:Ontology ."
        graph = parse_rdf_flexible(turtle, formats=("turtle",))
        assert len(graph) > 0
