"""LLM Turtle cleanup helpers on :class:`Owl` (release 0.9.0 extract)."""

import pytest

from back.objects.ontology.Ontology import Ontology
from back.objects.ontology.Owl import Owl

pytestmark = pytest.mark.unit


class TestTurtleToCamel:
    def test_empty_word_list_yields_empty_string(self):
        assert Owl._turtle_to_camel([], is_pascal=True) == ""

    def test_camel_case_skips_empty_words(self):
        assert Owl._turtle_to_camel(["First", "", "name"], is_pascal=False) == "firstName"

    def test_pascal_case_joins_capitalized_words(self):
        assert Owl._turtle_to_camel(["street", "address"], is_pascal=True) == "StreetAddress"


class TestSnakeKebabFix:
    @pytest.mark.parametrize(
        "source, expected",
        [
            (":first_name a", ":firstName a"),
            (":customer-id a", ":customerId a"),
            (":Street_Address a", ":StreetAddress a"),
            ("ex:first_name a", "ex:first_name a"),
            (":single a", ":single a"),
        ],
    )
    def test_converts_only_bare_local_names(self, source, expected):
        assert Owl._fix_snake_kebab_local_names(source) == expected


class TestSpacedFix:
    @pytest.mark.parametrize(
        "source, expected",
        [
            (":street address a owl:DatatypeProperty .", ":streetAddress a owl:DatatypeProperty ."),
            (":Street Address a owl:Class .", ":StreetAddress a owl:Class ."),
            (":big red car .", ":bigRedCar ."),
            (":street address.", ":streetAddress."),
            (":Order date x .", ":OrderDate x ."),
        ],
    )
    def test_joins_space_separated_words(self, source, expected):
        assert Owl._fix_spaced_local_names(source) == expected

    @pytest.mark.parametrize(
        "line",
        [
            "# :a comment line",
            "@prefix : <http://x.org#> .",
            "@base <http://x.org/> .",
            "ex:x :",
            ":(",
            ":Order 123",
        ],
    )
    def test_leaves_comments_directives_and_non_names_unchanged(self, line):
        assert Owl._fix_spaced_local_names(line) == line

    def test_word_followed_by_another_bare_name_is_not_merged(self):
        assert Owl._fix_spaced_local_names(":Order date :foo .") == ":Order date :foo ."

    def test_trailing_spaces_kept(self):
        assert Owl._fix_spaced_local_names(":Order   ") == ":Order   "

    @pytest.mark.parametrize(
        "line",
        [
            ":OrderLine a owl:Class .",
            ":hasName rdfs:domain :Customer .",
            ":Customer rdfs:subClassOf :LegalEntity .",
        ],
    )
    def test_existing_camel_case_names_are_preserved(self, line):
        assert Owl._fix_spaced_local_names(line) == line


class TestFixLocalNames:
    def test_snake_case_name_becomes_camel_case(self):
        assert Owl._fix_local_names(":first_name a owl:DatatypeProperty .") == (
            ":firstName a owl:DatatypeProperty ."
        )


class TestCleanOwlOutput:
    def test_extracts_fenced_turtle_block_and_drops_prose(self):
        content = "Here you go:\n```turtle\n@prefix : <http://x#> .\n:street address a owl:Class .\n```\nbye"
        assert Owl.clean_owl_output(content) == (
            "@prefix : <http://x#> .\n:streetAddress a owl:Class ."
        )

    def test_unlabelled_fence_without_closing_newline(self):
        assert Owl.clean_owl_output("```\n@prefix : <http://x#> .\n:A a owl:Class .") == (
            "@prefix : <http://x#> .\n:A a owl:Class ."
        )

    def test_fence_with_closing_marker_is_stripped(self):
        content = "```\n@prefix : <http://x#> .\n:A a owl:Class .\n```"
        assert Owl.clean_owl_output(content) == "@prefix : <http://x#> .\n:A a owl:Class ."

    def test_unknown_language_fence_is_stripped(self):
        content = "```owl\n@prefix : <http://x#> .\n:A a owl:Class .\n```"
        assert Owl.clean_owl_output(content) == "@prefix : <http://x#> .\n:A a owl:Class ."

    def test_preamble_before_prefix_removed(self):
        content = "Sure! here\n@prefix : <http://x#> .\n:A a owl:Class ."
        assert Owl.clean_owl_output(content) == "@prefix : <http://x#> .\n:A a owl:Class ."

    def test_preamble_before_base_removed(self):
        content = "text\n@base <http://x/> .\n@prefix : <http://x#> ."
        assert Owl.clean_owl_output(content).startswith("@base")


class TestOwlStats:
    def test_counts_both_declaration_styles(self):
        stats = Owl.calculate_owl_stats(
            "a owl:Class rdf:type owl:Class a owl:ObjectProperty "
            "rdf:type owl:DatatypeProperty"
        )
        assert stats == {"classes": 2, "properties": 1, "dataProperties": 1}

    def test_invalid_input_returns_zero_stats(self):
        assert Owl.calculate_owl_stats(None) == {
            "classes": 0, "properties": 0, "dataProperties": 0,
        }

    def test_facade_postprocess_returns_turtle_and_stats(self):
        turtle, stats = Ontology.postprocess_generated_owl("@prefix : <http://x#> .\n:A a owl:Class .")
        assert turtle.endswith(":A a owl:Class .")
        assert stats["classes"] == 1


class TestFacadeCleanupDelegators:
    def test_facade_matches_owl_helpers(self):
        assert Ontology._turtle_to_camel(["a", "b"], False) == "aB"
        assert Ontology._fix_snake_kebab_local_names(":a_b x") == ":aB x"
        assert Ontology._fix_spaced_local_names(":big red car .") == ":bigRedCar ."
        assert Ontology._fix_local_names(":street address .") == ":streetAddress ."
        assert Ontology.clean_owl_output("  @prefix : <a#> .  ") == "@prefix : <a#> ."
        assert Ontology.calculate_owl_stats("a owl:Class") == {
            "classes": 1, "properties": 0, "dataProperties": 0,
        }
