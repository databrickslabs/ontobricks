"""OWL/RDFS parse, generate, and Turtle cleanup extracted from :class:`Ontology`.

Fowler Extract Class. ``Ontology`` keeps one-line delegators.
"""

from __future__ import annotations

from typing import Any, Dict

from back.core.logging import get_logger
from back.core.w3c import OntologyGenerator, OntologyParser
from shared.config.constants import DEFAULT_BASE_URI

logger = get_logger(__name__)

class OntologyOwl:
    """Generate/parse OWL Turtle and compute lightweight stats."""

    @staticmethod
    def get_ontology_stats(config: Dict[str, Any]) -> Dict[str, int]:
        """Get statistics from ontology configuration.

        Args:
            config: Ontology configuration dict

        Returns:
            dict: Stats with counts
        """
        return {
            "classes": len(config.get("classes", [])),
            "properties": len(config.get("properties", [])),
            "constraints": len(config.get("constraints", [])),
            "swrl_rules": len(config.get("swrl_rules", [])),
            "axioms": len(config.get("axioms", [])),
            "expressions": len(config.get("expressions", [])),
        }

    @staticmethod
    def postprocess_generated_owl(content: str) -> tuple:
        """Clean LLM output and compute stats in one step. Returns ``(turtle, stats)``."""
        turtle = OntologyOwl.clean_owl_output(content)
        stats = OntologyOwl.calculate_owl_stats(turtle)
        return turtle, stats

    @staticmethod
    def generate_owl(
        data,
        constraints=None,
        swrl_rules=None,
        axioms=None,
        expressions=None,
        groups=None,
    ):
        """Generate OWL from ontology configuration.

        Args:
            data: dict with base_uri, name, classes, properties
            constraints: list of property constraints (optional)
            swrl_rules: list of SWRL rules (optional)
            axioms: list of OWL axioms (optional)
            expressions: list of OWL class expressions (optional)
            groups: list of entity group definitions (optional)

        Returns:
            str: Generated OWL content
        """
        generator = OntologyGenerator(
            base_uri=data.get("base_uri") or DEFAULT_BASE_URI,
            ontology_name=data.get("name", "MyOntology"),
            classes=data.get("classes", []),
            properties=data.get("properties", []),
            label_lang=data.get("label_lang"),
            constraints=constraints,
            swrl_rules=swrl_rules,
            axioms=axioms,
            expressions=expressions,
            groups=groups,
        )
        return generator.generate()

    @staticmethod
    def parse_owl(content, extract_advanced=True):
        """Parse OWL content and return structured data.

        Args:
            content: OWL/Turtle content
            extract_advanced: If True, also extract constraints, SWRL rules, axioms, expressions, and groups

        Returns:
            tuple: (ontology_info, classes, properties) or
                   (ontology_info, classes, properties, constraints, swrl_rules, axioms, expressions, groups)
                   if extract_advanced=True
        """
        parser = OntologyParser(content)
        ontology_info = parser.get_ontology_info()
        classes = parser.get_classes()
        properties = parser.get_properties()

        if extract_advanced:
            constraints = parser.get_constraints()
            swrl_rules = parser.get_swrl_rules()
            split = parser.get_axioms_and_expressions()
            groups = parser.get_groups()
            return (
                ontology_info,
                classes,
                properties,
                constraints,
                swrl_rules,
                split["axioms"],
                split["expressions"],
                groups,
            )

        return ontology_info, classes, properties

    @staticmethod
    def parse_rdfs(content):
        """Parse RDFS content and return structured data.

        Args:
            content: RDFS content (Turtle, RDF/XML, N3, etc.)

        Returns:
            tuple: (ontology_info, classes, properties)
        """
        from back.core.w3c import RDFSParser

        parser = RDFSParser(content)
        ontology_info = parser.get_ontology_info()
        classes = parser.get_classes()
        properties = parser.get_properties()

        return ontology_info, classes, properties

    @staticmethod
    def _turtle_to_camel(words: list, is_pascal: bool) -> str:
        """Convert a list of words to camelCase or PascalCase."""
        if not words:
            return ""
        if is_pascal:
            return "".join(w.capitalize() for w in words if w)
        result = words[0].lower()
        for w in words[1:]:
            if w:
                result += w.capitalize()
        return result

    @staticmethod
    def _fix_snake_kebab_local_names(content: str) -> str:
        """Convert snake_case / kebab-case local names to camelCase in Turtle."""
        import re

        def _fix_match(match):
            prefix = match.group(1)
            name = match.group(2)
            words = re.split(r"[_-]+", name)
            if len(words) <= 1:
                return match.group(0)
            is_pascal = words[0] and words[0][0].isupper()
            return prefix + OntologyOwl._turtle_to_camel(words, is_pascal)

        pattern = r"(?<![a-zA-Z])(:)([a-zA-Z][a-zA-Z0-9]*(?:[_-][a-zA-Z][a-zA-Z0-9]*)+)"
        return re.sub(pattern, _fix_match, content)

    @staticmethod
    def _fix_spaced_local_names(content: str) -> str:
        """Join space-separated words in bare ``:LocalName`` tokens."""
        _TURTLE_KEYWORDS = frozenset(
            {"a", "rdf", "rdfs", "owl", "xsd", "xml", "true", "false"}
        )

        def _fix_line(line: str) -> str:
            stripped = line.strip()
            if (
                stripped.startswith("#")
                or stripped.startswith("@prefix")
                or stripped.startswith("@base")
            ):
                return line

            result: list = []
            i = 0
            while i < len(line):
                if line[i] == ":" and (i == 0 or line[i - 1] in " \t;.,()[]"):
                    j = i + 1
                    if j >= len(line):
                        result.append(line[i])
                        i += 1
                        continue

                    words: list = []
                    current_word = ""
                    while j < len(line):
                        ch = line[j]
                        if ch.isalnum():
                            current_word += ch
                            j += 1
                        elif ch == " " and current_word:
                            k = j + 1
                            while k < len(line) and line[k] == " ":
                                k += 1
                            if k < len(line) and line[k].isalpha():
                                nwe = k
                                while nwe < len(line) and line[nwe].isalnum():
                                    nwe += 1
                                nw = line[k:nwe]
                                an = nwe
                                while an < len(line) and line[an] == " ":
                                    an += 1
                                if (
                                    (an < len(line) and line[an] == ":")
                                    or nw.lower() in _TURTLE_KEYWORDS
                                    or len(nw) == 1
                                ):
                                    words.append(current_word)
                                    break
                                words.append(current_word)
                                current_word = ""
                                j = k
                            elif k < len(line):
                                words.append(current_word)
                                break
                            else:
                                words.append(current_word)
                                break
                        else:
                            if current_word:
                                words.append(current_word)
                            break

                    if words:
                        is_pascal = words[0] and words[0][0].isupper()
                        result.append(":")
                        result.append(OntologyOwl._turtle_to_camel(words, is_pascal))
                        i = j
                    else:
                        result.append(line[i])
                        i += 1
                else:
                    result.append(line[i])
                    i += 1
            return "".join(result)

        return "\n".join(_fix_line(ln) for ln in content.split("\n"))

    @staticmethod
    def _fix_local_names(content: str) -> str:
        """Fix local names with spaces, underscores, or hyphens in Turtle content.

        Converts patterns like:
        - :street address -> :streetAddress
        - :Street Address -> :StreetAddress
        - :first_name -> :firstName
        - :customer-id -> :customerId
        """
        content = OntologyOwl._fix_snake_kebab_local_names(content)
        return OntologyOwl._fix_spaced_local_names(content)

    @staticmethod
    def clean_owl_output(content: str) -> str:
        """Clean up LLM output to extract valid Turtle content."""
        content = content.strip()

        # Remove markdown code fences
        if "```" in content:
            import re

            m = re.search(
                r"```(?:turtle|ttl|sparql|rdf)?\s*\n(.*?)```", content, re.DOTALL
            )
            if m:
                content = m.group(1).strip()
            elif content.startswith("```"):
                lines = content.split("\n")
                lines = lines[1:]
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
                content = "\n".join(lines)

        content = content.strip()

        # Strip any natural-language preamble before the first @prefix or @base
        prefix_idx = content.find("@prefix")
        base_idx = content.find("@base")
        candidates = [i for i in (prefix_idx, base_idx) if i > 0]
        if candidates:
            content = content[min(candidates) :]

        content = content.strip()

        # Fix any local names with spaces, underscores, or hyphens
        content = OntologyOwl._fix_local_names(content)

        return content

    @staticmethod
    def calculate_owl_stats(owl_content: str) -> Dict:
        """Calculate statistics from OWL content."""
        stats = {"classes": 0, "properties": 0, "dataProperties": 0}

        try:
            # Count owl:Class declarations
            stats["classes"] = owl_content.count("a owl:Class") + owl_content.count(
                "rdf:type owl:Class"
            )

            # Count owl:ObjectProperty declarations
            stats["properties"] = owl_content.count(
                "a owl:ObjectProperty"
            ) + owl_content.count("rdf:type owl:ObjectProperty")

            # Count owl:DatatypeProperty declarations
            stats["dataProperties"] = owl_content.count(
                "a owl:DatatypeProperty"
            ) + owl_content.count("rdf:type owl:DatatypeProperty")
        except Exception as exc:
            logger.warning("OWL stats calculation error: %s", exc)

        return stats
