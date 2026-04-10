"""R2RML utilities for mapping generation and parsing."""
from back.core.w3c.r2rml.R2RMLGenerator import R2RMLGenerator
from back.core.w3c.r2rml.R2RMLParser import R2RMLParser


def generate_r2rml_from_config(mapping_config, ontology_config=None):
    """Backward-compatible wrapper for :meth:`R2RMLGenerator.generate_r2rml_from_config`."""
    return R2RMLGenerator.generate_r2rml_from_config(mapping_config, ontology_config)


def parse_r2rml_content(content):
    """Backward-compatible wrapper for :meth:`R2RMLParser.parse_r2rml_content`."""
    return R2RMLParser.parse_r2rml_content(content)


__all__ = [
    'R2RMLGenerator',
    'generate_r2rml_from_config',
    'R2RMLParser',
    'parse_r2rml_content',
]
