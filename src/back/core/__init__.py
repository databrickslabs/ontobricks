"""Core functionality for OntoBricks (infrastructure: Databricks, W3C, graph stores, ...).

The package re-exports nothing: callers import the sub-package that does the
work (``back.core.databricks``, ``back.core.helpers``, ``back.core.w3c``, ...),
so importing one of them (e.g. ``back.core.streaming`` on the realtime job
cluster) does not import the others.
"""
