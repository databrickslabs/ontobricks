``back.core.external.pitfalls`` — Ontology Pitfalls Detector
============================================================

D2KLab Ontology-Pitfalls-Detector (OPD) integration. Detects 19 structural,
logical, and semantic pitfalls (P1.1–P4.7) across four categories.

Graph-only / fast checks run without optional ML dependencies.
Semantic similarity and NLP naming checks require the ``[pitfalls]`` extra
(``sentence-transformers``, ``scikit-learn``, ``nltk``, ``scipy``).

Package
-------

.. automodule:: back.core.external.pitfalls
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

Service
-------

.. automodule:: back.core.external.pitfalls.PitfallsService
   :members:
   :undoc-members:
   :show-inheritance:

Runner (OntologyPatternToolkit)
--------------------------------

.. automodule:: back.core.external.pitfalls.runner
   :members:
   :undoc-members:
   :show-inheritance:

Utilities
---------

.. automodule:: back.core.external.pitfalls.utils
   :members:
   :undoc-members:
   :show-inheritance:
