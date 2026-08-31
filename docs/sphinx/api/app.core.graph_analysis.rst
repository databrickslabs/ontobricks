``back.core.graph_analysis`` -- Community Detection & Clustering
================================================================

Models
------

.. automodule:: back.core.graph_analysis.models
   :members:
   :undoc-members:
   :show-inheritance:

Job Metrics
-----------

Source resolution and read-back for the Databricks analytics job. The job
always scans a Delta table: :func:`~back.core.graph_analysis.JobMetrics.analytics_snapshot`
materializes a disposable one for a domain whose ``…_data`` is a pass-through
view, and drops it when the run ends.

.. automodule:: back.core.graph_analysis.JobMetrics
   :members:
   :undoc-members:
   :show-inheritance:

Community Detector
------------------

.. automodule:: back.core.graph_analysis.CommunityDetector
   :members:
   :undoc-members:
   :show-inheritance:

Cohort Builder
--------------

.. automodule:: back.core.graph_analysis.CohortBuilder
   :members:
   :undoc-members:
   :show-inheritance:

Cohort Vocabulary
-----------------

Single class that owns every cohort-related URI fragment and predicate.
Used by the engine, materialiser, and route layer to agree on a single
set of cohort URIs derived from a domain ``base_uri``.

.. automodule:: back.core.graph_analysis.CohortVocabulary
   :members:
   :undoc-members:
   :show-inheritance:
