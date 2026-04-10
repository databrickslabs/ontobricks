``back.objects`` -- Application domain objects
==============================================

Session-scoped project state, ontology, mapping, and digital-twin query pipeline,
Unity Catalog registry, permissions, and related services. These packages
intentionally live outside
``back.core`` (which holds shared infrastructure: Databricks clients, triple
stores, W3C tooling, etc.). The package ``__init__`` is documentation-only;
concrete APIs live in the subpackages below.

Subpackages
-----------

.. toctree::
   :maxdepth: 1

   app.objects.project
   app.objects.ontology
   app.objects.mapping
   app.objects.digitaltwin
   app.objects.registry
   app.objects.session
