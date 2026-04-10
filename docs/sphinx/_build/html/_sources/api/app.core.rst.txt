``back.core`` -- Core Domain Logic
===================================

Shared infrastructure (Databricks, triple stores, W3C tooling, GraphQL core,
etc.). Submodules are documented below; the package ``__init__`` only
re-exports symbols and is not duplicated here.

Helpers
-------

.. automodule:: back.core.helpers
   :members:
   :undoc-members:
   :show-inheritance:

Logging
-------

.. automodule:: back.core.logging
   :members:
   :undoc-members:
   :show-inheritance:

Task Manager
------------

.. automodule:: back.core.task_manager
   :members:
   :undoc-members:
   :show-inheritance:

Subpackages
-----------

.. toctree::
   :maxdepth: 1

   app.core.errors
   app.core.databricks
   app.core.graphql
   app.core.industry
   app.core.reasoning
   app.core.sqlwizard
   app.core.triplestore
   app.core.w3c
