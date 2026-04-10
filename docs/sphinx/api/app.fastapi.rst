FastAPI application (UI server)
================================

The FastAPI stack is split across **shared** (factory and cross-cutting HTTP
concerns), **front** (UI-specific dependencies), and **back** (GraphQL and
domain-adjacent HTTP).

Application factory
-------------------

.. automodule:: shared.fastapi.main
   :members:
   :undoc-members:
   :show-inheritance:

Health
------

.. automodule:: shared.fastapi.health
   :members:
   :undoc-members:
   :show-inheritance:

Dependencies
------------

.. automodule:: front.fastapi.dependencies
   :members:
   :undoc-members:
   :show-inheritance:

GraphQL routes
--------------

The GraphQL router lives in ``back.fastapi.graphql_routes``. It is documented
in :doc:`api_external` (external mount ``/api/v1/graphql``; same handlers are
also mounted in-app at ``/graphql``).
