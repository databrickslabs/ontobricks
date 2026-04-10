``api`` -- External REST API (mounted at ``/api``)
===================================================

.. automodule:: api
   :members:
   :undoc-members:
   :show-inheritance:

Constants (versioning, paths, OpenAPI metadata)
------------------------------------------------

.. automodule:: api.constants
   :members:
   :undoc-members:
   :show-inheritance:

External Sub-Application
------------------------

.. automodule:: api.external_app
   :members:
   :undoc-members:
   :show-inheritance:

Routers package
---------------

.. automodule:: api.routers
   :members:
   :undoc-members:
   :show-inheritance:

API v1
------

.. automodule:: api.routers.v1
   :members:
   :undoc-members:
   :show-inheritance:

API Service
-----------

.. automodule:: api.service
   :members:
   :undoc-members:
   :show-inheritance:

Projects API (list & artifacts)
--------------------------------

.. automodule:: api.routers.projects
   :members:
   :undoc-members:
   :show-inheritance:

Digital Twin API
----------------

.. automodule:: api.routers.digitaltwin
   :members:
   :undoc-members:
   :show-inheritance:

GraphQL (mounted on external app)
---------------------------------

The GraphQL router is defined in ``back.fastapi.graphql_routes`` and included on the
external sub-application at ``/api/v1/graphql`` (see ``api.external_app``).

.. automodule:: back.fastapi.graphql_routes
   :members:
   :undoc-members:
   :show-inheritance:
