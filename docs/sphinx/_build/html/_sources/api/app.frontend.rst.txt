``front.routes`` & internal JSON API -- Web UI and HTMX/API helpers
====================================================================

HTML routes (Jinja2)
--------------------

.. automodule:: front.routes.home
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: front.routes.ontology
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: front.routes.mapping
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: front.routes.dtwin
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: front.routes.domain
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: front.routes.registry
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: front.routes.resolve
   :members:
   :undoc-members:
   :show-inheritance:

Internal JSON API routers
-------------------------

.. automodule:: api.routers.internal.home
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: api.routers.internal.settings
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: api.routers.internal.ontology
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: api.routers.internal.mapping
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: api.routers.internal.dtwin
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: api.routers.internal.domain
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: api.routers.internal.tasks
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: api.routers.internal.help
   :members:
   :undoc-members:
   :show-inheritance:

Home and settings orchestration
-------------------------------

Internal JSON routers above delegate to ``HomeService`` and ``SettingsService`` in
``back.objects.domain`` — see :doc:`app.objects.domain`.
