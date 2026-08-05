OntoBricks Application Packages
================================

The application is split into five top-level packages:

- **back** — Backend domain objects, core infrastructure, services
- **front** — Frontend HTML routes, Jinja2 configuration, templates, static assets
- **shared** — Shared FastAPI factory, middleware, settings, constants
- **api** — External REST API, internal JSON API routers
- **agents** — LLM agent engines and tool functions

Subpackages
-----------

.. toctree::
   :maxdepth: 1

   app.fastapi
   app.core
   app.objects
   app.frontend
   app.config
