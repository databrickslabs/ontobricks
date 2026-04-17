# Third-Party Licenses

This folder contains the license files for all external libraries used by OntoBricks.

---

## JavaScript / CSS Libraries

| Library | Version | License | File |
|---------|---------|---------|------|
| Bootstrap | 5.3.2 | MIT | [javascript/bootstrap-LICENSE.txt](javascript/bootstrap-LICENSE.txt) |
| Bootstrap Icons | 1.11.2 | MIT | [javascript/bootstrap-icons-LICENSE.txt](javascript/bootstrap-icons-LICENSE.txt) |
| Chart.js | 4.x | MIT | [javascript/chartjs-LICENSE.txt](javascript/chartjs-LICENSE.txt) |
| D3.js | 7.x | ISC | [javascript/d3-LICENSE.txt](javascript/d3-LICENSE.txt) |
| GraphiQL | 3.x | MIT | [javascript/graphiql-LICENSE.txt](javascript/graphiql-LICENSE.txt) |
| Graphology | 0.26.0 | MIT | [javascript/graphology-LICENSE.txt](javascript/graphology-LICENSE.txt) |
| Graphology Library | 0.8.0 | MIT | [javascript/graphology-library-LICENSE.txt](javascript/graphology-library-LICENSE.txt) |
| Grid.js | latest | MIT | [javascript/gridjs-LICENSE.txt](javascript/gridjs-LICENSE.txt) |
| Marked | latest | MIT | [javascript/marked-LICENSE.txt](javascript/marked-LICENSE.txt) |
| React / ReactDOM | 18.x | MIT | [javascript/react-LICENSE.txt](javascript/react-LICENSE.txt) |
| Sigma.js | 3.0.2 | MIT | [javascript/sigma-LICENSE.txt](javascript/sigma-LICENSE.txt) |

## Python Libraries (Main Application)

| Library | Version | License | File |
|---------|---------|---------|------|
| aiofiles | >=23.0.0 | Apache-2.0 | [python/aiofiles-LICENSE.txt](python/aiofiles-LICENSE.txt) |
| APScheduler | >=3.10 | MIT | [python/apscheduler-LICENSE.txt](python/apscheduler-LICENSE.txt) |
| Databricks SDK | >=0.20.0 | Apache-2.0 | [python/databricks-sdk-LICENSE.txt](python/databricks-sdk-LICENSE.txt) |
| databricks-sql-connector | >=3.0.0 | Apache-2.0 | [python/databricks-sql-connector-LICENSE.txt](python/databricks-sql-connector-LICENSE.txt) |
| FastAPI | >=0.109.0 | MIT | [python/fastapi-LICENSE.txt](python/fastapi-LICENSE.txt) |
| ItsDangerous | >=2.1.0 | BSD-3-Clause | [python/itsdangerous-LICENSE.txt](python/itsdangerous-LICENSE.txt) |
| Jinja2 | >=3.1.0 | BSD-3-Clause | [python/jinja2-LICENSE.txt](python/jinja2-LICENSE.txt) |
| MLflow | >=2.19.0 | Apache-2.0 | [python/mlflow-LICENSE.txt](python/mlflow-LICENSE.txt) |
| NetworkX | >=3.0 | BSD-3-Clause | [python/networkx-LICENSE.txt](python/networkx-LICENSE.txt) |
| OWL-RL | >=7.0.0 | W3C Software License | [python/owlrl-LICENSE.txt](python/owlrl-LICENSE.txt) |
| PyArrow | >=14.0.0 | Apache-2.0 | [python/pyarrow-LICENSE.txt](python/pyarrow-LICENSE.txt) |
| Pydantic | >=2.5.0 | MIT | [python/pydantic-LICENSE.txt](python/pydantic-LICENSE.txt) |
| Pydantic Settings | >=2.1.0 | MIT | [python/pydantic-settings-LICENSE.txt](python/pydantic-settings-LICENSE.txt) |
| pySHACL | >=0.26.0 | Apache-2.0 | [python/pyshacl-LICENSE.txt](python/pyshacl-LICENSE.txt) |
| python-dotenv | >=1.0.0 | BSD-3-Clause | [python/python-dotenv-LICENSE.txt](python/python-dotenv-LICENSE.txt) |
| python-multipart | >=0.0.6 | Apache-2.0 | [python/python-multipart-LICENSE.txt](python/python-multipart-LICENSE.txt) |
| real_ladybug (LadybugDB) | >=0.1.0 | MIT | [python/real-ladybug-LICENSE.txt](python/real-ladybug-LICENSE.txt) |
| RDFLib | >=7.0.0 | BSD-3-Clause | [python/rdflib-LICENSE.txt](python/rdflib-LICENSE.txt) |
| Requests | >=2.31.0 | Apache-2.0 | [python/requests-LICENSE.txt](python/requests-LICENSE.txt) |
| Starlette | >=0.35.0 | BSD-3-Clause | [python/starlette-LICENSE.txt](python/starlette-LICENSE.txt) |
| Strawberry GraphQL | >=0.220.0 | MIT | [python/strawberry-graphql-LICENSE.txt](python/strawberry-graphql-LICENSE.txt) |
| Uvicorn | >=0.27.0 | BSD-3-Clause | [python/uvicorn-LICENSE.txt](python/uvicorn-LICENSE.txt) |

## Python Libraries (MCP Server)

| Library | Version | License | File |
|---------|---------|---------|------|
| FastMCP | >=2.3.1 | Apache-2.0 | [python/fastmcp-LICENSE.txt](python/fastmcp-LICENSE.txt) |
| HTTPX | >=0.25.0 | BSD-3-Clause | [python/httpx-LICENSE.txt](python/httpx-LICENSE.txt) |

> FastAPI, Uvicorn, Pydantic, and Databricks SDK are also used by the MCP server; their licenses are listed in the main application section above.

## Fonts

| Font | License | File |
|------|---------|------|
| JetBrains Mono / Outfit (Google Fonts) | SIL OFL 1.1 | [fonts/google-fonts-LICENSE.txt](fonts/google-fonts-LICENSE.txt) |

---

## License Summary

| License Type | Libraries |
|-------------|-----------|
| **MIT** | Bootstrap, Bootstrap Icons, Chart.js, GraphiQL, Graphology, Graphology Library, Grid.js, Marked, React/ReactDOM, Sigma.js, APScheduler, FastAPI, Pydantic, Pydantic Settings, real_ladybug, Strawberry GraphQL |
| **ISC** | D3.js |
| **BSD-3-Clause** | HTTPX, ItsDangerous, Jinja2, NetworkX, python-dotenv, RDFLib, Starlette, Uvicorn |
| **Apache-2.0** | aiofiles, Databricks SDK, databricks-sql-connector, FastMCP, MLflow, PyArrow, pySHACL, python-multipart, Requests |
| **W3C Software License** | OWL-RL |
| **SIL OFL 1.1** | JetBrains Mono, Outfit (Google Fonts) |

All dependencies use permissive open-source licenses compatible with commercial use.
