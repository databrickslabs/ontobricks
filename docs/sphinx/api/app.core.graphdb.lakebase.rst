``back.core.graphdb.lakebase`` — Lakebase Postgres graph engine
===============================================================

Flat triple tables on the App-bound Lakebase Postgres instance.
Configured globally under **Settings → Graph DB** when ``engine=lakebase``.

Package
-------

.. automodule:: back.core.graphdb.lakebase
   :members:
   :undoc-members:
   :show-inheritance:

Base helpers
------------

.. automodule:: back.core.graphdb.lakebase.LakebaseBase
   :members:
   :undoc-members:
   :show-inheritance:

Flat store
----------

.. automodule:: back.core.graphdb.lakebase.LakebaseFlatStore
   :members:
   :undoc-members:
   :show-inheritance:

Bulk ingestion contract
~~~~~~~~~~~~~~~~~~~~~~~

The Knowledge Graph build pipeline never holds the full graph in memory when
the active engine is Lakebase. Triples flow from the Databricks SQL
warehouse to Postgres in fixed-size batches via the streaming bulk paths:

* :py:meth:`back.core.graphdb.lakebase.LakebaseFlatStore.bulk_insert_iter`
  — per batch ``CREATE TEMP TABLE _ob_copy_stage … ON COMMIT DROP``,
  ``COPY FROM STDIN``, then
  ``INSERT INTO {phy} … SELECT FROM _ob_copy_stage ON CONFLICT DO NOTHING``.
* :py:meth:`back.core.graphdb.lakebase.LakebaseFlatStore.bulk_delete_iter`
  — symmetrical ``COPY`` into ``_ob_del_stage`` followed by
  ``DELETE FROM {phy} USING _ob_del_stage d WHERE …``. Replaces the per-row
  ``DELETE`` loop on the incremental remove path.

Both paths run inside an explicit ``conn.transaction()`` because the
graph-DB connection pool uses ``autocommit=True``; ``ON COMMIT DROP`` would
otherwise fire immediately after the temp-table ``CREATE``.

Public ``insert_triples`` / ``delete_triples`` keep their signatures and
delegate to the bulk iterator paths once the payload crosses
``_BULK_INSERT_THRESHOLD`` / ``_BULK_DELETE_THRESHOLD`` (50 rows). Smaller
payloads stay on the ``executemany`` / per-row fallback to avoid temp-table
overhead for trivial diffs.

Managed-synced mode (data plane only)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When ``graph_engine_config.sync_mode = "managed_synced"``, the bulk R2RML
data movement is delegated to a Databricks Lakeflow snapshot pipeline; the
FastAPI process never iterates triples for ingest. The Postgres layout per
graph version becomes a triad in the same ``ontobricks_graph`` schema:

* ``g_<dom>_v<n>_sync`` — read-only synced table maintained by Lakeflow.
* ``g_<dom>_v<n>__app`` — writable companion populated by reasoning + cohort.
* ``g_<dom>_v<n>`` — UNION view (back-compat name) that readers query.

:py:class:`back.core.graphdb.lakebase.LakebaseFlatStore` routes direct
writes (``insert_triples``, ``delete_triples``, COPY-based bulk paths) to
the companion via ``_writable_table_id``, and routes reads
(``query_triples``, ``count_triples``, ``iter_triples``, ``table_exists``,
``get_status``) to the union view via ``_readable_table_id``. Existing
SPARQL / KG-search code paths see no behavioural change.

The Lakeflow side is driven through a thin
:py:class:`back.core.graphdb.lakebase.SyncedTableManager`:

* ``ensure(name, source_table_full_name, primary_key_columns, sync_mode)``
  — idempotent create wrapping ``WorkspaceClient.database.create_synced_database_table``.
* ``trigger_refresh(name)`` / ``wait_for_completion(name, timeout_s)`` /
  ``trigger_and_wait(...)`` — kicks the underlying Lakeflow pipeline and
  blocks until the synced table reaches ``ONLINE`` / ``ONLINE_NO_PENDING_UPDATE``.
* ``delete(name, purge_data=True)`` — used during graph-version teardown.

The build pipeline orchestrates the full sequence in
:py:meth:`back.objects.digitaltwin._build_pipeline._BuildPipeline._apply_via_synced_pipeline`
and the scheduler mirrors it in
``back.objects.registry.scheduler._apply_synced_pipeline``.

Synced table manager
~~~~~~~~~~~~~~~~~~~~

.. automodule:: back.core.graphdb.lakebase.SyncedTableManager
   :members:
   :undoc-members:
   :show-inheritance:

Companion + union view DDL
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: back.core.graphdb.lakebase._companion_ddl
   :members:
   :undoc-members:
   :show-inheritance:

Connection pool
---------------

.. automodule:: back.core.graphdb.lakebase.pool
   :members:
   :undoc-members:
   :show-inheritance:
