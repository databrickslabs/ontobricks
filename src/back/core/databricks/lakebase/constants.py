"""Shared tuning constants for Lakebase (Postgres) technical access.

These values were previously duplicated between the registry store
(``back.objects.registry.store.lakebase.store``) and the graph-db pool
(``back.core.graphdb.lakebase.pool``). They now live in one place so both
consumers share identical cold-start, retry and pool-lifetime behaviour.
"""

from __future__ import annotations

# SQLSTATEs used to classify connection failures during ``_open_one``.
COLD_START_SQLSTATES = frozenset({"57P03"})  # cannot_connect_now (scale-to-zero)
AUTH_FAILURE_SQLSTATES = frozenset({"28P01"})  # invalid_password / token expired

# Cold-start retry (Lakebase Autoscaling scales-to-zero when idle).
MAX_COLD_START_ATTEMPTS = 6
INITIAL_BACKOFF_S = 1.0
MAX_BACKOFF_S = 16.0

# Connection-pool tuning. ``POOL_MAX_LIFETIME_S`` is comfortably below the
# Lakebase JWT TTL (~1 h) so a connection is always retired before its
# credentials would expire mid-query. ``POOL_MAX_SIZE`` is small on purpose:
# both workloads are admin/low-concurrency traffic and Postgres connections
# are not cheap on the Lakebase side either.
POOL_MAX_SIZE = 4
POOL_MAX_LIFETIME_S = 45 * 60.0  # 45 min
POOL_ACQUIRE_TIMEOUT_S = 30.0

# JWT (Postgres password) lifetime — refresh ~5 min before the 1 h expiry.
TOKEN_TTL_S = 3300

# libpq ``application_name`` labels used for connection tracing. They select
# the per-workload pool namespace but are otherwise cosmetic.
APPLICATION_NAME_REGISTRY = "ontobricks-registry"
APPLICATION_NAME_GRAPH = "ontobricks-graphdb"
