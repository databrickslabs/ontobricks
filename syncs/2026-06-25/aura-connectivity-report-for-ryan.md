# Neo4j Aura connectivity issue — report for Ryan

**Date:** 2026-06-25
**Reporter:** Hugues Journeau (OntoBricks PR #47 smoke test)
**Aura instance:** `b4810af7.databases.neo4j.io:7687` (the one you gave us for the June 12 PFAS demo)

## TL;DR

We can't connect from the **Databricks Apps** container running `ontobricks-070` on the FEVM-Mjolnir workspace. The Neo4j Python driver inside the deployed app gets a `Name or service not known` DNS error.

**Side-channel checks suggest the issue is on the Databricks-Apps side, not Aura**:
- The hostname resolves fine from a public-internet macOS shell (returns 5 AWS IPs: `34.226.188.79`, `54.92.173.16`, `100.50.122.23`, `3.218.61.133`, `54.225.148.211`).
- Same hostname, same Aura URL we used successfully on **2026-06-12** for the 303-triple demo.
- We haven't touched the Aura instance since then.

So we strongly suspect a **network-egress restriction on the Databricks Apps platform** (no external DNS resolution to non-Databricks domains, or no TCP egress to `:7687`). We're checking on the Databricks side, but a quick "yes the instance is still up + credentials unchanged" from you would help us rule out the obvious.

## What we'd like you to check

1. Is the Aura instance `b4810af7.databases.neo4j.io` still **running** (not paused / not deleted)?
2. Are the **Bolt credentials** (the username `neo4j` + the password you shared with us on Slack) still valid?
3. If you ping/`cypher-shell` it from your laptop, does it respond?

If everything is green on your side, this is purely a Databricks Apps egress thing and we'll handle it.

## Evidence

### Error from the deployed Databricks App (`ontobricks-070`, FEVM-Mjolnir)

```
2026-06-25T14:43:49Z [INFO]  ontobricks.core.graphdb.neo4j.Neo4jConnection
                             Neo4j credentials sourced from NEO4J_PASSWORD env var
2026-06-25T14:43:49Z [INFO]  ontobricks.core.graphdb.neo4j.Neo4jConnection
                             Neo4j driver opened for neo4j+s://b4810af7.databases.neo4j.io (database=neo4j)
2026-06-25T14:43:49Z [DEBUG] ontobricks.core.graphdb.neo4j.Neo4jConnection
                             Cypher params: {'rdf_type': '...', 'rdfs_label': '...'}
2026-06-25T14:43:49Z [ERROR] ontobricks.routers.digitaltwin
                             dt_stats failed: Failed to DNS resolve address
                             b4810af7.databases.neo4j.io:7687: [Errno -2] Name or service not known
Traceback (most recent call last):
  File ".venv/lib/python3.11/site-packages/neo4j/_async_compat/network/_util.py", line 180, in _dns_resolver
    info = NetworkUtil.get_address_info(host, port, family, type, proto, flags)
  File ".venv/lib/python3.11/site-packages/neo4j/_async_compat/network/_util.py", line 166, in get_address_info
    return socket.getaddrinfo(host, port, family, type, proto, flags)
  File "/usr/lib/python3.11/socket.py", line 974, in getaddrinfo
    raise err_cls(...)
neo4j.exceptions.ServiceUnavailable:
    Failed to DNS resolve address b4810af7.databases.neo4j.io:7687: [Errno -2] Name or service not known

2026-06-25T14:43:49Z [WARNING] InfrastructureError [502]: Triple store stats retrieval failed
                              (detail=Failed to DNS resolve ...)
```

### Same hostname from a regular shell (resolves fine)

```
$ dig +short b4810af7.databases.neo4j.io
34.226.188.79
54.92.173.16
100.50.122.23
3.218.61.133
54.225.148.211
```

### Driver / library

- `neo4j-python-driver` 5.x (latest at deploy time)
- URI scheme: `neo4j+s://` (TLS embedded)
- Auth: `basic` (username `neo4j`, password from Databricks Apps secret resource)

## What we did on our side

For context: we just shipped a hardening of the credential path that lives on top of the v0.6 demo — the password no longer travels through `engine_config` JSON. It's pulled at runtime from a Databricks Apps secret resource (`ontobricks/neo4j-password`) injected as the `NEO4J_PASSWORD` env var into the deployed app. The log line *"Neo4j credentials sourced from NEO4J_PASSWORD env var"* confirms it.

So no credentials drift on our side either — we re-put your password into our workspace secret today (2026-06-25), bound the Apps resource to it, deployed, and the auth path resolved before the DNS failed. Bolt handshake never even tried because DNS aborts before TCP.

Thanks Ryan!
