# Session files stop accumulating, and stop trusting the cookie as a path

Date: 2026-08-04
Status: approved, ready for implementation planning

## Problem

`FileSessionMiddleware` writes one JSON file per session under
`settings.session_dir` and never deletes any of them. Locally that directory had
grown to **327 MB across 82,200 files**, which is what made `make clean` fail
with `rm: fastapi_session: Directory not empty` — the running app was minting new
files faster than `rm` could walk the directory.

The file count is not explained by user count. Three defects compound:

**Every session id is written to disk the moment it is minted.** `dispatch`
calls `_save_session` immediately after `_generate_session_id`
(`FileSessionMiddleware.py:126` and `:137`), so a request that never touches the
session still leaves a 2-byte `{}` file behind. `_SESSION_BYPASS_PREFIXES`
already excludes `/static/`, `/tasks/`, `/health` and the doc routes, so these
are real application requests — but the overwhelming majority of them are
cookie-less API probes and polls that have no session state to keep.

**A cookie whose file is missing is answered with a brand new id.** When
`session_file.exists()` is false the middleware mints a replacement rather than
reusing the id the client already holds (`:131`). Under any concurrency this
multiplies: N simultaneous requests carrying the same missing-file cookie
produce N different ids and N files, N−1 of which are immediately orphaned.
This is also why every browser tab got a fresh session id after the session
directory was wiped.

**Nothing ever expires.** There is no reaper anywhere in the codebase. Files
outlive their cookies — `session_max_age` is 24 hours — by however long the
volume survives, which on a developer machine is forever.

While tracing this, a second and more serious problem surfaced. The cookie value
is used verbatim as a filename: `_get_session_id_from_cookie` returns
`request.cookies.get(...)` with no validation despite a docstring that claims to
validate, and `_save_session` joins it straight onto `session_dir`. A cookie of
`../../../../etc/foo` therefore resolves outside the session directory, and any
request that sets `session_modified` will `write_text` JSON over whatever is at
that path. That is an arbitrary-file-write primitive reachable by anyone who can
set a cookie, and the cookie is unsigned so nothing detects tampering.

## Decisions

Taken with the user during brainstorming. Each one closes off an alternative an
implementer would otherwise reopen.

1. **Session ids must be exactly 32 lowercase hex characters, and a failing
   cookie is treated as absent.** That is exactly the shape
   `str(uuid.uuid4()).replace("-", "")` produces, so no legitimate client is
   affected. Rejected: `os.path.basename`-style sanitising, which silently
   rewrites a hostile value into a plausible one instead of refusing it, and
   signing the cookie, which is a larger change that does not remove the need to
   validate the shape.
2. **Validation is a precondition for the rest of the design, not an optional
   hardening pass.** Reusing a client-supplied id whose file is missing (decision
   3) means the id reaches `write_text` on a path that no prior request created,
   so it can only be done safely once the shape is known.
3. **Files are created lazily.** Minting an id writes nothing; a valid-format
   cookie with no file keeps its id and starts from `{}`. A file appears only
   when `request.state.session_modified` is set, which is the existing save path
   at the end of `dispatch`. This removes both the per-request empty file and the
   concurrent-mint multiplication in one move.
4. **`mtime` tracks last *use*, not last *edit*.** A session is touched with
   `os.utime` on any request that presents a valid id with an existing file, but
   only when the recorded `mtime` is already more than an hour old. Without this,
   "expired" would mean "not modified for 24h" — and because the cookie's
   `max_age` is refreshed on every response while the file's `mtime` is not, a
   user browsing read-only for over a day would hold a valid cookie pointing at a
   reap-eligible file and silently lose their session at the next restart.
   Rejected: touching on every request (unbounded metadata writes for no gain)
   and widening the reaper's threshold to 48h (narrows the window instead of
   closing it).
5. **The sweep runs at startup only.** Once creation is lazy, files exist only
   for sessions that did something, so growth tracks real users rather than raw
   request count and a boot-time sweep is enough. A periodic background sweep was
   considered and deliberately deferred.
6. **The in-memory `_session_cache` leak is out of scope.** It is a separate
   unbounded-growth bug with a different fix (eviction policy). It shrinks
   incidentally here, because minted-but-unmodified sessions are no longer
   inserted into it.

## Architecture

One module changes plus one line in the lifespan. No new settings, no schema
change, no data migration.

### Session id validation

A module-level compiled pattern in `FileSessionMiddleware.py`, matched with
`fullmatch`:

```python
_SESSION_ID_RE = re.compile(r"[0-9a-f]{32}")
```

`fullmatch` rather than `match` with `^...$` is load-bearing. Python's `$`
also matches immediately before a trailing newline, so an anchored `^[0-9a-f]{32}$`
would accept `"0"*32 + "\n"` — a 33-character value that resolves to a different
filename than the one validated. There is a test for exactly this.

`_get_session_id_from_cookie` gains the validation its docstring already
promises: a missing cookie returns `None` as today, and a present-but-malformed
cookie also returns `None`, logging at debug level with the value truncated. The
caller in `dispatch` already handles `None` by minting, so the rejection path
needs no new branch.

Returning `None` rather than raising matters: a user carrying a corrupted cookie
gets a fresh session, which is exactly what happens today, so there is no
functional regression for anyone legitimate.

### Lazy creation

The two `_save_session` calls in the mint branches of `dispatch` are removed, and
what remains is two branches rather than three:

- No cookie, or a cookie that fails validation → mint an id,
  `session_data = {}`, nothing written.
- Valid cookie → **keep the id** and `session_data = self._load_session(id)`.

Note what is gone: the `session_file.exists()` check at `:131`, and with it the
whole missing-file branch. `_load_session` already resolves all three cases —
cache hit, disk hit, or neither, returning `{}` — so the presence of the file
stops being `dispatch`'s concern. Collapsing it this way is not just tidier: the
`exists()` branch consults the filesystem while ignoring `_session_cache`, so a
session whose file was deleted under a live process would be reset even though
the data was still in memory. Deferring to `_load_session` preserves it.

The debug block that logs ontology-class and mapping counts moves out of the
former `else` branch to sit after the load, so it still runs for every returning
session.

The existing end-of-request block that saves when `session_modified` is set is
unchanged and becomes the only place a file is born. Because `_save_session`
populates `_session_cache` as a side effect, the cache entry also arrives lazily;
`request.state.session` holds the same dict object that will be saved, so a
request that modifies the session still persists correctly.

The `is_new_session` local is currently assigned and never read. It should either
gain a use or be deleted rather than carried through the rewrite.

### Idle refresh

A small private helper, called from `dispatch` as the first statement of the
valid-id branch, before the session is loaded. Either order works — nothing on
the load path writes `mtime` — but touching on entry reads as "record this use"
and keeps the helper independent of whether the load was a cache hit:

- `stat` the file; if it does not exist, return without creating it — the touch
  must never resurrect a lazily-absent file.
- If `now - st_mtime` is below the refresh interval (one hour), return.
- Otherwise `os.utime(path)`.

Every failure is caught and logged at debug level. A filesystem that refuses
`utime` must not turn a page load into a 500.

The one-hour interval is a module constant, not a setting. It is an
implementation detail of the reaper's semantics, and exposing it would invite
configurations where it exceeds `session_max_age` and reintroduces the bug it
exists to prevent.

### Startup sweep

A module-level function in the same module, so the id pattern that guards it
lives next to the pattern that validates cookies:

```python
def reap_expired_sessions(session_dir: str, max_age: int) -> int:
```

It iterates the directory, skips anything whose name fails `_SESSION_ID_RE`,
and unlinks entries whose `mtime` is older than `max_age`, returning the count
removed. Per-entry failures are logged and skipped so one undeletable file cannot
abort the sweep, and a failure to read the directory at all is logged and
returns 0.

Skipping non-matching names is deliberate: the directory is shared with whatever
else may land there, and a reaper that deletes by age alone is a reaper that
eventually deletes something it did not create.

It is exported from `back.objects.session` alongside `FileSessionMiddleware` and
`get_session`, and called from the lifespan in `src/shared/fastapi/main.py`
directly after the existing `os.makedirs(settings.session_dir, exist_ok=True)`
on line 106, wrapped so a sweep failure logs a warning and startup continues.
The count is logged at info level, since on a first deploy after this change it
is the only evidence the backlog was cleared.

Being a free function taking a directory and an age — rather than a method
reaching for `self.session_dir` — keeps it testable without constructing an app
and a middleware, and keeps the lifespan from having to find the middleware
instance.

Running at startup also sidesteps a coherence problem that a periodic sweep would
have to solve: the reaper deletes files but knows nothing about `_session_cache`,
so a mid-flight sweep could unlink a file whose data is still cached in memory
and would be silently resurrected by the next request. At startup the cache is
empty by construction. Anyone revisiting decision 5 needs to handle this.

## Impact on existing behaviour

For normal use nothing visible changes. Existing cookies stay valid, no stored
session is rewritten, and no migration is needed.

| Behaviour | Before | After |
|---|---|---|
| Request with no cookie | mints id, writes `{}` | mints id, writes nothing |
| N concurrent requests, cookie's file missing | N ids, N files, N−1 orphaned | all keep the cookie's id, 0 or 1 file |
| Cookie present, file missing | server replaces the id | server keeps the id |
| Cookie `../../some/file` | used as a path, overwritten on modify | rejected, fresh id minted |
| File older than `session_max_age` | kept forever | deleted at next startup |
| `mtime` semantics | last modification | last use |

Two consumers are affected and both degrade gracefully.
`Mapping.save_mappings_to_session` (`Mapping.py:1136`) reads the session file
directly and already warns and falls back to the in-memory `session_ref` when it
is absent; with lazy creation that warning becomes more frequent for young
sessions, and the fallback keeps the result correct. `health.py::_check_session_dir`
measures free space on the directory and is unaffected beyond seeing more of it.
A full grep for `session_dir` confirms these are the only two production readers
outside the middleware itself.

One existing test encodes the vulnerability as expected behaviour:
`tests/units/core/test_middleware_session.py::test_get_session_id_from_cookie`
asserts that a cookie of `"abc123"` is returned as the session id. It must be
repointed at a valid 32-hex id, with the rejection of `"abc123"` becoming its own
test.

## Testing

All in `tests/units/core/test_middleware_session.py`, extending the existing
file.

**Id validation.** A real `uuid4().hex` is accepted. Rejection is asserted as
`is None` over a `pytest.mark.parametrize` list with an explicit `id` per case,
so each failure is reported and named individually: `../../etc/passwd`, a bare
`..`, a leading-dot name, an uppercase-hex id, a 31-character id, a
33-character id, a 32-character string containing a non-hex letter, a valid id
with a trailing newline, and the empty string.

Give every case its own `id=`. An unnamed parametrize list reports failures as
`[bad3]`, which tells a reviewer nothing about which rule broke.

**Lazy creation.** A cookie-less request through `TestClient` leaves the session
directory empty while still setting the cookie. A request that sets
`session_modified` produces exactly one file, named for the id in the cookie. A
run of requests that each arrive without a cookie leaves a file count of zero,
which is the regression test for the 82,200 files — and it has to defeat
`TestClient`'s cookie jar, which otherwise replays the cookie from the first
response and makes the test assert nothing. Clear the jar between requests, or
use a fresh client per request.

**Id stability.** A valid cookie whose file is absent comes back with the *same*
id in the response cookie and an empty session dict — the assertion that the
mint-on-missing-file branch is gone. A valid cookie whose file is present still
loads its contents.

**Idle refresh.** A file with an `mtime` set well past the interval is touched.
A file with a fresh `mtime` is not, asserted by comparing the exact `st_mtime`
before and after. A valid id with no file on disk is not created by the touch.
A `utime` that raises does not fail the request.

**Reaper.** An old, valid-named file is deleted; a fresh one survives; a
non-matching name is left alone even when old. Use two shapes for that last one,
since they guard different mistakes: an obviously unrelated name such as
`notes.txt`, and a near-miss 31-hex name that a sloppier pattern would match.
Traversal strings cannot be used here — they are rejected at the cookie, and a
file cannot be *named* `../../etc/passwd` inside the directory. A file that
cannot be unlinked does not stop the others from being reaped, and the returned
count reflects what was actually removed. A non-existent directory returns 0
rather than raising.

Both the touch threshold and the reaper cutoff must be exercised on each side of
the boundary. A test that only checks the "delete" side passes against a reaper
that deletes everything.

## Out of scope

The `_session_cache` dictionary still grows without bound for the process
lifetime; that is a separate fix. The session cookie remains unsigned, so this
work reduces the blast radius of a forged cookie to "guess another user's random
122-bit id" without eliminating forgery as a category. No periodic sweep is
added, so a
deployment that never restarts never reclaims disk from sessions that did
legitimately expire.
