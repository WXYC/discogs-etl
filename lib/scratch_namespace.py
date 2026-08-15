"""Per-invocation namespacing for the rebuild pipeline's scratch tables.

``scripts/dedup_releases.py`` and ``scripts/verify_cache.py``'s ``--prune``
path both build working state in unqualified, globally-named UNLOGGED
tables in the shared ``public`` schema of the shared discogs-cache
database: ``dedup_delete_ids``, ``keep_release_ids``, ``_keep_ids``, and the
``new_release*`` copy-and-swap family. Two overlapping invocations against
the same database collide on these names -- the exact mechanism behind the
2026-08-04 incident that destroyed 27,163 releases (WXYC/discogs-etl#352,
#353). This module gives every scratch table a per-invocation suffix so
concurrent invocations can no longer observe or destroy each other's
working set.

Scope note (WXYC/discogs-etl#356): this is a per-invocation suffix, not a
per-rebuild-run schema. ``dedup_releases.py`` and ``verify_cache.py`` are
independent ``subprocess`` calls that each mint their own ``run_id`` for
logging (see ``docs/observability.md``) -- nothing threads a parent
process's id into them, and wiring that through would mean adding a
``--run-id`` flag to both scripts and every subprocess call site in
``run_pipeline.py``. That's out of scope here: collision-safety only
requires that two invocations never mint the *same* suffix, which a fresh
random suffix per invocation already guarantees. The tradeoff, made
deliberately: a single logical rebuild's dedup step and prune step (and a
manually re-run dedup after a crash) no longer share one debuggable
"pg_stat_activity-greppable" name -- each mints its own.
"""

from __future__ import annotations

import secrets


def new_scratch_suffix() -> str:
    """Return a short random suffix identifying one script invocation.

    Backed by ``secrets.token_hex`` (cryptographically random, not merely
    seeded from PID/wall-clock) so two invocations starting on the same
    host in the same instant still can't collide. 4 bytes = 32 bits of
    entropy -- effectively collision-free for the handful of concurrent
    rebuild invocations this pipeline could ever see, and short enough to
    stay well under PostgreSQL's 63-byte identifier limit once appended to
    the longest base name (``new_release_track_artist`` + 9 chars).

    The result contains only ``[0-9a-f]`` so it is safe to interpolate
    directly into the f-string DDL that :func:`scratch_name` callers build
    -- no SQL-identifier quoting or escaping is performed at any call site
    in ``dedup_releases.py`` / ``verify_cache.py``, matching how those
    modules already treat their other unqualified table names.
    """
    return secrets.token_hex(4)


def scratch_name(base: str, suffix: str) -> str:
    """Build a namespaced scratch table name from a base name and suffix.

    Returns ``base`` unchanged when ``suffix`` is empty, so every existing
    call site that doesn't opt into namespacing (the majority of today's
    unit/integration tests, which construct these functions' plain,
    unsuffixed table names directly) keeps working without modification.
    Production entrypoints (``dedup_releases.main()``,
    ``verify_cache.prune_releases_copy_swap()``) pass a real suffix from
    :func:`new_scratch_suffix`.
    """
    return f"{base}_{suffix}" if suffix else base
