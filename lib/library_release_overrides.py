"""Read-only helpers for querying LML's ``lml_cache.library_release_override`` table.

That table (owned and bootstrapped by library-metadata-lookup, never migrated
or written to by this repo -- see CLAUDE.md "entity.* schema ownership") pins
WXYC library items to specific Discogs ``release_id``s. Some pinned releases
fall outside the library-artist scope the monthly rebuild filters to, so they
are periodically absent from the discogs-cache's ``release`` table
(discogs-etl#327). This module holds the one query shape both operator tools
built for the one-time backfill (discogs-etl#329) share:

* ``scripts/query_missing_override_release_ids.py`` -- regenerates the
  ids-file fed to ``scripts/seed_cache_from_clone.py``.
* ``scripts/check_override_parity.py`` -- the post-backfill parity gate
  (``missing_from_cache == 0`` for every source).

Sharing the SQL here means the "what's missing" definition can't drift between
the two tools. Both connect to the discogs-cache database (the same one
``DATABASE_URL_DISCOGS`` names) -- ``lml_cache`` and ``public.release`` live
in the same Postgres instance, joined directly, exactly like
``run_pipeline.py::write_keep_release_ids``.
"""

from __future__ import annotations

from dataclasses import dataclass

# Mirrors the query in discogs-etl#327's issue body. ``LEFT JOIN release`` +
# ``r.id IS NULL`` is the authoritative definition of "missing from cache".
OVERRIDE_SUMMARY_SQL = """
    SELECT o.source,
           count(DISTINCT o.discogs_release_id) AS pinned,
           count(DISTINCT o.discogs_release_id) FILTER (WHERE r.id IS NULL) AS missing_from_cache
    FROM lml_cache.library_release_override o
    LEFT JOIN release r ON r.id = o.discogs_release_id
    GROUP BY o.source
    ORDER BY o.source
"""

MISSING_RELEASE_IDS_SQL = """
    SELECT DISTINCT o.discogs_release_id
    FROM lml_cache.library_release_override o
    LEFT JOIN release r ON r.id = o.discogs_release_id
    WHERE r.id IS NULL
      AND o.discogs_release_id IS NOT NULL
    ORDER BY 1
"""
# ``o.discogs_release_id IS NOT NULL`` matters even though ``r.id IS NULL``
# looks sufficient: a NULL pin is unmatched by the LEFT JOIN, so it survives
# ``r.id IS NULL`` and would be emitted as a lone ``NULL``/``None``. That would
# serialize into the ids-file as the literal ``"None"`` and abort
# ``seed_cache_from_clone.py`` at its consuming ``int()`` -- the same NULL that
# ``run_pipeline.py::write_keep_release_ids`` guards against.


@dataclass(frozen=True)
class OverrideSourceSummary:
    """One row of the per-source override/cache-coverage summary.

    Attributes:
        source: ``lml_cache.library_release_override.source`` value (e.g.
            ``alex-l-2026``, ``alex-l-2026-masters-api``).
        pinned: distinct ``discogs_release_id`` count pinned by this source.
        missing_from_cache: subset of ``pinned`` absent from ``release``.
    """

    source: str
    pinned: int
    missing_from_cache: int


def fetch_override_summary(conn) -> list[OverrideSourceSummary]:
    """Per-source pinned/missing counts (discogs-etl#327's measurement query).

    Read-only. Raises whatever psycopg raises if ``lml_cache.library_release_override``
    doesn't exist (e.g. ``UndefinedTable``) -- unlike the pipeline's
    ``write_keep_release_ids``, these are operator-invoked tools run
    specifically because the table is known to hold real pins, so silently
    degrading to an empty result would mask a real misconfiguration rather
    than a legitimate "no LML bootstrap yet" case.
    """
    with conn.cursor() as cur:
        cur.execute(OVERRIDE_SUMMARY_SQL)
        return [
            OverrideSourceSummary(source=row[0], pinned=row[1], missing_from_cache=row[2])
            for row in cur.fetchall()
        ]


def fetch_missing_release_ids(conn) -> list[int]:
    """Distinct ``discogs_release_id``s pinned by the override table but absent
    from ``release``, across every source. Read-only; see ``fetch_override_summary``
    for the missing-table behavior."""
    with conn.cursor() as cur:
        cur.execute(MISSING_RELEASE_IDS_SQL)
        return [row[0] for row in cur.fetchall()]


def format_summary_table(rows: list[OverrideSourceSummary]) -> str:
    """Render the per-source summary as an aligned, human-readable table with
    a totals line. Pure (no DB) so it is unit-testable independent of
    ``fetch_override_summary``."""
    header = f"{'source':<32} {'pinned':>10} {'missing_from_cache':>19}"
    lines = [header, "-" * len(header)]
    total_pinned = 0
    total_missing = 0
    for row in rows:
        total_pinned += row.pinned
        total_missing += row.missing_from_cache
        lines.append(f"{row.source:<32} {row.pinned:>10,} {row.missing_from_cache:>19,}")
    lines.append("-" * len(header))
    lines.append(f"{'TOTAL':<32} {total_pinned:>10,} {total_missing:>19,}")
    return "\n".join(lines)
