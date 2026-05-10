"""``EXPLAIN ANALYZE`` harness for the v2 ``wxyc_library`` hook.

Implements the **Pre-cutover query plan verification** step from
https://github.com/WXYC/wiki/blob/main/plans/library-hook-canonicalization.md#414-homebrew-discogs-port-5432-full-62-gb-cache--last

> Before flipping ``LML_USE_NEW_HOOK_DISCOGS_FULL=true`` for this cache, run
> ``EXPLAIN ANALYZE`` on the top-5 LML query patterns and confirm:
>
> - Each query uses the new ``wxyc_library`` index it expects (B-tree on
>   ``norm_artist``, GIN on ``norm_artist gin_trgm_ops``, etc.).
> - No query plan regresses to a sequential scan that wasn't a sequential
>   scan before.
> - Query latency at p95 is within 1.5× of the legacy plan.

This script runs ``EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`` on each query,
prints the plan + observed wall time, and exits ``0`` regardless. The
operator interprets the results — automated thresholds belong in CI for
fixture-sized data, not against the live 62 GB Homebrew cache.

Query selection
================

The §4.1.4 spec says the canonical query inventory comes from Sentry traces
captured during the dual-run. Until those traces exist, this harness ships
five representative query patterns derived from the existing LML hot paths
in `library-metadata-lookup`:

1. **Exact ``norm_artist`` lookup** — confirms a candidate artist is in the
   WXYC library. B-tree index on ``norm_artist``. Source pattern:
   `lookup/external_search.py` artist gating.
2. **Trigram ``norm_artist`` fuzzy match** — the mojibake-recovery / fuzzy
   artist path. GIN trigram index on
   ``norm_artist gin_trgm_ops``. Source pattern:
   `discogs/cache_service.py:search_artists_by_name`.
3. **Exact ``norm_title`` lookup** — exact-album hits during the
   canonicalization-driven match. Source pattern:
   `scripts/canonicalize_albums.py` Phase 2a (currently against
   ``wxyc_release_match``; will move to ``wxyc_library`` post-cutover).
4. **Trigram ``norm_title`` fuzzy match** — fuzzy album recovery. GIN
   trigram on ``norm_title gin_trgm_ops``. Source pattern:
   `discogs/cache_service.py:search_releases_by_title`.
5. **Composite ``(norm_artist, norm_title)`` exact hit** — LML's "is this
   exact (artist, album) pair in the WXYC library?" gate. Hits the
   ``norm_artist`` B-tree first, then narrows on ``norm_title``. Source
   pattern: `lookup/orchestrator.py` library-membership check.

Each query is parameterised with a placeholder bound at runtime. Use
``--artist`` / ``--title`` to override the defaults; the defaults are
WXYC-canonical fixture rows so the script works against the Docker dev
cache out of the box.

Usage::

    # Local docker dev cache:
    python scripts/wxyc_library_explain_analyze.py \\
        --database-url postgresql://discogs:discogs@localhost:5433/postgres

    # Full Homebrew (production-shape) cache, JSON output:
    DATABASE_URL_DISCOGS=postgresql://localhost:5432/discogs \\
        python scripts/wxyc_library_explain_analyze.py --json

The plans this script emits are the input to the §4.1.4 cutover gate; this
script does **not** itself enforce the gate. The operator reviews the plans
and the wall times.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.observability import init_logger  # noqa: E402

logger = logging.getLogger(__name__)


# Defaults are pulled from the canonical WXYC example data (see org-level
# CLAUDE.md). They produce non-empty result sets against fixture-loaded
# tests and are also sane against a real cache (Juana Molina is in the prod
# WXYC library; "DOGA" lower-cases trivially).
DEFAULT_ARTIST = "juana molina"
DEFAULT_TITLE = "doga"


@dataclass(frozen=True)
class QueryPattern:
    """One representative LML query against ``wxyc_library``.

    The ``origin`` field documents which LML code path this query represents
    so a future reader can trace the operational claim back to the source.
    """

    name: str
    sql: str
    params: tuple[Any, ...]
    expected_index: str
    origin: str


@dataclass
class ExplainResult:
    """One pattern's plan + measured wall time.

    ``plan`` is the raw Postgres EXPLAIN output (a list with a single dict
    when FORMAT=JSON). ``elapsed_ms`` is the wall-clock time around the
    EXPLAIN ANALYZE call — a coarse upper bound; the planner's reported
    actual time inside ``plan`` is more precise.
    """

    name: str
    expected_index: str
    origin: str
    sql: str
    elapsed_ms: float
    plan: list[dict[str, Any]] | None = None
    error: str | None = None
    summary: dict[str, Any] = field(default_factory=dict)


# Five representative LML query patterns. Each comment block names the
# code path the query represents. Update this list once dual-run Sentry
# traces are available — the mechanism below is generic.
QUERY_PATTERNS: tuple[QueryPattern, ...] = (
    QueryPattern(
        name="exact_norm_artist",
        sql="SELECT library_id, artist_name FROM wxyc_library WHERE norm_artist = %s",
        params=(DEFAULT_ARTIST,),
        expected_index="wxyc_library_norm_artist_idx (B-tree)",
        # library-metadata-lookup/lookup/external_search.py — gates whether
        # a discogs / mb fuzzy hit corresponds to a WXYC library artist.
        origin="lookup/external_search.py — artist gating after fuzzy match",
    ),
    QueryPattern(
        name="trgm_norm_artist",
        sql=(
            "SELECT library_id, artist_name, "
            "similarity(norm_artist, %s) AS score "
            "FROM wxyc_library "
            "WHERE norm_artist %% %s "
            "ORDER BY score DESC LIMIT 5"
        ),
        params=(DEFAULT_ARTIST, DEFAULT_ARTIST),
        expected_index="wxyc_library_norm_artist_trgm_idx (GIN trgm)",
        # library-metadata-lookup/discogs/cache_service.py:search_artists_by_name
        # — fuzzy artist recovery for the mojibake bucket.
        origin="discogs/cache_service.py:search_artists_by_name",
    ),
    QueryPattern(
        name="exact_norm_title",
        sql="SELECT library_id, album_title FROM wxyc_library WHERE norm_title = %s",
        params=(DEFAULT_TITLE,),
        expected_index="wxyc_library_norm_title_idx (B-tree)",
        # library-metadata-lookup/scripts/canonicalize_albums.py — Phase 2a
        # currently runs `WHERE norm_title = lower($1)` against
        # ``wxyc_release_match``; the post-cutover analog is this query.
        origin="scripts/canonicalize_albums.py Phase 2a (post-cutover analog)",
    ),
    QueryPattern(
        name="trgm_norm_title",
        sql=(
            "SELECT library_id, album_title, "
            "similarity(norm_title, %s) AS score "
            "FROM wxyc_library "
            "WHERE norm_title %% %s "
            "ORDER BY score DESC LIMIT 5"
        ),
        params=(DEFAULT_TITLE, DEFAULT_TITLE),
        expected_index="wxyc_library_norm_title_trgm_idx (GIN trgm)",
        # library-metadata-lookup/discogs/cache_service.py:search_releases_by_title
        origin="discogs/cache_service.py:search_releases_by_title",
    ),
    QueryPattern(
        name="composite_artist_title",
        sql=(
            "SELECT library_id, artist_name, album_title FROM wxyc_library "
            "WHERE norm_artist = %s AND norm_title = %s"
        ),
        params=(DEFAULT_ARTIST, DEFAULT_TITLE),
        expected_index=(
            "wxyc_library_norm_artist_idx (B-tree) — narrows on norm_title "
            "after; <=64K rows so this is acceptable without a composite idx"
        ),
        # library-metadata-lookup/lookup/orchestrator.py — "is this exact
        # (artist, album) pair in the WXYC library?" gate.
        origin="lookup/orchestrator.py — library-membership check",
    ),
)


def _summarize_plan(plan: list[dict[str, Any]]) -> dict[str, Any]:
    """Pull a small, readable summary out of EXPLAIN's full JSON tree.

    Returns the top-level node type, the "Actual Total Time" if present,
    and a flag for whether any node in the tree is a sequential scan over
    ``wxyc_library`` (the §4.1.4 regression check).
    """
    if not plan or not isinstance(plan, list):
        return {}
    root = plan[0].get("Plan", {}) if isinstance(plan[0], dict) else {}

    def _walk(node: dict[str, Any]) -> bool:
        if not isinstance(node, dict):
            return False
        if node.get("Node Type") == "Seq Scan" and node.get("Relation Name") == "wxyc_library":
            return True
        for child in node.get("Plans", []) or []:
            if _walk(child):
                return True
        return False

    return {
        "node_type": root.get("Node Type"),
        "actual_total_time_ms": root.get("Actual Total Time"),
        "rows": root.get("Actual Rows"),
        "seq_scan_on_wxyc_library": _walk(root),
    }


def run_explain(
    database_url: str,
    artist: str = DEFAULT_ARTIST,
    title: str = DEFAULT_TITLE,
) -> list[ExplainResult]:
    """Run EXPLAIN ANALYZE for each query pattern and return results.

    Substitutes ``artist`` / ``title`` into the patterns' placeholder slots
    (the order matches each pattern's ``params`` tuple).
    """
    import psycopg

    # Re-key the defaults onto the user-supplied values. ``params`` is a
    # frozen tuple of placeholder values; the patterns use ``DEFAULT_*``
    # in those slots, so we substitute by identity.
    def _substitute(p: tuple[Any, ...]) -> tuple[Any, ...]:
        return tuple(
            artist if v == DEFAULT_ARTIST else (title if v == DEFAULT_TITLE else v) for v in p
        )

    results: list[ExplainResult] = []
    with psycopg.connect(database_url) as conn, conn.cursor() as cur:
        for pat in QUERY_PATTERNS:
            params = _substitute(pat.params)
            explain_sql = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + pat.sql
            t0 = time.perf_counter()
            try:
                cur.execute(explain_sql, params)
                row = cur.fetchone()
                plan = row[0] if row else None
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                summary = _summarize_plan(plan) if plan else {}
                results.append(
                    ExplainResult(
                        name=pat.name,
                        expected_index=pat.expected_index,
                        origin=pat.origin,
                        sql=pat.sql,
                        elapsed_ms=elapsed_ms,
                        plan=plan,
                        summary=summary,
                    )
                )
            except Exception as e:  # pragma: no cover (operational helper)
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                results.append(
                    ExplainResult(
                        name=pat.name,
                        expected_index=pat.expected_index,
                        origin=pat.origin,
                        sql=pat.sql,
                        elapsed_ms=elapsed_ms,
                        error=str(e),
                    )
                )
    return results


def _print_human(results: list[ExplainResult]) -> None:
    for r in results:
        print(f"=== {r.name} ===")
        print(f"  origin:         {r.origin}")
        print(f"  expected index: {r.expected_index}")
        print(f"  wall ms:        {r.elapsed_ms:.2f}")
        if r.error:
            print(f"  ERROR: {r.error}")
            continue
        s = r.summary or {}
        print(f"  plan node_type:           {s.get('node_type')}")
        print(f"  plan actual_total_time:   {s.get('actual_total_time_ms')} ms")
        print(f"  plan rows:                {s.get('rows')}")
        seq_flag = s.get("seq_scan_on_wxyc_library")
        warn = " WARNING: regresses to seq scan" if seq_flag else ""
        print(f"  seq_scan_on_wxyc_library: {seq_flag}{warn}")
        print()


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Run EXPLAIN ANALYZE on the top-5 LML query patterns against the "
            "v2 wxyc_library hook. See the docstring for the §4.1.4 gate."
        ),
    )
    p.add_argument(
        "--database-url",
        default=None,
        help=(
            "PostgreSQL URL for the discogs-cache. Falls back to "
            "DATABASE_URL_DISCOGS, then DATABASE_URL."
        ),
    )
    p.add_argument(
        "--artist",
        default=DEFAULT_ARTIST,
        help=f"Artist (in normalized form) to substitute into the queries. Default: {DEFAULT_ARTIST!r}.",
    )
    p.add_argument(
        "--title",
        default=DEFAULT_TITLE,
        help=f"Album title (in normalized form) to substitute. Default: {DEFAULT_TITLE!r}.",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit the full plan + summary as JSON on stdout.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    init_logger(repo="discogs-etl", tool="discogs-etl wxyc_library_explain_analyze")

    database_url = (
        args.database_url
        or os.environ.get("DATABASE_URL_DISCOGS")
        or os.environ.get("DATABASE_URL")
    )
    if not database_url:
        print(
            "error: --database-url not provided and DATABASE_URL_DISCOGS/DATABASE_URL not set.",
            file=sys.stderr,
        )
        return 2

    try:
        results = run_explain(database_url, artist=args.artist, title=args.title)
    except Exception as e:  # pragma: no cover (live-DB path only)
        logger.exception("explain harness failed")
        print(f"error: explain harness failed: {e}", file=sys.stderr)
        return 3

    if args.json:
        print(json.dumps([asdict(r) for r in results], indent=2, default=str))
    else:
        _print_human(results)

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
