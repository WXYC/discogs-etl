# Testing

The repo follows architecture A from [the wiki test-patterns doc](https://github.com/WXYC/wiki/blob/main/plans/test-patterns.md): markers route CI by infrastructure, not by tier. Directory layout (`tests/unit/`, `tests/integration/`, `tests/e2e/`) documents the tier; markers describe operational requirements only.

Declared markers:

| Marker | Meaning | When to use |
|---|---|---|
| `pg` | needs a PostgreSQL service (`DATABASE_URL_TEST`) | every test that connects to Postgres, regardless of tier |
| `slow` | takes longer than ~10s (orthogonal to infra) | perf benchmarks; opt-out from CI sync-check, run manually |

Tests with no marker are the default pytest run (no infrastructure required).

```bash
# Default run: no-marker tests only (pure-logic unit tests + in-memory SQLite + library.db fixture tests)
pytest

# PG-backed tests (integration + E2E that touch Postgres)
docker compose up db -d
DATABASE_URL_TEST=postgresql://discogs:discogs@localhost:5433/postgres \
  pytest -m pg -v

# Everything that is not slow (PG-backed plus default tier in one run, for coverage)
DATABASE_URL_TEST=postgresql://discogs:discogs@localhost:5433/postgres \
  pytest -m "pg or not slow" -v

# Perf benchmarks (rare, manual; opted-out from the marker sync-check)
pytest -m slow -v
```

CI runs four jobs per push/PR: `lint`, `test` (default no-marker run, unit dir only), `pg` (`-m "pg or not slow"` against PostgreSQL with `--cov-fail-under=60` -- the PG-and-default combined run is what hits the gate), and `marker-sync` (the reusable sync-check workflow from wxyc-etl that catches markers silently deselected by addopts).

Test fixtures are in `tests/fixtures/` (CSV files, library.db, library_artists.txt). Regenerate with `python tests/fixtures/create_fixtures.py`.
