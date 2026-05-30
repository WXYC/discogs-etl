# Observability

Every Python entrypoint in `scripts/` initializes the shared logger at the top of `main()` via the local `lib.observability` shim:

```python
from lib.observability import init_logger

init_logger(repo="discogs-etl", tool="discogs-etl <subcommand>")
```

The shim delegates to `wxyc_etl.logger.init_logger` when it's importable, and falls back to a basic stderr `logging.basicConfig` when it isn't. As of `wxyc-etl` 0.1.0 (on PyPI), `wxyc_etl.logger` ships in the published wheel, so JSON logging and Sentry are live by default; the fallback exists so the entrypoints still work in environments where the wheel hasn't been installed.

When wired up, this installs a JSON formatter on the root logger and (when `SENTRY_DSN` is set) hands events to the Sentry SDK. Every log line carries the four contract tags:

| Tag | Source |
|-----|--------|
| `repo` | hard-coded `"discogs-etl"` per call site |
| `tool` | `"discogs-etl <subcommand>"`, e.g. `discogs-etl run_pipeline`, `discogs-etl verify_cache` |
| `step` | per-event, supplied via `logger.info("...", extra={"step": "import"})` |
| `run_id` | UUIDv4 generated at `init_logger` time (one per process) |

`SENTRY_DSN` is read from the environment. When unset, JSON logging still works and Sentry stays inactive — there is no hard requirement on the DSN being configured. Both the `rebuild-cache.yml` and `sync-library.yml` workflows propagate `secrets.SENTRY_DSN` into their pipeline-running steps, so adding the secret to the repo is enough to activate Sentry across both. EC2 / Railway runtime envs are separate operator tasks and not yet wired.

Scripts that initialize the logger (subprocesses each get their own run_id, since they are independent processes): `run_pipeline.py`, `import_csv.py`, `dedup_releases.py`, `verify_cache.py`, `filter_csv.py`, `resolve_collisions.py`, `tsv_to_sqlite.py`, `check_cache_drift.py`. The shim itself lives in `lib/observability.py`.
