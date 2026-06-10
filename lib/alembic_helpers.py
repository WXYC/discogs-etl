"""Shared helpers for autocommit side-channel migrations.

Migrations that need to issue DDL via a direct ``psycopg.connect(...,
autocommit=True)`` side-channel — rather than through alembic's
``op.execute`` path — all need the same two pieces of plumbing:

* Resolve the cache DB URL from the canonical env vars
  (``DATABASE_URL_DISCOGS`` with ``DATABASE_URL`` as deprecated fallback,
  matching ``alembic/env.py``).
* Refuse to run under ``alembic upgrade --sql`` (offline mode), which
  cannot intercept the side-channel connection and would otherwise emit
  no-op SQL while the migration's DDL silently does not land.

Used by 0010 (``release.not_found``), 0011 (``artist.not_found``), 0012
(``entity.release_identity`` family), and any future migration following
the same pattern.
"""

from __future__ import annotations

import os


def resolve_db_url(revision_label: str) -> str:
    """Return the discogs-cache DB URL or raise with an actionable message.

    ``revision_label`` is the migration's revision id, used in the error
    message so operators see which migration is missing the env var.

    Does NOT import alembic — callable from any context (unit tests, CLI
    scripts, etc.) without a live MigrationContext.
    """
    db_url = os.environ.get("DATABASE_URL_DISCOGS") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            f"DATABASE_URL_DISCOGS (or DATABASE_URL) must be set to apply {revision_label}."
        )
    return db_url


def refuse_offline(revision_label: str, direction: str) -> None:
    """Abort if the migration is being applied in offline (``--sql``) mode.

    Side-channel migrations open their own psycopg connection to apply DDL.
    Alembic's offline mode emits the migration body as SQL text without
    actually opening a connection, so the side-channel ``psycopg.connect``
    would never run — the operator would see a successful ``alembic
    upgrade --sql`` while the DDL never landed on the target DB.

    ``alembic.context`` is imported lazily so callers that only need
    ``resolve_db_url`` do not take a hard dependency on alembic.
    """
    from alembic import context

    if context.is_offline_mode():
        raise RuntimeError(
            f"{revision_label} does not support --sql / offline mode "
            f"({direction}): the migration opens its own psycopg connection "
            "to apply DDL. Run `alembic upgrade head` (or `downgrade`) "
            "against a live DB instead."
        )
