import os
import sys
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Resolve the database URL from the environment, matching the cache-builder
# CLI convention documented in CLAUDE.md:
#   1. DATABASE_URL_DISCOGS  (canonical)
#   2. DATABASE_URL          (deprecated fallback; emits a warning)
_db_url = os.environ.get("DATABASE_URL_DISCOGS")
if not _db_url:
    _db_url = os.environ.get("DATABASE_URL")
    if _db_url:
        print(
            "warning: DATABASE_URL is deprecated for discogs-etl; set DATABASE_URL_DISCOGS instead",
            file=sys.stderr,
        )
if not _db_url:
    raise RuntimeError(
        "DATABASE_URL_DISCOGS (or DATABASE_URL) must be set to run alembic "
        "migrations against the discogs cache."
    )

# discogs-etl uses psycopg (psycopg3); SQLAlchemy's default postgresql:// URL
# resolves to psycopg2, which isn't a runtime dep. Force the psycopg driver.
if _db_url.startswith("postgresql://"):
    _db_url = "postgresql+psycopg://" + _db_url[len("postgresql://") :]
elif _db_url.startswith("postgres://"):
    _db_url = "postgresql+psycopg://" + _db_url[len("postgres://") :]
config.set_main_option("sqlalchemy.url", _db_url)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
#
# discogs-etl uses hand-written migrations that op.execute() the
# canonical schema/*.sql files; --autogenerate is intentionally not wired up.
target_metadata = None

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
