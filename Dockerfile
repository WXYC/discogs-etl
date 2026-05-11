FROM rust:1.87 AS rust-builder
WORKDIR /build
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/WXYC/discogs-xml-converter.git . && \
    cargo build --release

FROM python:3.12-slim

# Install postgresql-client for psql (schema creation, VACUUM)
RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-client && \
    rm -rf /var/lib/apt/lists/*

# Copy Rust binary from builder
COPY --from=rust-builder /build/target/release/discogs-xml-converter /usr/local/bin/

WORKDIR /app

# Install Python dependencies
COPY pyproject.toml ./
RUN pip install --no-cache-dir \
    "psycopg[binary]>=3.1.0" \
    "asyncpg>=0.29.0" \
    "rapidfuzz>=3.0.0" \
    "wxyc-etl>=0.1.0" \
    "wxyc-catalog>=0.1.0"

# Copy application code
COPY scripts/ scripts/
COPY schema/ schema/
COPY lib/ lib/

# Alembic migrations + vendored wxyc-etl bytes + pin file. The cross-cache-
# identity migration 0004_wxyc_identity_match_fns reads the canonical SQL
# from `vendor/wxyc-etl/` at apply time (single source of truth, no body
# duplication), so the in-container `alembic upgrade head` path needs both
# trees present.
COPY alembic.ini ./
COPY alembic/ alembic/
COPY vendor/ vendor/
COPY wxyc-etl-pin.txt ./

CMD ["python", "scripts/run_pipeline.py"]
