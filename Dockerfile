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
    "lxml>=4.9.0" \
    "pymysql>=1.0.0"

# Copy application code
COPY scripts/ scripts/
COPY schema/ schema/
COPY lib/ lib/

CMD ["python", "scripts/run_pipeline.py"]
