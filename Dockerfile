# syntax=docker/dockerfile:1.7
#
# potato_etl — canonical container image (features = all).
#
# Driver install patterns mirror ../itdev-env-installer/scripts/system/*.sh —
# same versions, same paths, same install steps. Bump the versions/SHA below
# whenever that installer bumps theirs.
#
# Two-stage build:
#   1. builder  — Rust toolchain + unixODBC headers; compiles the CLI with
#                 every feature enabled.
#   2. runtime  — debian:trixie-slim with every vendor driver `features=all`
#                 can dlopen:
#                   • Microsoft ODBC 18 + bcp     (mssql-odbc, mssql-bcp)
#                   • Oracle Instant Client 23    (oracle)
#                   • Databricks ODBC driver      (databricks-odbc)
#
# Build (from the repo root):
#   docker build -t potato_etl:latest .
#
# Run:
#   docker run --rm -v "$PWD/pipelines:/pipelines" potato_etl:latest pipeline.yml
#
# The container takes ONE positional arg: the pipeline filename, resolved
# relative to /pipelines (the WORKDIR).

ARG RUST_VERSION=1
ARG DEBIAN_RELEASE=trixie
ARG DEBIAN_MAJOR=13

# Vendor driver pins — keep aligned with itdev-env-installer/scripts/settings.sh.
ARG ORACLE_MAJOR=23
ARG ORACLE_RPM_URL=https://download.oracle.com/otn_software/linux/instantclient/2326100/oracle-instantclient-basic-23.26.1.0.0-1.el9.x86_64.rpm
ARG DATABRICKS_ODBC_VERSION=2.11.0
ARG DATABRICKS_ODBC_DEB_SHA256=5a7823444cd1578a912c95e0355ff24223075c93f58f78c93bd74d66698e0d77

# ──────────────────────────────────────────────────────────────────────────────
# Stage 1 — builder
# ──────────────────────────────────────────────────────────────────────────────
FROM rust:${RUST_VERSION}-slim-${DEBIAN_RELEASE} AS builder

# Build deps:
#   unixodbc-dev → arrow-odbc / odbc-api headers
#   gcc/g++/make → ODPI-C (bundled in the `oracle` crate) + generic C deps
#   libssl-dev   → openssl-sys (some transports link native OpenSSL)
#   pkg-config   → finds the above
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates \
        gcc \
        g++ \
        make \
        pkg-config \
        unixodbc-dev \
        libssl-dev \
        libsmbclient-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

# BuildKit cache mounts keep the cargo registry + target dir warm across
# rebuilds — first build is slow, subsequent ones are quick.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build \
        --release \
        --locked \
        -p potato-etl-cli \
        --no-default-features \
        --features all \
 && cp target/release/potato_etl /usr/local/bin/potato_etl \
 && strip /usr/local/bin/potato_etl

# ──────────────────────────────────────────────────────────────────────────────
# Stage 2 — runtime
# ──────────────────────────────────────────────────────────────────────────────
FROM debian:${DEBIAN_RELEASE}-slim AS runtime

ARG DEBIAN_MAJOR
ARG ORACLE_MAJOR
ARG ORACLE_RPM_URL
ARG DATABRICKS_ODBC_VERSION
ARG DATABRICKS_ODBC_DEB_SHA256

ENV DEBIAN_FRONTEND=noninteractive \
    ACCEPT_EULA=Y \
    ORACLE_HOME=/usr/lib/oracle/23/client64 \
    LD_LIBRARY_PATH=/usr/lib/oracle/23/client64/lib:/opt/databricks/databricksodbc/lib/64 \
    PATH=/opt/mssql-tools18/bin:/usr/lib/oracle/23/client64/bin:$PATH

# Base runtime libs + the libaio.so.1 compatibility symlink Oracle still
# expects (Debian's 64-bit time_t transition renamed the package to
# libaio1t64; the symlink may already exist via Provides:, `ln -sf` is safe).
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
        unzip \
        alien \
        unixodbc \
        libaio1t64 \
        libssl3 \
        libsmbclient0 \
        libgssapi-krb5-2 \
 && ln -sf /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1 \
 && rm -rf /var/lib/apt/lists/*

# ── Microsoft SQL Server: ODBC 18 + BCP via packages-microsoft-prod ──────────
RUN curl -fsSL "https://packages.microsoft.com/config/debian/${DEBIAN_MAJOR}/packages-microsoft-prod.deb" \
        -o /tmp/ms-prod.deb \
 && apt-get install -y --no-install-recommends /tmp/ms-prod.deb \
 && rm /tmp/ms-prod.deb \
 && apt-get update \
 && apt-get install -y --no-install-recommends msodbcsql18 mssql-tools18 \
 && rm -rf /var/lib/apt/lists/*

# ── Oracle Instant Client (RPM via alien) ────────────────────────────────────
ADD ${ORACLE_RPM_URL} /tmp/oracle.rpm
RUN cd /tmp && alien -i /tmp/oracle.rpm \
 && rm /tmp/oracle.rpm

# ── Databricks ODBC driver (Simba) ───────────────────────────────────────────
# `apt install ./file.deb` (not `dpkg -i`) so libsasl2-modules-gssapi-mit
# auto-resolves — the Databricks .deb depends on it.
RUN curl -fsSL "https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simba-databricks-odbc-drivers/${DATABRICKS_ODBC_VERSION}/DatabricksODBC-${DATABRICKS_ODBC_VERSION}-Debian-64bit.zip" \
        -o /tmp/dbx.zip \
 && echo "${DATABRICKS_ODBC_DEB_SHA256}  /tmp/dbx.zip" | sha256sum -c - \
 && unzip -q /tmp/dbx.zip -d /tmp/dbx \
 && apt-get update \
 && apt-get install -y --no-install-recommends /tmp/dbx/databricksodbc_*_amd64.deb \
 && cat /opt/databricks/databricksodbc/Setup/odbcinst.ini >> /etc/odbcinst.ini \
 && rm -rf /tmp/dbx /tmp/dbx.zip /var/lib/apt/lists/*

# Drop tooling we only needed for installs.
RUN apt-get purge -y --auto-remove curl gnupg unzip alien \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/potato_etl /usr/local/bin/potato_etl

# Non-root runtime.
RUN useradd -m -u 10001 -s /usr/sbin/nologin etl
USER etl

WORKDIR /pipelines

ENTRYPOINT ["/usr/local/bin/potato_etl", "run", "--config"]
CMD ["pipeline.yml"]
