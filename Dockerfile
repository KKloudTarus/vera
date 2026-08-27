# syntax=docker/dockerfile:1

# One image runs all three processes; they differ only by the command. The API is the
# default; the worker and MCP server override CMD. A builder stage compiles the venv so
# no build tools reach the runtime image, which runs as an unprivileged user.
#
# The base is pinned to a Debian codename (bookworm) rather than the floating `slim` tag,
# so a rebuild is reproducible. Bump it deliberately to pick up a newer Python or distro.

FROM python:3.11-slim-bookworm AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Patch the venv's own pip/setuptools/wheel before installing, so no vulnerable packaging
# tool is baked into the image (e.g. PYSEC-2026-3721 in pip < 26.2).
RUN pip install --upgrade pip setuptools wheel

# Runtime extras only (no dev toolchain), so the image stays lean.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[memory,objectstore,observability,resilience,security]"


FROM python:3.11-slim-bookworm AS runtime

LABEL org.opencontainers.image.title="vera" \
      org.opencontainers.image.description="Verified Episodic Recall for Agents" \
      org.opencontainers.image.source="https://github.com/KKloudTarus/vera"

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Apply outstanding OS security updates, then drop the apt lists to keep the image small.
# No extra packages are installed; the app needs only the Python runtime.
RUN apt-get update \
    && apt-get upgrade -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Unprivileged runtime user.
RUN groupadd --system vera && useradd --system --gid vera --home-dir /app vera

WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY src ./src
COPY migrations ./migrations
COPY alembic.ini ./

USER vera
EXPOSE 8000

# No baked-in HEALTHCHECK: one image runs three different processes (API/worker/MCP), so a
# single command-specific probe would mark the others unhealthy. Orchestration supplies the
# right probe per process (see deploy/k8s), e.g. GET /health/live and /health/ready for API.

# Default: the API. Override the command for the other processes:
#   worker: python -m vera.entrypoints.worker.main
#   mcp:    python -m vera.entrypoints.mcp.main
CMD ["uvicorn", "vera.entrypoints.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
