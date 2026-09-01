# syntax=docker/dockerfile:1

# One image runs all three processes; they differ only by the command. The API is the
# default; the worker and MCP server override CMD. A builder stage compiles the venv so
# no build tools reach the runtime image, which runs as an unprivileged user.
#
# Image and dependency digests are deliberate release inputs. Bump them explicitly.

FROM python:3.11-slim-bookworm@sha256:0bee7276f83efd4a1ee05bbbf4281d95ed28e079220a9457f25a93e3f1e3c31b AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=30 \
    PIP_RETRIES=5 \
    PIP_CONSTRAINT=/app/constraints.lock

WORKDIR /app
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Runtime extras only (no dev toolchain), so the image stays lean. LICENSE and NOTICE are
# copied because the project metadata references the license file.
COPY pyproject.toml constraints.lock README.md LICENSE NOTICE ./
RUN --mount=type=cache,id=vera-pip,target=/root/.cache/pip \
    pip install pip==26.2 setuptools==84.0.0 wheel==0.48.0

COPY src ./src
RUN --mount=type=cache,id=vera-pip,target=/root/.cache/pip \
    pip install --constraint constraints.lock ".[memory,objectstore,observability,resilience,security,falkordb]"


FROM python:3.11-slim-bookworm@sha256:0bee7276f83efd4a1ee05bbbf4281d95ed28e079220a9457f25a93e3f1e3c31b AS runtime

LABEL org.opencontainers.image.title="vera" \
      org.opencontainers.image.description="Verified Episodic Recall for Agents" \
      org.opencontainers.image.source="https://github.com/KKloudTarus/vera"

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

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
