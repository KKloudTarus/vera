# syntax=docker/dockerfile:1

# One image runs all three processes; they differ only by the command. The API is the
# default; the worker and MCP server override CMD. A builder stage compiles the venv so
# no build tools reach the runtime image, which runs as an unprivileged user.

FROM python:3.11-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Runtime extras only (no dev toolchain), so the image stays lean.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[memory,objectstore,observability,resilience,security]"


FROM python:3.11-slim AS runtime

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

# Default: the API. Override the command for the other processes:
#   worker: python -m vera.entrypoints.worker.main
#   mcp:    python -m vera.entrypoints.mcp.main
CMD ["uvicorn", "vera.entrypoints.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
