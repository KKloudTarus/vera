# syntax=docker/dockerfile:1

FROM pgvector/pgvector:pg18@sha256:2ba9ca5f2e7daa0f0e7723cba1ee9167bab54efd3640516a44ac1a928dd67e7a

COPY deploy/recovery/postgres-harness.sh /usr/local/bin/postgres-recovery-harness

ENTRYPOINT ["/bin/sh", "/usr/local/bin/postgres-recovery-harness"]
