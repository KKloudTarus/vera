# Contributing to VERA

Thanks for your interest in VERA. This guide covers how to set up, make a change, and get
it merged.

## Ground rules

- Read the architecture and principles in the [README](README.md) first. Two rules shape
  almost every file: imports point inward (`entrypoints -> adapters -> application ->
  domain`, enforced by import-linter), and PostgreSQL plus S3 are the source of truth while
  Neo4j is a rebuildable projection.
- No vendor lock-in. Every dependency must be open-source and cloud-portable, reached
  through a port. Infrastructure libraries are imported only in `adapters/`.
- By contributing, you agree that your contributions are licensed under the project's
  [Apache License 2.0](LICENSE).

## Development setup

VERA targets Python 3.11+ and uses a conda environment named `vera`.

```bash
conda activate vera
cp .env.example .env
make install          # editable install with all extras, plus pre-commit
make up               # start postgres, neo4j, valkey, minio (Docker)
make migrate          # apply database migrations
```

## The local gate

Every change must pass the same gate CI runs:

```bash
make check            # ruff (lint + format), pyright (strict), import-linter, unit tests
make test-int         # integration tests (needs the compose stack)
```

Tests are split by marker: unit (default), `integration` (needs the compose stack), and
`llm` (needs a real OpenAI key; excluded from the default gate). Add tests with your change;
new behavior without a test will be asked to add one.

## Making a change

1. Create a branch from `main`.
2. Keep the change focused. Follow the existing style; the writing rules for code comments
   and docs are: explain the why, one point once, plainly, and no em dashes.
3. Run `make check` (and `make test-int` if you touched the data plane) until green.
4. Write a clear commit message: a short subject line, then a body explaining the why.
5. Open a pull request against `main` and fill in the template. Link any related issue.

## Pull request expectations

- CI must be green: lint, types, architecture contracts, unit and integration tests, and
  the dependency audit.
- A change to the data model includes an Alembic migration, and `alembic check` reports no
  drift.
- A change to a public interface (HTTP, MCP, or a port) updates the README where relevant.

## Reporting bugs and requesting features

Use the issue templates under **New issue**. For a security vulnerability, do not open a
public issue; follow [SECURITY.md](SECURITY.md).
