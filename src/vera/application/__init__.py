"""Application layer: use-case handlers orchestrating domain and ports.

Commands mutate (and return ids/receipts); queries read (and never mutate). Handlers
depend on ports only, so they run unchanged whether wired by FastAPI, the worker,
or a test with fakes. Pure: no SQLAlchemy/Graphiti/FastAPI imports (import-linter).
"""
