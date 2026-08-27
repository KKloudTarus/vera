"""Test configuration.

Sets the minimum environment the settings object requires so unit tests can import
the app and handlers without a running database. The DSN is a valid string; the
async engine is created lazily and never connects during unit tests.
"""

from __future__ import annotations

import os

os.environ.setdefault("VERA_DB__DSN", "postgresql+asyncpg://vera:vera@localhost:5432/vera")
os.environ.setdefault("VERA_ENVIRONMENT", "local")
os.environ.setdefault("VERA_LOG_JSON", "false")
# Force the null memory engine for unit tests, so importing the app never builds a
# Graphiti client even when .env sets provider=graphiti. (An env var beats .env.)
os.environ["VERA_MEMORY__PROVIDER"] = "null"
