"""Entrypoints: the three deployables. Each owns its composition root and wiring.

* ``api``: FastAPI HTTP surface
* ``worker``: async ingestion worker that consumes the Postgres-native queue
* ``mcp``: stateless MCP server for AI clients
"""
