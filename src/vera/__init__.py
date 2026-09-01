"""VERA: Verified Episodic Recall for Agents.

A shared, verified agent-memory platform. VERA owns trust, tenancy, provenance,
curation and lifecycle; the memory/knowledge-graph engine (Graphiti) is reached
only through the ``MemoryEngine`` port.

Architecture (imports point inward only, enforced by import-linter):

    entrypoints  ->  adapters  ->  application  ->  domain

``shared``, ``config`` and ``observability`` are cross-cutting and may be used
by any layer.
"""

__version__ = "0.2.1"
