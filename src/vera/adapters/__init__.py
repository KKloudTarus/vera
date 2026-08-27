"""Adapters: infrastructure implementations of the domain ports.

The only layer that may import SQLAlchemy, Graphiti, boto3, etc. Each adapter maps
between infrastructure types and VERA domain types at its boundary.
"""
