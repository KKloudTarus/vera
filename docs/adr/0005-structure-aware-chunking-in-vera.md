# ADR-0005: Structure-aware chunking lives in VERA, not Graphiti

Status: accepted (Phase 1 schema, Phase 2 algorithm)

## Context

Retrieval needs citable, retrieval-sized passages with exact source coordinates. Graphiti
does not solve document chunking, and delegating it there would put a projection in charge of
authoritative citation coordinates.

## Decision

VERA owns chunking. A `Chunk` is a citable piece of an `artifact_version` with a deterministic
`chunk_key`, ordinal, heading path, character offsets, page number, and, for code, symbol
name and line range. The chunk text is stored in Postgres so the passage index is a
rebuildable projection (ADR-0003).

Chunking strategy (implemented in Phase 2):

- Markdown and Confluence: split by headings and blocks, preserving the heading path.
- Prose: split on sentence boundaries into bounded token windows with small overlaps.
- Code: split by symbol or AST unit where a parser is available, else bounded windows.

Deterministic parsers run before any LLM extraction; LLM extraction is used only where a
parser cannot provide the required semantics.

## Consequences

- Every retrieved passage carries exact provenance and can be cited.
- The `chunk_key` makes re-chunking an unchanged artifact version a no-op.
- Chunk text duplicated between Postgres and S3 raw bytes; acceptable because Postgres text is
  the citation source and the index is rebuilt from it.
