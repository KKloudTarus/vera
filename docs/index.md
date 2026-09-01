# VERA documentation

Step-by-step guides for running VERA, loading knowledge into it, and using it from an
application or an AI agent.

## Guides

1. [Getting started](getting-started.md) - run the full stack locally and confirm it works.
2. [Loading knowledge](loading-knowledge.md) - the end-to-end flow to feed a repository,
   Markdown files, or Confluence into memory, and how VERA turns them into verified facts.
3. [Using VERA (HTTP API)](usage.md) - search, multi-hop explore, point-in-time queries,
   and retraction, with `curl` examples.
4. [Connecting an AI agent (MCP)](mcp.md) - what the MCP server exposes and how a client
   connects to it.
5. [Agent integration GUIDE](integrations/GUIDE.md) - the versioned, normative contract a
   coding runtime follows to wire VERA into an agent safely, with tested adapters for
   [Claude Code](integrations/adapters/claude-code.md),
   [Cursor](integrations/adapters/cursor.md), and
   [OpenCode](integrations/adapters/opencode.md).
6. [Deployment](deployment.md) - Docker Compose, Kubernetes, and the Helm chart, plus the
   operational commands.
7. [Architecture and algorithms](architecture.md) - how VERA is built and the methods it
   uses: scoring, ranking, embeddings, the temporal model, and more.

## The mental model in one minute

- Knowledge does not go straight into the graph. A **connector** pulls records from a
  **source** (a repo, a Markdown folder, Confluence), an **extractor** turns them into
  `(subject, predicate, object)` claims, and **curation** decides, by the source's **trust
  tier**, whether to publish them.
- A published fact is stored in **PostgreSQL** (the source of truth) and projected into the
  **Neo4j** knowledge graph. It carries provenance: where it came from, how trusted it is,
  when it was true.
- You read memory back through the **HTTP API** or the **MCP server**. Every read is scoped
  to the caller's tenant; every hit carries its provenance.
- Content in **many languages** works: full-text search is language-agnostic and entity names
  keep their diacritics, so Vietnamese (and other non-English) knowledge indexes, matches, and
  deduplicates correctly.

For the architecture, algorithms (ranking, embeddings, scoring), and design rationale, see
[Architecture and algorithms](architecture.md).

## Prerequisites

- Docker (for the local infrastructure: Postgres, Neo4j, Valkey, MinIO).
- The conda env `vera` (Python 3.11+) for the CLI commands.
- An OpenAI API key **if** you want to extract knowledge from free text (Markdown,
  Confluence, Git commits). Structured sources (CMDB triples) work without one. See
  [loading-knowledge.md](loading-knowledge.md#do-i-need-an-llm).
