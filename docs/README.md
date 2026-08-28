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
5. [Deployment](deployment.md) - Docker Compose and Kubernetes, plus the operational
   commands.

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

For the architecture, algorithms (ranking, embeddings, scoring), and design rationale, see
the top-level [README](../README.md).

## Prerequisites

- Docker (for the local infrastructure: Postgres, Neo4j, Valkey, MinIO).
- The conda env `vera` (Python 3.11+) for the CLI commands.
- An OpenAI API key **if** you want to extract knowledge from free text (Markdown,
  Confluence, Git commits). Structured sources (CMDB triples) work without one. See
  [loading-knowledge.md](loading-knowledge.md#do-i-need-an-llm).
