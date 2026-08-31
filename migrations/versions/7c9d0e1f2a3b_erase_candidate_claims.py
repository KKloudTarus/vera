"""erasure completeness: also delete candidate_claims verbatim source text

`erase_artifact_retrieval_inputs` (from 1c3d4e5f6a7b) removes evidence, chunks, snapshots, and
context packs, but never `candidate_claims`, which stores the verbatim `source_quote` plus the
extracted subject/predicate/object. `candidate_claims` only cascades from `artifact_versions`,
which erasure never deletes, so a data-subject erasure completed "successfully" while the
person's quoted text survived. This replaces the function to also delete the target versions'
`candidate_claims`, and grants the erasure role the privilege to do so.

Revision ID: 7c9d0e1f2a3b
Revises: 6b8c9d0e1f2a
Create Date: 2026-08-31
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "7c9d0e1f2a3b"
down_revision: str | None = "6b8c9d0e1f2a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ERASURE_ROLE = "vera_erasure"

# The function body, parameterized by the extra deletes appended before RETURN. Keeping the two
# variants in one template guarantees upgrade and downgrade differ only by that one statement.
_FUNCTION = """
CREATE OR REPLACE FUNCTION erase_artifact_retrieval_inputs(
    p_group_id text,
    p_artifact_version_ids uuid[]
) RETURNS uuid[] AS $$
DECLARE
    removed_snapshot_ids uuid[];
BEGIN
    IF p_group_id IS NULL
       OR p_group_id IS DISTINCT FROM current_setting('vera.group_id', true) THEN
        RAISE EXCEPTION 'erasure tenant mismatch' USING ERRCODE = '42501';
    END IF;
    PERFORM set_config('vera.erasure_mode', 'on', true);

    SELECT COALESCE(array_agg(DISTINCT doomed.snapshot_id), ARRAY[]::uuid[])
      INTO removed_snapshot_ids
      FROM (
          SELECT sc.snapshot_id
            FROM snapshot_chunks sc
           WHERE sc.group_id = p_group_id
             AND sc.artifact_version_id = ANY(p_artifact_version_ids)
          UNION
          SELECT sfc.snapshot_id
            FROM snapshot_fact_citations sfc
           WHERE sfc.group_id = p_group_id
              AND sfc.artifact_version_id = ANY(p_artifact_version_ids)
          UNION
          SELECT sfs.snapshot_id
            FROM snapshot_fact_sources sfs
           WHERE sfs.group_id = p_group_id
             AND sfs.artifact_version_id = ANY(p_artifact_version_ids)
      ) doomed;

    DELETE FROM context_packs cp
     WHERE cp.group_id = p_group_id
       AND (
           cp.snapshot_id = ANY(removed_snapshot_ids)
           OR EXISTS (
               SELECT 1
                 FROM jsonb_array_elements(cp.results) result
                WHERE result->'citation'->>'artifact_version_id' IN (
                          SELECT version_id::text
                            FROM unnest(p_artifact_version_ids) version_id
                      )
                   OR EXISTS (
                       SELECT 1
                         FROM jsonb_array_elements(
                             COALESCE(result->'citations', '[]'::jsonb)
                         ) citation
                        WHERE citation->>'artifact_version_id' IN (
                            SELECT version_id::text
                              FROM unnest(p_artifact_version_ids) version_id
                        )
                   )
           )
       );
    DELETE FROM knowledge_snapshots snapshot
     WHERE snapshot.group_id = p_group_id
       AND snapshot.id = ANY(removed_snapshot_ids);
    DELETE FROM evidence e
     WHERE e.group_id = p_group_id
       AND e.artifact_version_id = ANY(p_artifact_version_ids);
    DELETE FROM chunks chunk
     WHERE chunk.group_id = p_group_id
       AND chunk.artifact_version_id = ANY(p_artifact_version_ids);
{extra_deletes}
    RETURN removed_snapshot_ids;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp
"""

_CANDIDATE_DELETE = """    DELETE FROM candidate_claims cc
     WHERE cc.group_id = p_group_id
       AND cc.artifact_version_id = ANY(p_artifact_version_ids);"""


def upgrade() -> None:
    # CREATE OR REPLACE preserves the function's owner (vera_erasure) and ACL, so the SECURITY
    # DEFINER identity and the PUBLIC revoke from 1c3d4e5f6a7b stay intact.
    op.execute(f"GRANT SELECT, DELETE ON candidate_claims TO {_ERASURE_ROLE}")
    op.execute(_FUNCTION.format(extra_deletes=_CANDIDATE_DELETE))


def downgrade() -> None:
    op.execute(_FUNCTION.format(extra_deletes=""))
    op.execute(f"REVOKE SELECT, DELETE ON candidate_claims FROM {_ERASURE_ROLE}")
