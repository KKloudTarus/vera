"""Claim extractors.

`StructuredClaimExtractor` reads pre-extracted claims or triples from the artifact
metadata, so ingestion is deterministic and testable without an LLM. An LLM-backed
extractor for free text is a later addition; the pipeline depends only on the port.
"""

from __future__ import annotations

from vera.domain.knowledge.models import ClaimType
from vera.domain.ports.curation import ExtractedClaim
from vera.shared.types import JsonDict


class StructuredClaimExtractor:
    async def extract(
        self, *, body: str, knowledge_type: str, metadata: JsonDict
    ) -> list[ExtractedClaim]:
        claims: list[ExtractedClaim] = []
        for raw in metadata.get("claims", []):
            claims.append(
                ExtractedClaim(
                    statement=str(raw["statement"]),
                    claim_type=ClaimType(raw.get("claim_type", ClaimType.FACT.value)),
                    subject=raw.get("subject"),
                    predicate=raw.get("predicate"),
                    object=raw.get("object"),
                    confidence=raw.get("confidence"),
                )
            )
        for triple in metadata.get("triples", []):
            subject, predicate, obj = triple["subject"], triple["predicate"], triple["object"]
            claims.append(
                ExtractedClaim(
                    statement=f"{subject} {predicate} {obj}",
                    claim_type=ClaimType.FACT,
                    subject=subject,
                    predicate=predicate,
                    object=obj,
                )
            )
        return claims
