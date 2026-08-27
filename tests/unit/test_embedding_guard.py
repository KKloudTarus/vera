"""The embedding fingerprint guard keeps one dimension per group."""

from __future__ import annotations

import pytest

from vera.application.ingestion.embedding_guard import EmbeddingFingerprint, reconcile
from vera.entrypoints.reprocess import RebuildReport
from vera.shared.errors import VeraError

_SMALL = EmbeddingFingerprint(model="text-embedding-3-small", dim=1536)


def test_fresh_group_initializes() -> None:
    assert reconcile(None, _SMALL) == "initialize"


def test_matching_fingerprint_is_ok() -> None:
    assert reconcile(EmbeddingFingerprint(model="text-embedding-3-small", dim=1536), _SMALL) == "ok"


def test_changed_dimension_is_refused() -> None:
    with pytest.raises(VeraError, match="reprocess"):
        reconcile(EmbeddingFingerprint(model="text-embedding-3-small", dim=1024), _SMALL)


def test_changed_model_is_refused() -> None:
    with pytest.raises(VeraError, match="reprocess"):
        reconcile(EmbeddingFingerprint(model="other-model", dim=1536), _SMALL)


def test_rebuild_report_ok_semantics() -> None:
    assert RebuildReport(episodes=0, nodes=0, edges=0).ok  # nothing to rebuild
    assert RebuildReport(episodes=3, nodes=4, edges=5).ok  # graph repopulated
    assert not RebuildReport(episodes=3, nodes=0, edges=0).ok  # facts but empty graph
