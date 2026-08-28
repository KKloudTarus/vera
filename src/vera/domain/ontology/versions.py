"""Pipeline versions stamped on every published episode.

Recording the parser, normalizer, extractor, ontology, prompt, and model versions makes
each episode reproducible and lets a reprocess target exactly what changed. Bump the
relevant field when a stage's behavior changes.
"""

from __future__ import annotations

from dataclasses import dataclass

from vera.domain.ontology.registry import ONTOLOGY_VERSION


@dataclass(frozen=True, slots=True)
class PipelineVersions:
    ontology: int = ONTOLOGY_VERSION
    parser: str = "1"
    normalizer: str = "1"
    extractor: str = "2"
    prompt: str = "2"
    model: str = "gpt-4.1-mini"

    def as_dict(self) -> dict[str, str]:
        return {
            "ontology": str(self.ontology),
            "parser": self.parser,
            "normalizer": self.normalizer,
            "extractor": self.extractor,
            "prompt": self.prompt,
            "model": self.model,
        }


CURRENT_PIPELINE_VERSIONS = PipelineVersions()
