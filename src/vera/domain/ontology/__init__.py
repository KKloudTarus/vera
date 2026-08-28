"""The VERA domain ontology: typed entities and edges, and pipeline versioning."""

from vera.domain.ontology.descriptor import (
    OntologyDescriptor,
    current_descriptor,
    descriptor_from_row,
    detect_drift,
    diff_descriptors,
    governed_predicates,
)
from vera.domain.ontology.registry import (
    EDGE_TYPE_MAP,
    EDGE_TYPES,
    ENTITY_TYPES,
    ONTOLOGY_NAME,
    ONTOLOGY_VERSION,
    SINGLE_VALUED_PREDICATES,
    edge_type_names,
    entity_type_names,
    is_edge_predicate,
    is_single_valued,
)
from vera.domain.ontology.versions import CURRENT_PIPELINE_VERSIONS, PipelineVersions

__all__ = [
    "CURRENT_PIPELINE_VERSIONS",
    "EDGE_TYPES",
    "EDGE_TYPE_MAP",
    "ENTITY_TYPES",
    "ONTOLOGY_NAME",
    "ONTOLOGY_VERSION",
    "SINGLE_VALUED_PREDICATES",
    "OntologyDescriptor",
    "PipelineVersions",
    "current_descriptor",
    "descriptor_from_row",
    "detect_drift",
    "diff_descriptors",
    "edge_type_names",
    "entity_type_names",
    "governed_predicates",
    "is_edge_predicate",
    "is_single_valued",
]
