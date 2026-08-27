"""The VERA domain ontology: entity and edge types for infra and engineering.

These are Pydantic models Graphiti uses to steer extraction: an entity type's fields
are the attributes the LLM fills in, an edge type's fields describe a relationship. The
docstrings are part of the prompt, so keep them accurate and concrete. Changing this
file is an ontology change and must be paired with an ontology-version bump.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

# --------------------------------------------------------------- entity types ---


class Service(BaseModel):
    """A deployable software service or application (e.g. paymentapi, checkout)."""

    tier: str | None = Field(default=None, description="Criticality tier, e.g. tier-1")
    language: str | None = Field(default=None, description="Primary implementation language")


class Environment(BaseModel):
    """A deployment environment such as prod, staging, or a named cluster."""

    provider: str | None = Field(default=None, description="Cloud or platform provider")
    region: str | None = Field(default=None, description="Region or location")


class Team(BaseModel):
    """A team or squad that owns services, repositories, or decisions."""

    contact: str | None = Field(default=None, description="Contact channel or email")


class Person(BaseModel):
    """An individual engineer, reviewer, or decision maker."""

    role: str | None = Field(default=None, description="Role or title")


class Repository(BaseModel):
    """A source-code repository."""

    url: str | None = Field(default=None, description="Clone or web URL")


class Datastore(BaseModel):
    """A database, cache, queue, or other stateful store (e.g. postgres, valkey)."""

    engine: str | None = Field(default=None, description="Engine, e.g. postgres, valkey")


class Component(BaseModel):
    """A library, module, or sub-component that is not independently deployable."""


class Incident(BaseModel):
    """An operational incident or outage."""

    severity: str | None = Field(default=None, description="Severity, e.g. sev1")
    status: str | None = Field(default=None, description="Open, mitigated, resolved")


class Decision(BaseModel):
    """An engineering or architectural decision (an ADR-like record)."""

    status: str | None = Field(default=None, description="Proposed, accepted, superseded")


# ----------------------------------------------------------------- edge types ---


class RunsOn(BaseModel):
    """A service runs on an environment."""


class DependsOn(BaseModel):
    """A service or component depends on another service, component, or datastore."""

    kind: str | None = Field(default=None, description="Nature of the dependency")


class Owns(BaseModel):
    """A team or person owns a service, repository, or decision."""


class DeployedTo(BaseModel):
    """A service is deployed to an environment."""


class MemberOf(BaseModel):
    """A person is a member of a team."""


class Caused(BaseModel):
    """A change, service, or component caused an incident."""


class DecidedBy(BaseModel):
    """A decision was made by a team or person."""
