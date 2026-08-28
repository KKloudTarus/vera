"""Typed application settings.

One ``Settings`` object, composed of nested sections, loaded from the environment
(prefix ``VERA_``, nested delimiter ``__``). Required fields with no default make
the process **fail fast** at startup rather than at first use.

Each process (api / worker / mcp) reads the same class but only depends on the
sections it needs; ``extra="ignore"`` lets a single shared ``.env`` serve all
three in local development.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, PostgresDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from vera.shared.ids import deterministic_id

Environment = Literal["local", "dev", "staging", "prod"]


class DatabaseSettings(BaseModel):
    """PostgreSQL, VERA's source of truth."""

    dsn: PostgresDsn
    pool_size: int = 5
    max_overflow: int = 10
    pool_pre_ping: bool = True
    pool_recycle_s: int = 1800
    echo: bool = False


class ObjectStoreSettings(BaseModel):
    """S3-compatible object store for raw artifacts (MinIO, Ceph, or any)."""

    endpoint_url: str | None = None
    bucket: str = "vera-artifacts"
    access_key: SecretStr | None = None
    secret_key: SecretStr | None = None
    region: str = "us-east-1"


class ApiSettings(BaseModel):
    host: str = "0.0.0.0"  # noqa: S104  bind-all is intended inside the container
    port: int = 8000
    cors_origins: list[str] = Field(default_factory=list)
    # Require a valid principal on protected routes. Off by default so local dev and the
    # test suite run without minting credentials; production sets it on.
    auth_required: bool = False
    # OIDC login. Enabled when a signing key is set; without it only API keys authenticate.
    oidc_issuer: str = "https://auth.vera.local"
    oidc_audience: str = "https://api.vera.local"
    oidc_signing_key: SecretStr | None = None
    oidc_jwks_url: str | None = None  # production: fetch the issuer's rotating keys
    oidc_algorithms: list[str] = Field(default_factory=lambda: ["RS256"])


class WorkerSettings(BaseModel):
    lanes: int = 8
    poll_interval_ms: int = 500
    batch_size: int = 20
    visibility_timeout_s: int = 300
    # Warn (and count a metric) when the pending backlog exceeds this; 0 disables it.
    queue_depth_alert_threshold: int = 1000


class McpSettings(BaseModel):
    host: str = "0.0.0.0"  # noqa: S104
    port: int = 8080
    # Auth-disabled local development uses one stable principal with only its personal
    # scope. Override this id to attach the local client to explicit memberships.
    local_principal_id: UUID = deterministic_id("mcp", "local-principal")
    # OAuth 2.1 Resource Server. Auth is enabled when a JWT secret is set; without it
    # the server runs unauthenticated for local development.
    auth_issuer: str = "https://auth.vera.local"
    auth_audience: str = "https://mcp.vera.local"
    jwt_secret: SecretStr | None = None
    jwt_algorithm: str = "HS256"
    required_scopes: list[str] = Field(default_factory=lambda: ["memory:read"])


class ObservabilitySettings(BaseModel):
    """Tracing, metrics, and LLM cost tracking.

    Tracing exports only when an OTLP endpoint is set, so local and test runs create
    spans against a no-op provider with no collector required. Metrics are always
    collected in-process; the worker exposes them on its own port.
    """

    tracing_enabled: bool = True
    otlp_endpoint: str | None = None  # e.g. "http://localhost:4317"; None disables export
    metrics_enabled: bool = True
    worker_metrics_port: int = 9100
    cost_tracking_enabled: bool = True


class ResilienceSettings(BaseModel):
    """Rate limiting, retry, circuit breaking, and timeouts for provider calls.

    The limiter is in-process by default; set ``valkey_url`` to share buckets across
    replicas (needed past ~3). Limits are per provider, sized to stay under its quota.
    """

    valkey_url: str | None = None  # e.g. "redis://localhost:6379/0"; None = in-process
    requests_per_minute: int = 3500
    tokens_per_minute: int = 1_000_000
    retry_attempts: int = 4
    retry_initial_backoff_s: float = 0.5
    retry_max_backoff_s: float = 20.0
    breaker_failure_threshold: int = 5
    breaker_reset_timeout_s: float = 30.0
    per_call_timeout_s: float = 30.0
    per_episode_timeout_s: float = 180.0
    read_timeout_s: float = 10.0


class RerankSettings(BaseModel):
    """Stage-2 rerank blend weights and recency half-life.

    Tunable (not hard-coded) so weights can be calibrated from real feedback. The handler
    normalizes the weights, so they need not sum to exactly 1.
    """

    w_relevance: float = 0.40
    w_authority: float = 0.18
    w_verification: float = 0.12
    w_recency: float = 0.12
    w_feedback: float = 0.08
    w_confidence: float = 0.10
    recency_half_life_days: float = 30.0
    # Calibration applies (persists) new weights only with at least this many labeled
    # feedback samples, so a handful of votes cannot swing ranking.
    min_calibration_samples: int = 20
    # Optional stage-3 cross-encoder over the reranked head. Off by default (adds an LLM
    # call per search); cross_encoder_weight blends its score with the stage-2 blend.
    cross_encoder_enabled: bool = False
    cross_encoder_weight: float = 0.5
    cross_encoder_top_n: int = 20


class ConnectorsSettings(BaseModel):
    """Scheduled source connectors.

    ``specs`` is a list of connector configs the worker syncs on a schedule; each is a
    dict with ``kind``, ``source_id``, ``group_id``, ``interval_s`` and kind-specific
    fields (e.g. ``root`` for filesystem, ``repo_path`` for git, ``base_url``/``token``
    for HTTP connectors). Supply it as JSON via ``VERA_CONNECTORS__SPECS``.
    """

    specs: list[dict[str, object]] = Field(default_factory=lambda: [])


class Neo4jSettings(BaseModel):
    """Graphiti's graph backend."""

    uri: str | None = None
    user: str = "neo4j"
    password: SecretStr | None = None


class MemorySettings(BaseModel):
    """Memory-engine selection and its LLM/embedder wiring.

    Defaults to the null engine so the app runs with no graph or LLM credentials.
    Set provider="graphiti" (plus Neo4j) to use the real engine.
    """

    provider: Literal["null", "graphiti"] = "null"
    embedder: Literal["deterministic", "openai"] = "deterministic"
    embedding_model: str = "text-embedding-3-small"
    embedding_dim: int = 1536
    openai_api_key: SecretStr | None = None
    llm_model: str = "gpt-4.1-mini"
    small_llm_model: str = "gpt-4.1-nano"
    # Semantic (embedding) canonical-entity linking: off by default; enable to merge
    # synonyms/cross-lingual names above the cosine threshold.
    # On by default: an embedding-blocked, LLM-confirmed merge of synonyms and
    # cross-lingual names. It is a no-op unless an embedder is available (graphiti +
    # a key), so it stays inert in offline and unit runs. Run the embedding backfill for
    # pre-existing entities before relying on it, and watch vera_entity_resolution_total.
    semantic_dedup_enabled: bool = True
    semantic_dedup_threshold: float = 0.86
    # Below the auto-link threshold, names this similar become candidates an LLM judge
    # confirms. Kept low, since embedding cosine over bare names is only a coarse blocker.
    semantic_dedup_block_threshold: float = 0.55


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="VERA_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    environment: Environment = "local"
    log_level: str = "INFO"
    log_json: bool = False
    service_name: str = "vera"

    db: DatabaseSettings
    objectstore: ObjectStoreSettings = Field(default_factory=ObjectStoreSettings)
    api: ApiSettings = Field(default_factory=ApiSettings)
    worker: WorkerSettings = Field(default_factory=WorkerSettings)
    mcp: McpSettings = Field(default_factory=McpSettings)
    neo4j: Neo4jSettings = Field(default_factory=Neo4jSettings)
    memory: MemorySettings = Field(default_factory=MemorySettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
    resilience: ResilienceSettings = Field(default_factory=ResilienceSettings)
    connectors: ConnectorsSettings = Field(default_factory=ConnectorsSettings)
    rerank: RerankSettings = Field(default_factory=RerankSettings)

    @property
    def is_prod(self) -> bool:
        return self.environment == "prod"


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings singleton (built once, cached)."""
    return Settings()  # type: ignore[call-arg]  # values come from the environment
