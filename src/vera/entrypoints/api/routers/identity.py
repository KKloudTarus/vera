"""Identity and tenancy endpoints: the admin surface for access control.

Self-service signup (POST /identity/register) returns an API key when
``api.registration_open`` is on; a closed deployment turns it off and an admin hands out
access instead (POST /identity/users). Everything else needs an authenticated principal,
and workspace-scoped actions check the caller's role. VERA assigns every group_id, so no
endpoint accepts one.
"""

from __future__ import annotations

from typing import Literal, TypeVar
from uuid import UUID

from fastapi import APIRouter, HTTPException, Response, status
from pydantic import BaseModel, Field

from vera.adapters.mcp.auth import issue_mcp_jwt
from vera.domain.identity.models import Role
from vera.entrypoints.api.deps import (
    ContainerDep,
    IdentityServiceDep,
    PrincipalDep,
    ScopesDep,
)
from vera.shared.errors import Conflict, DomainError, Err, Forbidden, NotFound, Result

router = APIRouter(prefix="/identity", tags=["identity"])

_T = TypeVar("_T")


def _unwrap(result: Result[_T, DomainError]) -> _T:
    if isinstance(result, Err):
        raise _http_error(result.error)
    return result.value


def _http_error(error: DomainError) -> HTTPException:
    mapping: dict[type[DomainError], int] = {
        Forbidden: status.HTTP_403_FORBIDDEN,
        NotFound: status.HTTP_404_NOT_FOUND,
        Conflict: status.HTTP_409_CONFLICT,
    }
    code = mapping.get(type(error), status.HTTP_400_BAD_REQUEST)
    return HTTPException(status_code=code, detail=error.message)


# --------------------------------------------------------------------- schemas ---


class RegisterRequest(BaseModel):
    display_name: str = Field(min_length=1, max_length=256)
    email: str | None = Field(default=None, max_length=320)


class RegisteredOut(BaseModel):
    principal_id: str
    personal_group_id: str
    api_key: str  # shown once


class OrgRequest(BaseModel):
    name: str = Field(min_length=1, max_length=256)
    slug: str = Field(min_length=1, max_length=128)


class OrgOut(BaseModel):
    id: str
    slug: str
    group_id: str


class WorkspaceRequest(BaseModel):
    org_id: UUID
    name: str = Field(min_length=1, max_length=256)
    slug: str = Field(min_length=1, max_length=128)


class WorkspaceOut(BaseModel):
    id: str
    org_id: str
    group_id: str


class ProjectRequest(BaseModel):
    workspace_id: UUID
    name: str = Field(min_length=1, max_length=256)
    slug: str = Field(min_length=1, max_length=128)


class ProjectOut(BaseModel):
    id: str
    workspace_id: str
    group_id: str


class PrincipalRequest(BaseModel):
    display_name: str = Field(min_length=1, max_length=256)
    email: str | None = Field(default=None, max_length=320)


class PrincipalOut(BaseModel):
    id: str
    kind: str
    display_name: str
    personal_group_id: str


class MembershipRequest(BaseModel):
    workspace_id: UUID
    principal_id: UUID
    role: Role
    project_id: UUID | None = None


class MembershipOut(BaseModel):
    id: str
    principal_id: str
    workspace_id: str
    project_id: str | None
    role: str


class ServiceAccountRequest(BaseModel):
    workspace_id: UUID
    name: str = Field(min_length=1, max_length=256)
    description: str | None = Field(default=None, max_length=512)


class ServiceAccountOut(BaseModel):
    id: str
    workspace_id: str
    name: str
    api_key: str  # shown once


class ProvisionUserRequest(BaseModel):
    workspace_id: UUID
    display_name: str = Field(min_length=1, max_length=256)
    email: str | None = Field(default=None, max_length=320)
    role: Role = Role.MEMBER


class ProvisionedUserOut(BaseModel):
    principal_id: str
    workspace_id: str
    role: str
    api_key: str  # shown once


class ApiKeyOut(BaseModel):
    credential_id: str
    principal_id: str
    api_key: str  # shown once


class MeOut(BaseModel):
    principal_id: str
    kind: str
    display_name: str
    personal_group_id: str
    group_ids: list[str]


class McpTokenOut(BaseModel):
    access_token: str
    token_type: Literal["Bearer"] = "Bearer"  # noqa: S105 - OAuth token type, not a secret
    expires_in: int
    scope: str


class McpTokenRequest(BaseModel):
    scopes: list[str] | None = Field(default=None, min_length=1, max_length=16)


# ---------------------------------------------------------------------- routes ---


@router.post("/register", response_model=RegisteredOut, status_code=status.HTTP_201_CREATED)
async def register(
    req: RegisterRequest, identity: IdentityServiceDep, container: ContainerDep
) -> RegisteredOut:
    if not container.settings.api.registration_open:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="self-service registration is closed; ask an admin to provision an account",
        )
    principal, issued = await identity.register(display_name=req.display_name, email=req.email)
    return RegisteredOut(
        principal_id=str(principal.id),
        personal_group_id=principal.personal_group_id,
        api_key=issued.api_key,
    )


@router.get("/me", response_model=MeOut)
async def me(principal: PrincipalDep, scopes: ScopesDep) -> MeOut:
    group_ids = await scopes.allowed_group_ids(principal.id)
    return MeOut(
        principal_id=str(principal.id),
        kind=principal.kind.value,
        display_name=principal.display_name,
        personal_group_id=principal.personal_group_id,
        group_ids=list(group_ids),
    )


@router.post(
    "/mcp-token",
    response_model=McpTokenOut,
    summary="Issue a short-lived MCP JWT for the authenticated principal",
)
async def issue_mcp_token(
    response: Response,
    principal: PrincipalDep,
    container: ContainerDep,
    req: McpTokenRequest | None = None,
) -> McpTokenOut:
    settings = container.settings.mcp
    if settings.jwt_secret is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="MCP token issuance is not configured",
        )
    supported_scopes = {
        settings.scope_read,
        settings.scope_propose,
        settings.scope_feedback,
        settings.scope_snapshot,
        *settings.required_scopes,
    }
    scopes = list(
        dict.fromkeys(
            req.scopes if req and req.scopes else [settings.scope_read, *settings.required_scopes]
        )
    )
    if not set(scopes).issubset(supported_scopes) or not set(settings.required_scopes).issubset(
        scopes
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="requested MCP scopes are unsupported or omit a required scope",
        )
    token = issue_mcp_jwt(
        principal_id=principal.id,
        secret=settings.jwt_secret.get_secret_value(),
        algorithm=settings.jwt_algorithm,
        issuer=settings.auth_issuer,
        audience=settings.auth_audience,
        scopes=scopes,
        ttl_seconds=settings.token_ttl_seconds,
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return McpTokenOut(
        access_token=token,
        expires_in=settings.token_ttl_seconds,
        scope=" ".join(scopes),
    )


@router.post("/orgs", response_model=OrgOut, status_code=status.HTTP_201_CREATED)
async def create_org(
    req: OrgRequest, _principal: PrincipalDep, identity: IdentityServiceDep
) -> OrgOut:
    org = await identity.create_organization(name=req.name, slug=req.slug)
    return OrgOut(id=str(org.id), slug=org.slug, group_id=org.group_id)


@router.post("/workspaces", response_model=WorkspaceOut, status_code=status.HTTP_201_CREATED)
async def create_workspace(
    req: WorkspaceRequest, principal: PrincipalDep, identity: IdentityServiceDep
) -> WorkspaceOut:
    ws = await identity.create_workspace(
        actor=principal, org_id=req.org_id, name=req.name, slug=req.slug
    )
    return WorkspaceOut(id=str(ws.id), org_id=str(ws.org_id), group_id=ws.group_id)


@router.post("/projects", response_model=ProjectOut, status_code=status.HTTP_201_CREATED)
async def create_project(
    req: ProjectRequest, principal: PrincipalDep, identity: IdentityServiceDep
) -> ProjectOut:
    project = _unwrap(
        await identity.create_project(
            actor=principal, workspace_id=req.workspace_id, name=req.name, slug=req.slug
        )
    )
    return ProjectOut(
        id=str(project.id), workspace_id=str(project.workspace_id), group_id=project.group_id
    )


@router.post("/principals", response_model=PrincipalOut, status_code=status.HTTP_201_CREATED)
async def create_principal(
    req: PrincipalRequest, _principal: PrincipalDep, identity: IdentityServiceDep
) -> PrincipalOut:
    created = await identity.create_principal(display_name=req.display_name, email=req.email)
    return PrincipalOut(
        id=str(created.id),
        kind=created.kind.value,
        display_name=created.display_name,
        personal_group_id=created.personal_group_id,
    )


@router.post("/memberships", response_model=MembershipOut, status_code=status.HTTP_201_CREATED)
async def add_member(
    req: MembershipRequest, principal: PrincipalDep, identity: IdentityServiceDep
) -> MembershipOut:
    membership = _unwrap(
        await identity.add_member(
            actor=principal,
            workspace_id=req.workspace_id,
            principal_id=req.principal_id,
            role=req.role,
            project_id=req.project_id,
        )
    )
    return MembershipOut(
        id=str(membership.id),
        principal_id=str(membership.principal_id),
        workspace_id=str(membership.workspace_id),
        project_id=str(membership.project_id) if membership.project_id else None,
        role=membership.role.value,
    )


@router.post(
    "/service-accounts", response_model=ServiceAccountOut, status_code=status.HTTP_201_CREATED
)
async def create_service_account(
    req: ServiceAccountRequest, principal: PrincipalDep, identity: IdentityServiceDep
) -> ServiceAccountOut:
    account, issued = _unwrap(
        await identity.create_service_account(
            actor=principal,
            workspace_id=req.workspace_id,
            name=req.name,
            description=req.description,
        )
    )
    return ServiceAccountOut(
        id=str(account.id),
        workspace_id=str(account.workspace_id),
        name=account.name,
        api_key=issued.api_key,
    )


@router.post(
    "/users",
    response_model=ProvisionedUserOut,
    status_code=status.HTTP_201_CREATED,
    summary="Provision a user in a workspace and issue its first key (workspace admin)",
)
async def provision_user(
    req: ProvisionUserRequest, principal: PrincipalDep, identity: IdentityServiceDep
) -> ProvisionedUserOut:
    created, issued = _unwrap(
        await identity.provision_user(
            actor=principal,
            workspace_id=req.workspace_id,
            display_name=req.display_name,
            email=req.email,
            role=req.role,
        )
    )
    return ProvisionedUserOut(
        principal_id=str(created.id),
        workspace_id=str(req.workspace_id),
        role=req.role.value,
        api_key=issued.api_key,
    )


@router.post("/api-keys", response_model=ApiKeyOut, status_code=status.HTTP_201_CREATED)
async def issue_api_key(principal: PrincipalDep, identity: IdentityServiceDep) -> ApiKeyOut:
    issued = _unwrap(await identity.issue_api_key(actor=principal, principal_id=principal.id))
    return ApiKeyOut(
        credential_id=str(issued.credential_id),
        principal_id=str(issued.principal_id),
        api_key=issued.api_key,
    )


@router.post(
    "/principals/{principal_id}/api-keys",
    response_model=ApiKeyOut,
    status_code=status.HTTP_201_CREATED,
    summary="Issue an API key for another principal (workspace admin)",
)
async def issue_api_key_for(
    principal_id: UUID, principal: PrincipalDep, identity: IdentityServiceDep
) -> ApiKeyOut:
    issued = _unwrap(await identity.issue_api_key(actor=principal, principal_id=principal_id))
    return ApiKeyOut(
        credential_id=str(issued.credential_id),
        principal_id=str(issued.principal_id),
        api_key=issued.api_key,
    )


@router.post(
    "/api-keys/{credential_id}/rotate",
    response_model=ApiKeyOut,
    summary="Revoke a credential and issue a fresh one for the same principal",
)
async def rotate_api_key(
    credential_id: UUID, principal: PrincipalDep, identity: IdentityServiceDep
) -> ApiKeyOut:
    issued = _unwrap(await identity.rotate_api_key(actor=principal, credential_id=credential_id))
    return ApiKeyOut(
        credential_id=str(issued.credential_id),
        principal_id=str(issued.principal_id),
        api_key=issued.api_key,
    )


@router.delete(
    "/api-keys/{credential_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Revoke a credential (self or workspace admin)",
)
async def revoke_api_key(
    credential_id: UUID, principal: PrincipalDep, identity: IdentityServiceDep
) -> None:
    _unwrap(await identity.revoke_credential(actor=principal, credential_id=credential_id))
