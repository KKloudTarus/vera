"""Authentication adapters: API key, OIDC, and a shape-dispatching composite."""

from vera.adapters.identity.apikey import ApiKeyAuthenticator
from vera.adapters.identity.composite import CompositeAuthenticator
from vera.adapters.identity.oidc import OidcAuthenticator, OidcTokenVerifier

__all__ = [
    "ApiKeyAuthenticator",
    "CompositeAuthenticator",
    "OidcAuthenticator",
    "OidcTokenVerifier",
]
