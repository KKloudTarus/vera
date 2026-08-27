"""Credential secret handling for API keys.

An API key is a high-entropy random secret, so a single SHA-256 is the right hash:
the slow password hashes (bcrypt, argon2) exist to defend low-entropy human
passwords against brute force, which does not apply to a 256-bit random token. The
key is presented as ``<prefix>.<secret>``. The prefix is stored in the clear and is
unique, so a lookup is one indexed read; only the secret half is hashed, and it is
verified with a constant-time compare.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass

_KEY_NAMESPACE = "vera"
_PREFIX_ENTROPY_BYTES = 6
_SECRET_ENTROPY_BYTES = 32


@dataclass(frozen=True, slots=True)
class GeneratedApiKey:
    full_key: str  # shown to the caller exactly once
    key_prefix: str  # stored in the clear, unique, used for lookup
    hashed_secret: str  # stored; the secret half is never persisted


def hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def verify_secret(secret: str, hashed_secret: str) -> bool:
    return hmac.compare_digest(hash_secret(secret), hashed_secret)


def generate_api_key() -> GeneratedApiKey:
    key_prefix = f"{_KEY_NAMESPACE}_{secrets.token_urlsafe(_PREFIX_ENTROPY_BYTES)}"
    secret = secrets.token_urlsafe(_SECRET_ENTROPY_BYTES)
    return GeneratedApiKey(
        full_key=f"{key_prefix}.{secret}",
        key_prefix=key_prefix,
        hashed_secret=hash_secret(secret),
    )


def split_api_key(full_key: str) -> tuple[str, str] | None:
    """Split ``<prefix>.<secret>`` into its parts, or None if malformed."""
    key_prefix, separator, secret = full_key.partition(".")
    if not separator or not key_prefix or not secret:
        return None
    return key_prefix, secret
