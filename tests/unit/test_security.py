"""API-key generation, hashing, and parsing."""

from __future__ import annotations

from vera.shared.security import (
    generate_api_key,
    hash_secret,
    split_api_key,
    verify_secret,
)


def test_generated_key_splits_into_stored_prefix_and_secret() -> None:
    generated = generate_api_key()
    parts = split_api_key(generated.full_key)
    assert parts is not None
    key_prefix, secret = parts
    assert key_prefix == generated.key_prefix
    assert key_prefix.startswith("vera_")
    assert verify_secret(secret, generated.hashed_secret)


def test_two_keys_are_distinct() -> None:
    a = generate_api_key()
    b = generate_api_key()
    assert a.full_key != b.full_key
    assert a.key_prefix != b.key_prefix
    assert a.hashed_secret != b.hashed_secret


def test_secret_is_never_the_stored_hash() -> None:
    generated = generate_api_key()
    _, secret = split_api_key(generated.full_key)  # type: ignore[misc]
    assert secret not in generated.hashed_secret
    assert generated.hashed_secret == hash_secret(secret)


def test_wrong_secret_does_not_verify() -> None:
    generated = generate_api_key()
    assert not verify_secret("not-the-secret", generated.hashed_secret)


def test_split_rejects_malformed_keys() -> None:
    assert split_api_key("no-dot-here") is None
    assert split_api_key(".only-secret") is None
    assert split_api_key("only-prefix.") is None
    assert split_api_key("") is None
