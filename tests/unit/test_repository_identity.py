from __future__ import annotations

import pytest

from vera.domain.repository_identity import canonical_repository_ref


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("https://user:secret@GitHub.com/Org/Repo.git?token=secret#readme", "github.com/Org/Repo"),
        ("git@github.com:Org/Repo.git", "github.com/Org/Repo"),
        ("git@github.com:Org/Repo.git?token=secret#readme", "github.com/Org/Repo"),
        ("user:secret@github.com/Org/Repo.git", None),
        ("ssh://git@github.com:2222/Org/Repo.git", "github.com:2222/Org/Repo"),
        ("github.com:2222/Org/Repo", "github.com:2222/Org/Repo"),
        ("ssh://git@github.com:02222/Org/Repo.git", "github.com:2222/Org/Repo"),
        ("github.com:02222/Org/Repo", "github.com:2222/Org/Repo"),
        ("GitHub.COM/Org/Repo.git", "github.com/Org/Repo"),
        ("https://github.com:65536/Org/Repo.git", None),
        ("github.com:65536/Org/Repo", None),
        ("ssh://git@[2001:db8::1]:2222/Org/Repo.git", "[2001:db8::1]:2222/Org/Repo"),
        ("[2001:db8::1]:2222/Org/Repo", "[2001:db8::1]:2222/Org/Repo"),
        ("org/repo.git", "org/repo"),
        ("https://github.com/Org/./Repo.git", "github.com/Org/Repo"),
        ("org/././repo.git", "org/repo"),
        ("repo?token=a:b#fragment", "repo"),
        ("\tGitHub.COM/Org/././Repo.git?token=secret\n", "github.com/Org/Repo"),
        ("vera", "vera"),
        ("/home/alice/private/repo", None),
        ("~alice/private/repo", None),
        ("C:\\Users\\alice\\repo", None),
        ("C:private\\repo", None),
        ("\\\\server\\share\\repo", None),
        ("org\\repo", None),
        ("file:///home/alice/repo", None),
        ("file:relative/repo", None),
        ("https://[invalid/repo", None),
        ("https:///Org/Repo.git", None),
        ("https://github.com:not-a-port/Org/Repo.git", None),
        ("../repo", None),
        (None, None),
    ],
)
def test_canonical_repository_ref_removes_secrets_and_local_paths(
    raw: str | None, expected: str | None
) -> None:
    assert canonical_repository_ref(raw) == expected
