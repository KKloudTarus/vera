"""Text normalization shared by the app and the database.

`normalize_name` must produce the same result as the SQL generated column on
``entity_aliases.alias_norm`` so an app-side lookup hits the exact-match index.
"""

from __future__ import annotations

import re

_NON_ALNUM = re.compile(r"[^a-zA-Z0-9]+")


def normalize_name(value: str) -> str:
    """Lowercase, replace any run of non-alphanumerics with one space, trim."""
    return _NON_ALNUM.sub(" ", value).strip().lower()
