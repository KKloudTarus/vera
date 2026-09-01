"""Text normalization shared by the app and the database.

``normalize_name`` is the single source of truth for ``entity_aliases.alias_norm``: the app
computes it and writes the column, so an app-side lookup hits the exact-match index by
construction. It preserves diacritics, since in languages like Vietnamese an accent changes
meaning (má, ma, and mà are different words), so folding accents would merge distinct names.
"""

from __future__ import annotations

import re
import unicodedata

# Any run of non-word characters (Unicode-aware) plus underscore becomes one separator; this
# keeps Unicode letters and digits, so accented names survive intact.
_SEPARATORS = re.compile(r"[\W_]+")


def normalize_name(value: str) -> str:
    """NFC-compose, collapse separators to single spaces, trim, and lowercase, keeping
    diacritics. ``"Đội nền tảng"`` -> ``"đội nền tảng"`` rather than the old ``"i n n t ng"``.
    """
    composed = unicodedata.normalize("NFC", value)
    return _SEPARATORS.sub(" ", composed).strip().lower()
