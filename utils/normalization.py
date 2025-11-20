import re
from typing import Optional

import pandas as pd

from domain.models.event_participant import DocType
from domain.models.participant import Gender

_BOOL_MAP = {
    "yes": True,
    "no": False,
    "true": True,
    "false": False,
}


def normalize_string(value: Optional[str]) -> str:
    """Normalize whitespace and coerce ``None`` to an empty string."""

    return re.sub(r"\s+", " ", (value or "").strip())


def as_str_or_empty(obj: object) -> str:
    """Fast, null-safe conversion to stripped string."""

    return str(obj).strip() if obj is not None else ""


def parse_bool_value(value: object) -> Optional[bool]:
    """Accept boolean-like inputs; otherwise return ``None``."""

    if isinstance(value, bool):
        return value
    s = as_str_or_empty(value).lower()
    return _BOOL_MAP.get(s)


def normalize_gender(value) -> Optional[Gender]:
    """Normalize diverse gender labels into the ``Gender`` enum."""

    if isinstance(value, Gender):
        return value
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    normalized = text.lower().rstrip(".")
    if normalized in {"m", "male", "man", "mr"}:
        return Gender.male
    if normalized in {"f", "female", "woman", "ms", "mrs"}:
        return Gender.female

    return None


def normalize_doc_type_label(value: object) -> str:
    """Return 'Passport' only if value == 'Passport'; otherwise 'ID Card'."""

    if value == "Passport":
        return str(DocType.passport.value)
    return str(DocType.id_card.value)
