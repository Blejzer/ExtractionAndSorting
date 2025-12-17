from __future__ import annotations

from typing import Optional

from utils.helpers import _normalize, _parse_bool_value
from utils.participants import _normalize_gender, _normalize_doc_type_label

__all__ = [
    "_normalize",
    "_parse_bool_value",
    "_normalize_gender",
    "_normalize_doc_type_label",
]


def normalize_string(value: Optional[str]) -> str:
    """Alias for ``_normalize`` to aid discoverability."""

    return _normalize(value)


def normalize_bool(value: object) -> Optional[bool]:
    """Alias for ``_parse_bool_value`` to aid discoverability."""

    return _parse_bool_value(value)
