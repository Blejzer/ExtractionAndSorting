from typing import Optional

from utils.normalize_phones import normalize_phone
from utils.normalization import (
    as_str_or_empty,
    normalize_string,
    parse_bool_value,
)


def _normalize(s: Optional[str]) -> str:
    """Normalize whitespace and coerce None to an empty string."""

    return normalize_string(s)


def _as_str_or_empty(obj: object) -> str:
    """Fast, null-safe conversion to stripped string."""

    return as_str_or_empty(obj)


def _parse_bool_value(value: object):
    """Accept only True/False or case-insensitive Yes/No."""

    return parse_bool_value(value)


def _fill_if_missing(dst: dict, key: str, src: dict, src_key: Optional[str] = None) -> None:
    """If ``dst[key]`` is empty, copy ``src[src_key or key]`` if truthy."""

    k = src_key or key
    if dst.get(key):
        return

    value = src.get(k)
    if not value:
        return

    if key == "phone":
        normalized = normalize_phone(value)
        if not normalized:
            return
        dst[key] = normalized
        return

    dst[key] = value
