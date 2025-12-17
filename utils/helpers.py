from __future__ import annotations

import re
from typing import Optional

from domain.models.event import EventType

_BOOL_MAP = {
    "yes": True,
    "no": False,
    "true": True,
    "false": False,
}


def _normalize(s: Optional[str]) -> str:
    """Normalize whitespace and coerce None to an empty string."""

    return re.sub(r"\s+", " ", (s or "").strip())


def _as_str_or_empty(obj: object) -> str:
    """Fast, null-safe conversion to stripped string."""

    return str(obj).strip() if obj is not None else ""


def _parse_bool_value(value: object) -> Optional[bool]:
    """
    Accept only True/False or case-insensitive Yes/No.
    Everything else returns None.
    """

    if isinstance(value, bool):
        return value
    s = _as_str_or_empty(value).lower()
    return _BOOL_MAP.get(s)


def _coerce_event_type(value: object) -> Optional[EventType]:
    """
    Coerce a raw value to EventType if possible.
    Accepts EventType or case-insensitive string.
    """

    if value is None:
        return None
    if isinstance(value, EventType):
        return value
    s = _as_str_or_empty(value)
    if not s:
        return None
    for variant in (s, s.title(), s.upper()):
        try:
            return EventType(variant)
        except ValueError:
            continue
    return None


def _fill_if_missing(dst: dict, key: str, src: dict, src_key: Optional[str] = None) -> None:
    """Copy src[src_key or key] into dst[key] if missing or falsy."""

    if dst.get(key):
        return
    src_key = src_key or key
    if src.get(src_key):
        dst[key] = src.get(src_key)
