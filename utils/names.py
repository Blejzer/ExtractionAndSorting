# utils/names.py
"""Utilities for canonicalizing and formatting participant names."""

from __future__ import annotations

import re
import unicodedata
from typing import Iterator, Tuple

_WHITESPACE_RE = re.compile(r"\s+")


def _normalize_whitespace(value: str | None) -> str:
    """Collapse internal whitespace and trim leading/trailing spaces."""

    return _WHITESPACE_RE.sub(" ", (value or "").strip())


def _canon(name: str) -> str:
    """Return a lowercase, accent-stripped version of *name*."""

    if not name:
        return ""
    nfd = unicodedata.normalize("NFD", name)
    return "".join(ch for ch in nfd if not unicodedata.combining(ch)).lower()


def _name_key(last: str, first_middle: str) -> str:
    """Build canonical key ``last|first middle`` for name-based lookups."""

    return f"{_canon(last)}|{_canon(first_middle)}".strip()


def _split_name_variants(raw: str) -> Iterator[Tuple[str, str, str]]:
    """Yield possible (first, middle, last) tuples for ``raw`` name text."""

    s = _normalize_whitespace(raw)
    if not s:
        return

    if "," in s:
        last_part, first_part = [x.strip() for x in s.split(",", 1)]
        tokens = first_part.split() + last_part.split()
    else:
        tokens = s.split()

    tokens = [_canon(t) for t in tokens]

    if len(tokens) == 1:
        yield tokens[0], "", ""
        return

    max_surname = min(3, len(tokens) - 1)
    for i in range(1, max_surname + 1):
        first_middle = tokens[:-i]
        last_tokens = tokens[-i:]
        first = first_middle[0]
        middle = " ".join(first_middle[1:]) if len(first_middle) > 1 else ""
        last = " ".join(last_tokens)
        yield first, middle, last


def _name_key_from_raw(raw_display: str) -> str:
    """Normalize 'Last, First' or 'First Last' → canonical ``last|first`` key."""

    s = _normalize_whitespace(raw_display)
    if not s:
        return ""
    if "," in s:
        last, first = [x.strip() for x in s.split(",", 1)]
    else:
        parts = s.split()
        last = parts[-1] if len(parts) > 1 else s
        first = " ".join(parts[:-1]) if len(parts) > 1 else ""
    return _name_key(last, first)


def normalize_name(full_name: str) -> str:
    """Normalize a display name to ``First Middle LAST`` form."""

    name = (full_name or "").strip()
    if not name:
        return ""

    if "," in name:
        last_part, first_part = [segment.strip() for segment in name.split(",", 1)]
        name = f"{first_part} {last_part}".strip()

    if name.isupper():
        return name

    parts = name.split()
    if len(parts) == 1:
        return name

    return f"{' '.join(parts[:-1])} {parts[-1].upper()}"


def _to_app_display_name(fullname: str) -> str:
    """Convert ``'First Middle Last'`` to ``'First Middle LAST'`` display form."""

    name = normalize_name(fullname)
    parts = name.split(" ")
    if len(parts) <= 1:
        return name
    return " ".join(parts[:-1]) + " " + parts[-1].upper()


__all__ = [
    "_canon",
    "_name_key",
    "_split_name_variants",
    "_name_key_from_raw",
    "normalize_name",
    "_to_app_display_name",
]