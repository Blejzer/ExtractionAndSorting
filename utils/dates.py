"""Utility helpers for working with date-like values."""

from __future__ import annotations

import math
import re
from datetime import date as date_cls
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

try:  # Optional dependency; pandas is available in the app but tests should not hard fail.
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - pandas import guard
    pd = None  # type: ignore

MONTHS: Dict[str, int] = {
    "JANUARY": 1,
    "FEBRUARY": 2,
    "MARCH": 3,
    "APRIL": 4,
    "MAY": 5,
    "JUNE": 6,
    "JULY": 7,
    "AUGUST": 8,
    "SEPTEMBER": 9,
    "OCTOBER": 10,
    "NOVEMBER": 11,
    "DECEMBER": 12,
}

_EXCEL_EPOCH = datetime(1899, 12, 30)
_GHOST_DATES = {date_cls(1900, 1, 1)}  # Excel "empty" date
_DATE_PATTERNS = {
    "ymd": re.compile(r"^\d{4}-\d{1,2}-\d{1,2}$"),
    "dmy": re.compile(r"^\d{1,2}\.\d{1,2}\.\d{4}$"),
    "mdy": re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$"),
}


def _is_excel_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_missing_value(value: object) -> bool:
    if value is None:
        return True
    if pd is not None:  # pragma: no branch - pandas optional dependency guard
        try:
            if pd.isna(value):  # type: ignore[attr-defined]
                return True
        except Exception:  # pragma: no cover - defensive guard for odd pandas objects
            return False
    return False


def _parse_excel_number(value: object) -> Optional[datetime]:
    """Convert an Excel serial number (days since 1899-12-30) to datetime."""

    if not _is_excel_number(value):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or number <= 0:
        return None
    try:
        return _EXCEL_EPOCH + timedelta(days=number)
    except OverflowError:
        return None


def _parse_date_string(value: str) -> Optional[datetime]:
    text = value.strip()
    if not text:
        return None
    try:
        if _DATE_PATTERNS["ymd"].match(text):
            return datetime.strptime(text, "%Y-%m-%d")
        if _DATE_PATTERNS["dmy"].match(text):
            return datetime.strptime(text, "%d.%m.%Y")
        if _DATE_PATTERNS["mdy"].match(text):
            return datetime.strptime(text, "%m/%d/%Y")
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _coerce_datetime(value: object) -> Optional[datetime]:
    """Best-effort coercion of value into ``datetime`` without timezone handling."""

    if _is_missing_value(value):
        return None

    if isinstance(value, datetime):
        return value

    if pd is not None and isinstance(value, pd.Timestamp):  # type: ignore[attr-defined]
        return value.to_pydatetime()

    if isinstance(value, date_cls):
        return datetime(value.year, value.month, value.day)

    excel_dt = _parse_excel_number(value)
    if excel_dt:
        return excel_dt

    if isinstance(value, str):
        return _parse_date_string(value)

    return None


def normalize_dob(value: object, *, strict: bool = False) -> Optional[datetime]:
    """
    Normalize DOB inputs to naive ``datetime`` objects truncated to midnight.

    Supports strings, datetime/date objects, pandas ``Timestamp`` values, and
    Excel serial numbers. Ghost dates (e.g., 1900-01-01) return ``None``. Any
    timezone information is stripped by converting to UTC before truncation.
    """

    dt = _coerce_datetime(value)
    if dt is None:
        if strict and isinstance(value, str) and value.strip():
            raise ValueError("invalid dob format")
        return None

    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)

    normalized = datetime(dt.year, dt.month, dt.day)
    if normalized.date() in _GHOST_DATES:
        return None

    return normalized


__all__ = ["MONTHS", "normalize_dob"]
