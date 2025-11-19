# ----------------- helpers.py (or top of import_service.py) -----------------
from datetime import datetime
import re

import pandas as pd

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - circular import avoidance
    from domain.models.participant import Gender


def as_dt(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return None
    if isinstance(v, pd.Timestamp): return v.to_pydatetime().replace(tzinfo=None)
    if isinstance(v, datetime): return v.replace(tzinfo=None)
    try: return datetime(v.year, v.month, v.day)  # date -> datetime
    except Exception: return None

def empty_to_none(v):
    if v is None: return None
    if isinstance(v, float) and pd.isna(v): return None
    s = str(v).strip()
    return s or None

def _norm_tablename(name: str) -> str:
    """Normalize an Excel table name to a lowercase alphanumeric key."""

    return re.sub(r"[^0-9a-zA-Z]+", "", (name or "")).lower()

def parse_enum_safe(enum_cls, value, default):
    try:
        return enum_cls(value) if value not in (None, "") else default
    except Exception:
        return default

def _normalize_gender(value):
    from domain.models.participant import Gender  # local import avoids circular

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

def ensure_country(countries_col, country_lookup: dict, name: str) -> str:
    key = (name or "").strip().lower()
    if not key:
        return "c000"
    cid = country_lookup.get(key)
    if cid: return cid
    new_cid = f'c{len(country_lookup)+1:03d}'
    countries_col.insert_one({"cid": new_cid, "country": name})
    country_lookup[key] = new_cid
    return new_cid

def generate_pid(n: int) -> str:
    return f"P{n:04d}"

