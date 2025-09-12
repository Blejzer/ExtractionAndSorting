# ----------------- helpers.py (or top of import_service.py) -----------------
from datetime import datetime
import pandas as pd

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

def normalize_name(full_name: str) -> str:
    name = (full_name or "").strip()
    if not name: return ""
    if name.isupper(): return name
    parts = name.split()
    if len(parts) == 1: return name
    return f'{" ".join(parts[:-1])} {parts[-1].upper()}'

def _norm_tablename(name: str) -> str:
    """Normalize an Excel table name to a lowercase alphanumeric key."""

    return re.sub(r"[^0-9a-zA-Z]+", "", (name or "")).lower()

def parse_enum_safe(enum_cls, value, default):
    try:
        return enum_cls(value) if value not in (None, "") else default
    except Exception:
        return default

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

def _to_app_display_name(fullname: str) -> str:
    """Convert 'First Middle Last' to 'First Middle LAST' (app display convention)."""

    name = normalize_name(fullname)
    parts = name.split(" ")
    if len(parts) <= 1:
        return name
    return " ".join(parts[:-1]) + " " + parts[-1].upper()