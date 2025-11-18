# ----------------- helpers.py (or top of import_service.py) -----------------
import os
from datetime import datetime
import re
from functools import lru_cache

import pandas as pd

from typing import TYPE_CHECKING, Optional, Dict

from domain.models.event_participant import DocType
from services.xlsx_tables_inspector import TableRef

if TYPE_CHECKING:  # pragma: no cover - circular import avoidance
    from domain.models.participant import Gender

DEBUG_PRINT = os.getenv("DEBUG_PRINT")

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

def _to_app_display_name(fullname: str) -> str:
    """Convert 'First Middle Last' to 'First Middle LAST' (app display convention)."""

    name = normalize_name(fullname)
    parts = name.split(" ")
    if len(parts) <= 1:
        return name
    return " ".join(parts[:-1]) + " " + parts[-1].upper()

@lru_cache(maxsize=20000)
def _normalize_cached(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def fast_norm(x: object) -> str:
    s = str(x).strip()
    if "  " in s or "\t" in s or "\n" in s:
        return _normalize(s)
    return s

def _normalize(s: Optional[str]) -> str:
    """Normalize whitespace and coerce None to an empty string."""
    return re.sub(r"\s+", " ", (s or "").strip())

def _normalize_doc_type_label(value: object) -> str:
    """Return 'Passport' only if value == 'Passport'; everything else 'ID Card'."""
    if DEBUG_PRINT:
        print(f"[DEBUG] Normalizing doc type label: {value}")

    if value == "Passport":
        return str(DocType.passport.value)
    return str(DocType.id_card.value)

def _date_to_iso(val: object) -> str:
    """Format datetime → 'YYYY-MM-DD' (or '' if not a date)."""
    if isinstance(val, datetime):
        return val.date().isoformat()
    return ""

def _coerce_datetime(value: object) -> Optional[datetime]:
    """Return ``datetime`` when the input resembles a date-like object."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None
    return None


class TableDataCache:
    """Simple helper that stores DataFrames for every Excel table once."""

    def __init__(self) -> None:
        self._by_ref: Dict[TableRef, pd.DataFrame] = {}
        self._by_name: Dict[str, pd.DataFrame] = {}
        self._by_norm: Dict[str, pd.DataFrame] = {}

    def add(self, table: TableRef, df: pd.DataFrame) -> None:
        self._by_ref[table] = df
        self._by_name[table.name] = df
        self._by_norm[table.name_norm] = df

    def get(self, key: object) -> Optional[pd.DataFrame]:
        if isinstance(key, TableRef):
            return self._by_ref.get(key)
        if isinstance(key, str):
            if key in self._by_name:
                return self._by_name[key]
            return self._by_norm.get(_norm_tablename(key))
        return None

    def get_df(self, key: object) -> pd.DataFrame:
        df = self.get(key)
        return df if df is not None else pd.DataFrame()

    def __getitem__(self, key: object) -> pd.DataFrame:
        df = self.get(key)
        if df is None:
            raise KeyError(key)
        return df

