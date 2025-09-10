# services/import_service.py
"""Excel import/validation helpers.

This service locates Excel Tables (ListObjects) across all worksheets and performs:
- Structural validation (required table names must be present)
- Optional preview: event header extraction (Participants!A1/A2), discovered tables, etc.
- Dry-run parsing that prints diagnostic output (no DB writes here)

Conventions
-----------
- Country table names are looked up exactly (e.g., 'tableAlb', 'tableBih', etc.).
- 'ParticipantsLista' must exist for position lookups.
- Titles and dates are read from the 'Participants' worksheet in cells A1 and A2.
"""

from __future__ import annotations

import os
import re
import unicodedata
from typing import Dict, List, Optional, Tuple, Iterator

import pandas as pd
import openpyxl
from openpyxl.utils import range_boundaries
from datetime import datetime, UTC, date

from config.database import mongodb
from services.xlsx_tables_inspector import (
    list_sheets,
    list_tables,
    TableRef,
)
from utils.translation import translate

# ============================
# Configuration / Constants
# ============================

COUNTRY_TABLE_MAP: Dict[str, str] = {
    "tableAlb": "Albania, Europe & Eurasia, World",
    "tableBih": "Bosnia and Herzegovina, Europe & Eurasia, World",
    "tableCro": "Croatia, Europe & Eurasia, World",
    "tableKos": "Kosovo, Europe & Eurasia, World",
    "tableMne": "Montenegro, Europe & Eurasia, World",
    "tableNmk": "North Macedonia, Europe & Eurasia, World",
    "tableSer": "Serbia, Europe & Eurasia, World",
}

MONTHS: Dict[str, int] = {
    "JANUARY": 1, "FEBRUARY": 2, "MARCH": 3, "APRIL": 4, "MAY": 5, "JUNE": 6,
    "JULY": 7, "AUGUST": 8, "SEPTEMBER": 9, "OCTOBER": 10, "NOVEMBER": 11, "DECEMBER": 12
}

DEBUG_PRINT = False  # flip to True for verbose logging and extra debug data

# --- ADD near the top with other constants ---
# We now require the full roster table for enrichment:
REQUIRE_PARTICIPANTS_LIST = True

# --- ADD under existing helpers: name/build-key utilities ---
def _canon(name: str) -> str:
    """Return a lowercase, accent-stripped version of ``name``."""
    if not name:
        return ""
    nfd = unicodedata.normalize("NFD", name)
    return "".join(ch for ch in nfd if not unicodedata.combining(ch)).lower()


def _name_key(last: str, first_middle: str) -> str:
    return f"{_canon(last)}|{_canon(first_middle)}".strip()

def _split_name_variants(raw: str) -> Iterator[tuple[str, str, str]]:
    """Yield (first, middle, last) variants for a raw name string.

    The last 1-3 tokens are treated as possible surnames. Names may also be
    provided as ``LAST, First Middle``; in that case the tokens are reordered
    to ``First Middle LAST`` before generating variants.
    """
    s = _normalize(raw)
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

# --- ADD: lookups for ParticipantsLista and MAIN ONLINE/ParticipantsList ---
def _build_lookup_participantslista(df_positions: pd.DataFrame) -> Dict[str, Dict[str, str]]:
    """
    Key: 'LAST|First Middle'   -> {position, phone, email}
    """
    name_col = next((c for c in df_positions.columns if "name (" in c.lower()), None)
    pos_col = next((c for c in df_positions.columns if "position" in c.lower()), None)
    phone_col = next((c for c in df_positions.columns if "phone" in c.lower()), None)
    email_col = next((c for c in df_positions.columns if "email" in c.lower()), None)
    look: Dict[str, Dict[str, str]] = {}
    if not name_col:
        return look

    for _, r in df_positions.iterrows():
        raw = _normalize(str(r.get(name_col, "")))
        if not raw:
            continue
        if "," in raw:
            last, first = [x.strip() for x in raw.split(",", 1)]
        else:
            parts = raw.split(" ")
            last = parts[-1] if len(parts) > 1 else raw
            first = " ".join(parts[:-1])
        key = _name_key(last, first)
        look[key] = {
            "position": _normalize(str(r.get(pos_col, ""))) if pos_col else "",
            "phone": _normalize(str(r.get(phone_col, ""))) if phone_col else "",
            "email": _normalize(str(r.get(email_col, ""))) if email_col else "",
        }
    return look

def _build_lookup_main_online(df_online: pd.DataFrame) -> Dict[str, Dict[str, object]]:
    """
    MAIN ONLINE → ParticipantsList table (split name columns).
    Key: 'LAST|First Middle' (and we will also try 'LAST|First' as fallback).
    """
    cols = {c.lower().strip(): c for c in df_online.columns}

    def col(label: str) -> Optional[str]:
        return cols.get(label.lower())

    look: Dict[str, Dict[str, object]] = {}
    for _, r in df_online.iterrows():
        first = _normalize(str((r.get(col("Name")) or "")))
        middle = _normalize(str((r.get(col("Middle name")) or "")))
        last = _normalize(str((r.get(col("Last name")) or "")))
        if not first and not last:
            continue
        key = _name_key(last, " ".join([first, middle]).strip())
        full_name = " ".join([first, middle, last]).strip()
        gender = _normalize(str(r.get(col("Gender"), "")))
        if _normalize(str(r.get(col("Gender"), ""))) == "Mr":
            gender = "male"
        elif _normalize(str(r.get(col("Gender"), ""))) == "Mrs":
            gender = "female"
        entry = {
            "name": full_name,
            "gender": gender,
            "dob": r.get(col("Date of Birth (DOB)")),
            "pob": translate(_normalize(str(r.get(col("Place Of Birth (POB)"), ""))), "en"),
            "birth_country": _normalize(str(r.get(col("Country of Birth"), ""))),
            "citizenships": [ _normalize(x) for x in str(r.get(col("Citizenship(s)"), "")).split(",") if _normalize(x) ],
            "email_list": _normalize(str(r.get(col("Email address"), ""))),
            "phone_list": _normalize(str(r.get(col("Phone number"), ""))),
            "travel_doc_type": translate(_normalize(str(r.get(col("Travelling document type"), ""))), "en"),
            "travel_doc_number": _normalize(str(r.get(col("Travelling document number"), ""))),
            "travel_doc_issue": r.get(col("Travelling document issuance date")),
            "travel_doc_expiry": r.get(col("Travelling document expiration date"))
                                 or r.get(col("Travelling document expiry date")),
            "travel_doc_issued_by": translate(_normalize(str(r.get(col("Travelling document issued by"), ""))), "en"),
            "requires_visa_hr": _normalize(str(r.get(col("Do you require Visa to travel to Croatia"), ""))),
            "transportation_declared": _normalize(str(r.get(col("Transportation"), ""))),
            "travelling_from_declared": _normalize(str(r.get(col("Travelling from"), ""))),
            "returning_to": translate(_normalize(str(r.get(col("Returning to"), ""))), "en"),
            "diet_restrictions": translate(_normalize(str(r.get(col("Diet restrictions"), ""))), "en"),
            "organization": translate(_normalize(str(r.get(col("Organization"), ""))), "en"),
            "unit": translate(_normalize(str(r.get(col("Unit"), ""))), "en"),
            "position_online": _normalize(str(r.get(col("Position"), ""))),
            "rank": translate(_normalize(str(r.get(col("Rank"), ""))), "en"),
            "intl_authority": _normalize(str(r.get(col("Authority"), ""))),
            "bio_short": translate(_normalize(str(r.get(col("Short professional biography"), ""))), "en"),
            "bank_name": _normalize(str(r.get(col("Bank name"), ""))),
            "iban": _normalize(str(r.get(col("IBAN"), ""))),
            "iban_type": _normalize(str(r.get(col("IBAN Type"), ""))),
            "swift": _normalize(str(r.get(col("SWIFT"), ""))),
        }
        look[key] = entry
    return look

# --- CHANGE: tighten validation to require 'ParticipantsList' too ---
def validate_excel_file_for_import(path: str) -> tuple[bool, list[str], dict]:
    """
    Validate that:
      - Sheet 'Participants' exists
      - A1 (eid+title) and A2 (dates+location) present (non-empty)
      - Table 'ParticipantsLista' exists (any sheet)
      - Table 'ParticipantsList' (MAIN ONLINE) exists (if REQUIRED_PARTICIPANTS_LIST=True)
      - ≥1 country table exists
    Returns: (ok, missing_list, tables_info_dict)
    """
    missing: list[str] = []

    # 1) A1/A2 on "Participants"
    wb = openpyxl.load_workbook(path, data_only=True)
    if "Participants" not in wb.sheetnames:
        missing.append("Sheet 'Participants'")
        return False, missing, {}
    ws = wb["Participants"]
    a1 = str(ws["A1"].value or "").strip()
    a2 = str(ws["A2"].value or "").strip()
    if not a1:
        missing.append("Participants!A1 (eid + title)")
    if not a2:
        missing.append("Participants!A2 (dates + location)")

    # 2) Tables via inspector
    tables = list_tables(path)
    idx = _index_tables(tables)

    # ParticipantsLista present?
    if not _find_table_exact(idx, "ParticipantsLista"):
        missing.append("Table 'ParticipantsLista'")

    # MAIN ONLINE → ParticipantsList (for enrichment)
    if REQUIRE_PARTICIPANTS_LIST and not _find_table_exact(idx, "ParticipantsList"):
        missing.append("Table 'ParticipantsList' (worksheet 'MAIN ONLINE')")

    # At least one country table present?
    if not any(_find_table_exact(idx, k) for k in COUNTRY_TABLE_MAP.keys()):
        missing.append(
            "At least one country table (tableAlb, tableBih, tableCro, tableKos, tableMne, tableNmk, tableSer)"
        )

    ok = len(missing) == 0

    # For visibility return a compact dict of what we saw
    seen = {t.name_norm: (t.name, t.sheet_title, t.ref) for t in tables}

    if DEBUG_PRINT:
        print("[VALIDATE] OK:", ok)
        if missing:
            print("[VALIDATE] Missing:", missing)
        print("[VALIDATE] Tables seen:", seen)

    return ok, missing, seen

# --- ADD: core finder for country-table columns (robust to minor header variations) ---
def _find_col(df: pd.DataFrame, want: str) -> Optional[str]:
    wl = want.lower()
    for c in df.columns:
        cl = c.lower().strip()
        if wl == "name" and ("name and last name" in cl or ("name" in cl and "last" in cl)):
            return c
        if wl == "transport" and (cl == "travel" or "transport" in cl):
            return c
        if wl == "from" and ("travelling from" in cl or "traveling from" in cl):
            return c
        if wl == "grade" and "grade" in cl:
            return c
    return None

# --- ADD: the new public API for full parse (NO DB WRITES) ---
def parse_for_commit(path: str) -> dict:
    """
    Returns a dict with:
      - event: {eid, title, date_from, date_to, location}
      - attendees: [ {name_display, name,
                      representing_country, transportation, travelling_from, grade,
                      position, phone, email, ...plus MAIN ONLINE fields when present} ]
    The raw attendee records (``initial_attendees``) are collected only when
    ``DEBUG_PRINT`` is enabled and otherwise omitted from the returned payload.
    """
    # 1) Event header
    wb = openpyxl.load_workbook(path, data_only=True)
    if "Participants" not in wb.sheetnames:
        raise RuntimeError("Sheet 'Participants' not found")
    ws = wb["Participants"]
    a1 = ws["A1"].value or ""
    a2 = ws["A2"].value or ""
    year = _filename_year_from_eid(os.path.basename(path))
    eid, title, date_from, date_to, location = _parse_event_header(a1, a2, year)

    if DEBUG_PRINT:
        print("[STEP] Event header:", {"eid": eid, "title": title, "date_from": date_from,
                                      "date_to": date_to, "location": location})

    # 2) Tables + lookups
    tables = list_tables(path)
    idx = _index_tables(tables)

    plist = _find_table_exact(idx, "ParticipantsLista")
    ponl  = _find_table_exact(idx, "ParticipantsList")
    if not plist:
        raise RuntimeError("Required table 'ParticipantsLista' not found")
    if REQUIRE_PARTICIPANTS_LIST and not ponl:
        raise RuntimeError("Required table 'ParticipantsList' (MAIN ONLINE) not found")

    df_positions = _read_table_df(path, plist)
    df_online    = _read_table_df(path, ponl) if ponl else pd.DataFrame()

    positions_lookup = _build_lookup_participantslista(df_positions)
    online_lookup    = _build_lookup_main_online(df_online) if not df_online.empty else {}

    if DEBUG_PRINT:
        print(f"[STEP] Positions lookup entries: {len(positions_lookup)}")
        print(f"[STEP] Online lookup entries: {len(online_lookup)}")

    # 3) Collect attendees from country tables
    attendees: List[dict] = []
    initial_attendees: List[dict] = []
    for key, country_label in COUNTRY_TABLE_MAP.items():
        t = _find_table_exact(idx, key)
        if not t:
            continue
        df = _read_table_df(path, t)
        if df.empty:
            continue

        nm_col    = _find_col(df, "name")
        trans_col = _find_col(df, "transport")
        from_col  = _find_col(df, "from")
        grade_col = _find_col(df, "grade")

        for _, row in df.iterrows():
            raw_name = _normalize(str(row.get(nm_col, ""))) if nm_col else ""
            if not raw_name or raw_name.upper() == "TOTAL":
                continue

            transportation = _normalize(str(row.get(trans_col, ""))) if trans_col else ""
            travelling_from = _normalize(str(row.get(from_col, ""))) if from_col else ""
            grade_val = row.get(grade_col, None)
            grade = None
            if isinstance(grade_val, (int, float)) and not pd.isna(grade_val):
                try:
                    grade = int(grade_val)
                except Exception:
                    pass

            # Key for enrichment
            variants = list(_split_name_variants(raw_name))
            p_list = {}
            p_comp = {}
            for f, m, l in variants:
                key_a = _name_key(l, " ".join([f, m]).strip())
                key_b = _name_key(l, f) if f else None
                cand_list = (online_lookup.get(key_a) or (online_lookup.get(key_b) if key_b else None)) or {}
                cand_comp = (positions_lookup.get(key_a) or (positions_lookup.get(key_b) if key_b else None)) or {}
                if cand_list or cand_comp:
                    p_list, p_comp = cand_list, cand_comp
                    break

            # ✅ always fall back to {} so .get(...) is safe
            # (p_list and p_comp already default to {})

            # Determine name and display from raw country table string
            ordered = _normalize(raw_name)
            if "," in ordered:
                last_part, first_part = [x.strip() for x in ordered.split(",", 1)]
                ordered = f"{first_part} {last_part}".strip()
            base_name = ordered
            name_display = _to_app_display_name(base_name)

            # Compose attendee record
            base_record = {
                "name_display": name_display,
                "name": base_name,
                "representing_country": country_label,
                "transportation": transportation,
                "travelling_from": travelling_from,
                "grade": grade,
            }
            initial_attendees.append(base_record)

            record = {
                **base_record,
                "position": p_comp.get("position") or p_list.get("position_online") or "",
                "phone":    p_comp.get("phone")    or p_list.get("phone_list") or "",
                "email":    p_comp.get("email")    or p_list.get("email_list") or "",
            }

            # add remaining MAIN ONLINE fields when present
            if p_list:
                dob_val = p_list.get("dob")
                if isinstance(dob_val, (datetime, date)):
                    dob_out = dob_val.date().isoformat() if isinstance(dob_val, datetime) else dob_val.isoformat()
                else:
                    dob_out = str(dob_val).strip() if dob_val else ""

                issue_val = p_list.get("travel_doc_issue")
                if isinstance(issue_val, (datetime, date)):
                    issue_out = issue_val.date().isoformat() if isinstance(issue_val, datetime) else issue_val.isoformat()
                else:
                    issue_out = str(issue_val).strip() if issue_val else ""

                expiry_val = p_list.get("travel_doc_expiry")
                if isinstance(expiry_val, (datetime, date)):
                    expiry_out = expiry_val.date().isoformat() if isinstance(expiry_val, datetime) else expiry_val.isoformat()
                else:
                    expiry_out = str(expiry_val).strip() if expiry_val else ""

                record.update({
                    "gender": p_list.get("gender", ""),
                    "dob": dob_out,
                    "pob": p_list.get("pob",""),
                    "birth_country": p_list.get("birth_country",""),
                    "citizenships": p_list.get("citizenships", []),
                    "travel_doc_type": p_list.get("travel_doc_type", ""),
                    "travel_doc_number": p_list.get("travel_doc_number", ""),
                    "travel_doc_issue_date": issue_out,
                    "travel_doc_expiry_date": expiry_out,
                    "travel_doc_issued_by": p_list.get("travel_doc_issued_by", ""),
                    "requires_visa_hr": str(p_list.get("requires_visa_hr", "")).lower() in ("yes", "true", "1"),
                    "returning_to": p_list.get("returning_to", ""),
                    "diet_restrictions": p_list.get("diet_restrictions", ""),
                    "organization": p_list.get("organization", ""),
                    "unit": p_list.get("unit", ""),
                    "rank": p_list.get("rank", ""),
                    "intl_authority": str(p_list.get("intl_authority", "")).lower() in ("yes", "true", "1"),
                    "bio_short": p_list.get("bio_short", ""),
                    "bank_name": p_list.get("bank_name", ""),
                    "iban": p_list.get("iban", ""),
                    "iban_type": p_list.get("iban_type", ""),
                    "swift": p_list.get("swift", ""),
                })

            attendees.append(record)

    if DEBUG_PRINT:
        print("[STEP] Initial participant list:")
        for rec in initial_attendees:
            print("  ", rec)

    payload = {
        "event": {
            "eid": eid,
            "title": title,
            "date_from": date_from,
            "date_to": date_to,
            "location": location,
        },
        "attendees": attendees,
    }

    if DEBUG_PRINT:
        payload["initial_attendees"] = initial_attendees

    return payload


# ============================
# String / Name helpers
# ============================

def _normalize(s: Optional[str]) -> str:
    """Normalize whitespace and coerce None to an empty string."""
    return re.sub(r"\s+", " ", (s or "").strip())

def _norm_tablename(name: str) -> str:
    """Normalize an Excel table name to a lowercase alphanumeric key."""

    return re.sub(r"[^0-9a-zA-Z]+", "", (name or "")).lower()


def _to_app_display_name(fullname: str) -> str:
    """Convert 'First Middle Last' to 'First Middle LAST' (app display convention)."""

    name = _normalize(fullname)
    parts = name.split(" ")
    if len(parts) <= 1:
        return name
    return " ".join(parts[:-1]) + " " + parts[-1].upper()


# ============================
# Workbook / Table helpers
# ============================

def _index_tables(tables: List[TableRef]) -> Dict[str, List[TableRef]]:
    """Group tables by normalized name for quick lookup."""
    idx: Dict[str, List[TableRef]] = {}
    for t in tables:
        idx.setdefault(t.name_norm, []).append(t)
    return idx

def _find_table_any(idx: Dict[str, List[TableRef]], desired: str) -> Optional[TableRef]:
    """
    Find a table whose normalized name matches exactly, or
    whose normalized name starts with the desired normalized value.
    """
    target = _norm_tablename(desired)
    # exact
    if target in idx and idx[target]:
        return idx[target][0]
    # prefix
    for key, group in idx.items():
        if key.startswith(target) and group:
            return group[0]
    return None

def _find_table_exact(idx: Dict[str, List[TableRef]], desired: str) -> Optional[TableRef]:
    """Find first table with an exact normalized name match."""

    target = _norm_tablename(desired)
    group = idx.get(target)
    return group[0] if group else None

def _read_table_df(path: str, table: TableRef) -> pd.DataFrame:
    """
    Read a ListObject range (e.g., 'A4:K7') from the given sheet into a DataFrame.
    Uses the header row in the table as columns.
    """
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb[table.sheet_title]
    min_col, min_row, max_col, max_row = range_boundaries(table.ref)
    rows = list(ws.iter_rows(min_row=min_row, max_row=max_row,
                             min_col=min_col, max_col=max_col, values_only=True))
    if not rows:
        return pd.DataFrame()
    header = [_normalize(str(h)) if h is not None else "" for h in rows[0]]
    return pd.DataFrame(rows[1:], columns=header).dropna(how="all")


# ============================
# Event header parsing (A1, A2)
# ============================

def _filename_year_from_eid(filename: str) -> int:
    r"""
    From 'PFE25M2 - whatever.xlsx' infer 2025 via regex: r'PFE(\d{2})M'.
    Fallback to current UTC year if not found.
    """
    m = re.search(r"PFE(\d{2})M", filename.upper())
    return 2000 + int(m.group(1)) if m else datetime.now(UTC).year

def _parse_event_header(a1: str, a2: str, year: int):
    """
    A1: 'PFE25M2 TITLE OF THE EVENT' -> (eid, title)
    A2: 'JUNE 23 - 27 - Opatija, CROATIA' -> (dateFrom, dateTo, location)
    Month for end date assumed same as start (per your spec).
    """
    a1 = _normalize(a1)
    sp = a1.find(" ")
    eid, title = (a1, "") if sp == -1 else (a1[:sp], a1[sp + 1:])

    a2 = _normalize(a2)
    parts = [p.strip() for p in a2.split(" - ")]
    date_from = date_to = None
    location = ""
    if len(parts) >= 3:
        month_and_start, end_day_str, location = parts[0], parts[1], parts[2]
        m = re.match(r"([A-Z]+)\s+(\d{1,2})", month_and_start.upper())
        if m:
            month_num = MONTHS.get(m.group(1))
            start_day = int(m.group(2))
            if month_num:
                end_day = int(re.sub(r"\D", "", end_day_str))
                date_from = datetime(year, month_num, start_day, tzinfo=UTC)
                date_to = datetime(year, month_num, end_day, tzinfo=UTC)
    return eid, title, date_from, date_to, location


# ============================
# DB helpers (read-only)
# ============================

def _country_id(name: str) -> Optional[str]:
    """Return the country _id for `name`, or None if not found."""
    if not name:
        return None
    try:
        doc = mongodb.collection('countries').find_one({'country': name})
        return doc.get('_id') if doc else None
    except Exception:
        # Avoid raising during preview; treat missing/DB errors as not found
        return None

def _participant_exists(name_display: str, country_name: str):
    """
    Existence check by normalized app name (First ... LAST) + country_id if available.
    """
    q = {"name": _to_app_display_name(name_display)}
    cid = _country_id(country_name)
    if cid is not None:
        q["country_id"] = cid
    try:
        doc = mongodb.collection('participants').find_one(q)
        return (doc is not None), (doc or {})
    except Exception:
        return False, {}


# ============================
# Public API
# ============================

def validate_excel_file_for_import(path: str) -> tuple[bool, list[str], dict]:
    """
    Validate that:
      - Sheet 'Participants' exists
      - A1 (eid+title) and A2 (dates+location) present (non-empty)
      - Table 'ParticipantsLista' exists (any sheet)
      - At least one of the country tables exists (any sheet)
    Returns: (ok, missing_list, tables_info_dict)
    """
    missing: list[str] = []

    # 1) A1/A2 on "Participants"
    wb = openpyxl.load_workbook(path, data_only=True)
    if "Participants" not in wb.sheetnames:
        missing.append("Sheet 'Participants'")
        return False, missing, {}
    ws = wb["Participants"]
    a1 = (ws["A1"].value or "").strip()
    a2 = (ws["A2"].value or "").strip()
    if not a1:
        missing.append("Participants!A1 (eid + title)")
    if not a2:
        missing.append("Participants!A2 (dates + location)")

    # 2) Tables via inspector (cross-sheet, ZIP-safe)
    tables = list_tables(path)
    idx = _index_tables(tables)

    # ParticipantsLista present?
    if not _find_table_exact(idx, "ParticipantsLista"):
        missing.append("Table 'ParticipantsLista'")

    # At least one country table present?
    if not any(_find_table_exact(idx, k) for k in COUNTRY_TABLE_MAP.keys()):
        missing.append("At least one country table (tableAlb, tableBih, tableCro, tableKos, tableMne, tableNmk, tableSer)")

    ok = len(missing) == 0

    # For visibility return a compact dict of what we saw
    seen = {t.name_norm: (t.name, t.sheet_title, t.ref) for t in tables}

    if DEBUG_PRINT:
        print("[VALIDATE] OK:", ok)
        if missing:
            print("[VALIDATE] Missing:", missing)
        print("[VALIDATE] Tables seen:", seen)

    return ok, missing, seen


def inspect_and_preview_uploaded(path: str) -> None:
    """
    NO WRITES.
    - Logs EVENT NEW/EXIST (from A1/A2 on 'Participants' sheet)
    - Logs ATTENDEES from any present country tables
      (marks new with '*', includes grade, country, and position from ParticipantsLista)
    """
    # Event header
    wb = openpyxl.load_workbook(path, data_only=True)
    if "Participants" not in wb.sheetnames:
        raise RuntimeError("Sheet 'Participants' not found")
    ws = wb["Participants"]
    a1 = ws["A1"].value or ""
    a2 = ws["A2"].value or ""
    year = _filename_year_from_eid(os.path.basename(path))
    eid, title, date_from, date_to, location = _parse_event_header(a1, a2, year)

    # Event exist check (read-only)
    existing = mongodb.collection('events').find_one({"eid": eid})
    if existing:
        print(f"[EVENT] EXIST {eid}  title='{existing.get('title','')}' "
              f"dateFrom={existing.get('dateFrom')} location='{existing.get('location','')}'")
    else:
        print(f"[EVENT] NEW   {eid}  title='{title}' dateFrom={date_from} dateTo={date_to} location='{location}'")

    # Tables + positions
    tables = list_tables(path)
    idx = _index_tables(tables)

    plist = _find_table_exact(idx, "ParticipantsLista")
    if not plist:
        raise RuntimeError("Required table 'ParticipantsLista' not found (any sheet)")

    df_positions = _read_table_df(path, plist)
    name_col_pos = next((c for c in df_positions.columns if "name (" in c.lower()), None)
    pos_col = next((c for c in df_positions.columns if "position" in c.lower()), None)

    positions_lookup: Dict[str, str] = {}
    if name_col_pos and pos_col:
        for _, r in df_positions.iterrows():
            raw = _normalize(str(r.get(name_col_pos, "")))
            pos = _normalize(str(r.get(pos_col, "")))
            if not raw:
                continue
            # raw is "LAST, First Middle"
            if "," in raw:
                last, first = [x.strip() for x in raw.split(",", 1)]
            else:
                parts = raw.split(" ")
                last = parts[-1] if len(parts) > 1 else raw
                first = " ".join(parts[:-1])
            key = _name_key(last, first)
            positions_lookup[key] = pos

    print("[ATTENDEES]")

    # Iterate any present country tables
    for key, country_label in COUNTRY_TABLE_MAP.items():
        t = _find_table_exact(idx, key)
        if not t:
            continue

        df = _read_table_df(path, t)
        if df.empty:
            continue

        nm_col = next((c for c in df.columns if "name" in c.lower()), None)
        grade_col = next((c for c in df.columns if "grade" in c.lower()), None)

        for _, row in df.iterrows():
            raw_name = _normalize(str(row.get(nm_col, ""))) if nm_col else ""
            if not raw_name:
                continue
            grade = _normalize(str(row.get(grade_col, ""))) if grade_col else ""

            # Build key to lookup position: "LAST|First Middle"
            parts = raw_name.split(" ")
            if len(parts) > 1:
                first = " ".join(parts[:-1])
                last = parts[-1]
            else:
                first = ""
                last = parts[0]
            key_lookup = _name_key(last, first)
            pos = positions_lookup.get(key_lookup, "")

            exists, doc = _participant_exists(raw_name, country_label)
            norm = _to_app_display_name(raw_name)
            star = "*" if not exists else " "
            pid = doc.get("pid", "NEW")

            print(
                f"{star} {'NEW' if star=='*' else 'EXIST'} {pid:>6}  {norm}  ({grade}, {country_label})  "
                f"{'pos='+pos if pos else ''}"
            )
