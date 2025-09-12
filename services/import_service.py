"""
import_service.py

Unified Excel -> Mongo import pipeline with helpers and zero duplicated logic.
- Single validator: validate_excel_file_for_import
- Clear ImportContext for state (collections, lookups, counters)
- DRY helpers taken from helpers.py
- ParticipantRow (lightweight) for row-level validation (emails/phones optional)
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Tuple, Dict, Any

import pandas as pd
from pydantic import BaseModel, EmailStr, field_validator, ConfigDict

from config.database import mongodb
from domain.models.participant import Grade, Gender  # your enums
from domain.models.participantDTO import ParticipantRow

# ---- helpers (from your helpers.py) ----
from utils.helpers import (
    as_dt,             # pandas.Timestamp|date|datetime -> datetime|None
    empty_to_none,     # "", "   ", NaN -> None
    normalize_name,    # "John doe" -> "John DOE"
    parse_enum_safe,   # parse enum with default fallback
    ensure_country,    # ensure country exists & returns cid
    generate_pid,      # "P0001" style
)


# ------------------------------- Validation -------------------------------

def validate_excel_file_for_import(
    path: str,
    required_sheets: tuple[str, ...] = ("Participant", "Country", "Events"),
    required_columns: dict[str, tuple[str, ...]] | None = None,
):
    """
    LEGACY-COMPATIBLE signature for routes/imports.py:
    Returns (ok: bool, missing: list[str], seen: list[str])
    - 'missing' aggregates missing sheets/columns
    - 'seen' returns the sheet names found
    """
    errors: list[str] = []
    seen: list[str] = []

    try:
        xl = pd.ExcelFile(path)
        seen = list(xl.sheet_names)
    except FileNotFoundError:
        return (False, [f"File not found: {path}"], seen)
    except Exception as exc:
        return (False, [f"Failed to open Excel: {exc}"], seen)

    sheets = set(seen)

    if required_columns is None:
        required_columns = {
            "Participant": ("Name", "Position", "Country", "Gender", "DOB", "POB", "Birth Country", "Email", "Phone", "Event"),
            "Country": ("Country",),
            "Events": ("Event", "Title", "Location", "Date From", "Date To"),
        }

    # Missing required sheets
    for s in required_sheets:
        if s not in sheets:
            errors.append(f"Missing required sheet: {s}")

    # Missing required columns
    for sheet, cols in required_columns.items():
        if sheet not in sheets:
            continue
        df = xl.parse(sheet, nrows=1)
        present = set(df.columns)
        for c in cols:
            if c not in present:
                errors.append(f"Sheet '{sheet}' missing column: '{c}'")

    return len(errors) == 0, errors, seen


# ------------------------------ Import Context ----------------------------

class ImportContext:
    """Holds DB collections, lookups, and counters for a single import run."""
    def __init__(self):
        db = mongodb.db()
        self.events_col = db["events"]
        self.participants_col = db["participants"]
        self.countries_col = db["countries"]
        self.participant_events_col = db["participant_events"]

        self.country_lookup: dict[str, str] = {}  # name.lower() -> cid
        self.event_lookup: dict[str, pd.Timestamp] = {}  # eid -> earliest date for dedup logic
        self.pid_counter: int = 1

    def next_pid(self) -> str:
        pid = generate_pid(self.pid_counter)
        self.pid_counter += 1
        return pid


# -------------------------- Mapping & Builders ----------------------------

def map_participant_row(
    row: pd.Series,
    ctx: ImportContext,
) -> dict:
    """
    Map an Excel row to a semi-normalized participant dict (pre-validation).
    """
    raw_name = empty_to_none(row.get("Name"))
    if not raw_name:
        return {}

    name = normalize_name(raw_name)
    position = empty_to_none(row.get("Position")) or ""
    country_name = empty_to_none(row.get("Country")) or "Unknown"
    birth_country_name = empty_to_none(row.get("Birth Country"))
    gender_str = empty_to_none(row.get("Gender"))
    dob_val = row.get("DOB")
    pob = empty_to_none(row.get("POB")) or ""
    email = empty_to_none(row.get("Email"))
    phone = empty_to_none(row.get("Phone"))
    grade_val = row.get("Grade")
    event_id = empty_to_none(row.get("Event"))

    # Countries
    rep_cid = ensure_country(ctx.countries_col, ctx.country_lookup, country_name)
    birth_cid = ensure_country(
        ctx.countries_col,
        ctx.country_lookup,
        birth_country_name or country_name
    )

    # Enums & dates
    gender = parse_enum_safe(Gender, gender_str, Gender.male)
    try:
        grade = Grade(int(grade_val)) if pd.notna(grade_val) else Grade.NORMAL
    except Exception:
        grade = Grade.NORMAL

    dob = as_dt(pd.to_datetime(dob_val, errors="coerce")) or datetime(1900, 1, 1)
    event_date = ctx.event_lookup.get(event_id, pd.Timestamp.min) if event_id else pd.Timestamp.min

    return {
        "name": name,
        "position": position,
        "grade": grade,
        "representing_country": rep_cid,
        "gender": gender,
        "dob": dob,
        "pob": pob,
        "birth_country": birth_cid,
        "email": email,    # None or valid email
        "phone": phone,    # None or string
        "event_id": event_id,
        "event_date": event_date,
    }


def build_participant_row(pid: str, pdata: dict) -> dict:
    """
    Validate a participant dict via ParticipantRow and return Mongo-ready dict.
    """
    row = ParticipantRow(
        pid=pid,
        name=pdata["name"],
        position=pdata["position"],
        grade=pdata["grade"],
        representing_country=pdata["representing_country"],
        gender=pdata["gender"],
        dob=pdata["dob"],
        pob=pdata["pob"],
        birth_country=pdata["birth_country"],
        email=pdata.get("email") or None,
        phone=pdata.get("phone") or None,
    )
    return row.to_mongo()


# ------------------------------ Importers ---------------------------------

def import_events(ctx: ImportContext, df_events: pd.DataFrame) -> None:
    """
    Upsert events and build an event_lookup of earliest 'Date From' per eid.
    """
    for _, row in df_events.iterrows():
        eid = empty_to_none(row.get("Event"))
        if not eid:
            continue

        title = empty_to_none(row.get("Title")) or ""
        location = empty_to_none(row.get("Location")) or ""
        dt_from = as_dt(pd.to_datetime(row.get("Date From"), errors="coerce"))
        dt_to = as_dt(pd.to_datetime(row.get("Date To"), errors="coerce"))

        ctx.events_col.update_one(
            {"eid": eid},
            {"$set": {"title": title, "location": location, "dateFrom": dt_from, "dateTo": dt_to}},
            upsert=True,
        )

        # for dedup ancestry, use the earliest known date per event
        ts = pd.to_datetime(dt_from) if dt_from else pd.Timestamp.min
        if (eid not in ctx.event_lookup) or (ts < ctx.event_lookup[eid]):
            ctx.event_lookup[eid] = ts


def import_countries(ctx: ImportContext, df_countries: pd.DataFrame) -> None:
    """
    Insert countries with stable CIDs (c001, c002, ...). Rebuild lookup.
    """
    ctx.country_lookup.clear()
    for idx, row in df_countries.iterrows():
        name = empty_to_none(row.get("Country")) or f"Country {idx+1}"
        # ensure_country will insert and register cid if not present
        ensure_country(ctx.countries_col, ctx.country_lookup, name)


def import_participants(ctx: ImportContext, df_participants: pd.DataFrame) -> int:
    """
    Import participants with deduplication by (normalized name, representing country),
    keeping the latest position/grade from the most recent event date.
    Returns number of unique participants added.
    """
    def make_key(name: str, cid: str) -> Tuple[str, str]:
        return (name.lower(), cid.lower())

    participant_data: dict[Tuple[str, str], dict] = {}

    for _, row in df_participants.iterrows():
        pdata = map_participant_row(row, ctx)
        if not pdata:
            continue

        key = make_key(pdata["name"], pdata["representing_country"])
        if key not in participant_data:
            participant_data[key] = {
                "pid": ctx.next_pid(),
                **pdata,
                "events": [pdata["event_id"]] if pdata["event_id"] else [],
                "latest_date": pdata["event_date"],
            }
        else:
            entry = participant_data[key]
            if pdata["event_id"]:
                entry["events"].append(pdata["event_id"])
            # Prefer the most recent event's attributes
            if pdata["event_date"] and (pdata["event_date"] > entry["latest_date"]):
                entry["latest_date"] = pdata["event_date"]
                entry["position"] = pdata["position"]
                entry["grade"] = pdata["grade"]

    # Persist
    added = 0
    for entry in participant_data.values():
        # Build validated doc
        try:
            doc = build_participant_row(entry["pid"], entry)
        except Exception as exc:
            print(f"Skipping participant {entry['pid']}: {exc}")
            continue

        try:
            ctx.participants_col.insert_one(doc)
            added += 1
        except Exception as exc:
            print(f"Insert failed for {entry['pid']}: {exc}")
            continue

        # participant_events links
        for eid in set(e for e in entry["events"] if e):
            try:
                ctx.participant_events_col.insert_one({
                    "participant_id": entry["pid"],
                    "event_id": eid,
                })
            except Exception as exc:
                print(f"Link insert failed pid={entry['pid']} eid={eid}: {exc}")

    return added


# ------------------------------ Orchestrator -------------------------------

def check_and_import_data(path: str = "FILES/final_results.xlsx") -> None:
    """
    Top-level import function. Validates, checks DB state, and runs import steps.
    """
    print("🔍 Checking for existing data...")

    # DB connection check
    try:
        _ = mongodb.db()
    except AttributeError:
        print("⚠️ Database connection not available. Skipping import.")
        return

    ctx = ImportContext()

    event_count_db = ctx.events_col.count_documents({})
    participant_count_db = ctx.participants_col.count_documents({})
    country_count_db = ctx.countries_col.count_documents({})
    print(f"Found {event_count_db} events, {participant_count_db} participants, and {country_count_db} countries in database")

    # Quick short-circuit if DB already populated
    try:
        xl = pd.ExcelFile(path)
        df_events = xl.parse("Events")
        df_countries = xl.parse("Country")
        df_participants = xl.parse("Participant")
        event_count_excel = df_events["Event"].nunique()
    except FileNotFoundError:
        print(f"❌ Excel file not found at '{path}'")
        print("💡 Import skipped - using existing database data")
        return
    except Exception as exc:
        print(f"❌ Failed to read Excel: {exc}")
        return

    if event_count_db >= event_count_excel and participant_count_db > 10 and country_count_db > 5:
        print("✅ Data already exists. Skipping import.")
        return

    # Structural validation
    v = validate_excel_file_for_import(path)
    if not v["ok"]:
        print("❌ Excel structure invalid:")
        for e in v["errors"]:
            print("   -", e)
        return

    print("📦 Importing fresh data...")
    # Clean slate
    ctx.participants_col.delete_many({})
    ctx.events_col.delete_many({})
    ctx.countries_col.delete_many({})
    ctx.participant_events_col.delete_many({})

    # Run steps
    import_events(ctx, df_events)
    import_countries(ctx, df_countries)
    added = import_participants(ctx, df_participants)

    print(f"✅ Import complete: {added} unique participants added.")

def _clean_rec(d: dict) -> dict:
    """Convert pandas NaN/None to clean JSON-serializable values."""
    out = {}
    for k, v in d.items():
        if isinstance(v, float) and pd.isna(v):
            out[k] = None
        elif isinstance(v, pd.Timestamp):
            out[k] = v.to_pydatetime().replace(tzinfo=None).isoformat()
        else:
            # Strip strings; keep others as-is
            if isinstance(v, str):
                s = v.strip()
                out[k] = s if s else None
            else:
                out[k] = v
    return out


def parse_for_commit(path: str) -> dict:
    """
    LEGACY-COMPATIBLE: Build a preview payload without touching the DB.
    Returns:
      {
        "event": {...},
        "participants": [ {...}, ... ]
      }
    """
    try:
        xl = pd.ExcelFile(path)
    except Exception as exc:
        raise RuntimeError(f"Unable to open Excel file: {exc}") from exc

    # ---- Event preview ----
    df_events = xl.parse("Events")
    # Prefer the first row; if multiple events exist, pick the earliest Date From
    if not df_events.empty:
        # Try to select the earliest Date From; fallback to first row
        try:
            df_tmp = df_events.copy()
            df_tmp["__df__"] = pd.to_datetime(df_tmp.get("Date From"), errors="coerce")
            df_tmp = df_tmp.sort_values(by="__df__", na_position="last")
            event_row = df_tmp.iloc[0].to_dict()
        except Exception:
            event_row = df_events.iloc[0].to_dict()

        event = _clean_rec({
            "Event": event_row.get("Event"),
            "Title": event_row.get("Title"),
            "Location": event_row.get("Location"),
            "Date From": event_row.get("Date From"),
            "Date To": event_row.get("Date To"),
        })
    else:
        event = {}

    # ---- Participants preview ----
    df_participants = xl.parse("Participant")
    participants = []
    if not df_participants.empty:
        # Keep the columns your preview template expects
        keep_cols = [
            "Name", "Position", "Country", "Gender",
            "DOB", "POB", "Birth Country", "Email", "Phone", "Event",
        ]
        for _, row in df_participants.iterrows():
            rec = {col: row.get(col) for col in keep_cols if col in df_participants.columns}
            participants.append(_clean_rec(rec))

    return {"event": event, "participants": participants}

