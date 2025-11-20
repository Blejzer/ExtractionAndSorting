# services/import_service_v2.py
"""
================================================================================
Excel Import and Validation Service (Refactored for Readability)
================================================================================

Purpose:
--------
This module parses complex Excel workbooks used for event and participant data.
It validates required sheets and tables, extracts structured data, and builds
preview payloads for database import—without performing any writes.

Functional Sections:
--------------------
1. Configuration & Constants
2. Name / String Normalization Helpers
3. XML Extraction and Parsing Utilities
4. Data Coercion Helpers (dates, enums, booleans)
5. Object Builders (Event, Participant, EventParticipant)
6. Serialization for Preview
7. Lookups (ParticipantsLista, MAIN ONLINE tables)
8. Parsing Logic (country tables, enrichment, payload creation)
9. Validation & Inspection (Excel structure, preview)
10. Workbook/Table Utilities & DB Lookups

Notes:
------
• All logic and behavior are identical to the original `import_service.py`.
• Debug prints remain intact (toggle with `DEBUG_PRINT`).
• No DB writes occur here; only structure, readability, and order improved.
• Type hints will be added in the optimization phase.

================================================================================
"""

# === Standard Library Imports ===
import os
import re
from datetime import datetime, UTC
from typing import Dict, List, Optional, Any

# === Third-Party Imports ===
import openpyxl
import pandas as pd
from openpyxl.utils import range_boundaries

from config.database import mongodb
from config.settings import DEBUG_PRINT, REQUIRE_PARTICIPANTS_LIST
from domain.models.event import Event, EventType
from domain.models.event_participant import DocType, EventParticipant
from domain.models.participant import Grade, Participant
from repositories.participant_repository import ParticipantRepository
from services.xlsx_tables_inspector import list_tables, TableRef
from utils.country_resolver import COUNTRY_TABLE_MAP, resolve_country_flexible, get_country_cid_by_name, \
    _split_multi_country
from utils.dates import MONTHS, normalize_dob, coerce_datetime, date_to_iso
# === Internal Imports ===
from utils.builders import (
    EU_TZ,
    _build_event_from_record,
    _build_participant_event_from_record,
    _build_participant_from_record,
)
from utils.custom_xml import _collect_custom_xml_records
from utils.events import _filename_year_from_eid
from utils.excel import WorkbookCache
from utils.excel import _norm_tablename, get_mapping
from utils.helpers import _as_str_or_empty, _fill_if_missing, _normalize, _parse_bool_value
from utils.lookup import _build_lookup_main_online, _build_lookup_participantslista
from utils.names import (
    _name_key,
    _name_key_from_raw,
    _split_name_variants,
    _to_app_display_name,
)
from utils.normalize_phones import normalize_phone
from utils.participants import _normalize_gender, lookup
from utils.translation import translate
from utils.serialization import (
    merge_attendee_preview,
    serialize_event,
    serialize_model_for_preview,
    serialize_participant_event,
    serialize_participant,
)

# ==============================================================================
# 1. Configuration & Constants
# ==============================================================================

try:  # pragma: no cover - exercised indirectly via repo lookups
    _participant_repo: Optional[ParticipantRepository] = ParticipantRepository()
except Exception:  # pragma: no cover - allow parsing when DB is unavailable
    _participant_repo = None  # type: ignore

# ==============================================================================
# 4. Data Coercion and Normalization Helpers  (REPLACED / OPTIMIZED)
# ==============================================================================



# ==============================================================================
# 6. Serialization Helpers (Preview / Merging)  (REPLACED / OPTIMIZED)
#     (Moved to utils.serialization)
# ==============================================================================

def _load_custom_xml_objects(path: str) -> Optional[Dict[str, Any]]:
    """
    Load structured objects from embedded Custom XML in an Excel file.

    Returns:
        {
            "events": [Event, ...],
            "event":  Event or None,
            "participants": [Participant, ...],
            "participant_events": [EventParticipant, ...]
        }
    """
    records = _collect_custom_xml_records(path)
    if not records:
        return None

    # --- Events ---
    events = []
    for rec in records.get("events", []):
        try:
            events.append(_build_event_from_record(rec))
        except Exception as exc:
            if DEBUG_PRINT:
                print(f"[CUSTOM-XML] Failed to build Event: {exc}")

    # --- Participants ---
    participants: List[Participant] = []
    for rec in records.get("participants", []):
        participant = _build_participant_from_record(rec)
        if participant:
            participants.append(participant)

    # --- Participant ↔ Event relations ---
    participant_events: List[EventParticipant] = []
    for rec in records.get("participant_events", []):
        ep = _build_participant_event_from_record(rec)
        if ep:
            participant_events.append(ep)

    if not events and not participants and not participant_events:
        return None

    return {
        "events": events,
        "event": events[0] if events else None,
        "participants": participants,
        "participant_events": participant_events,
    }

# ==============================================================================
# 9. Column Finder and Main Parsing Routine
# ==============================================================================


def parse_for_commit(path: str, *, preview_only: bool = True) -> dict:
    """
    Parse an Excel workbook into structured event and attendee data.

    Returns:
        dict {
            "event": {...},
            "attendees": [...],
            "objects": None or {events, participants, participant_events},
            "preview": {...}
        }

    Args:
        path: Filesystem path to the uploaded workbook.
        preview_only: Skip participant DB lookups when True (default) to speed
            preview generation.
    """
    # --- Custom XML shortcut (if embedded data exists) ---
    custom_bundle = _load_custom_xml_objects(path)
    if custom_bundle:
        event_obj: Optional[Event] = custom_bundle.get("event")
        participants: List[Participant] = custom_bundle.get("participants", [])
        participant_events: List[EventParticipant] = custom_bundle.get("participant_events", [])

        participants_by_id = {p.pid: p for p in participants}
        attendees = []
        for ep in participant_events:
            participant = participants_by_id.get(ep.participant_id)
            if not participant:
                continue
            attendees.append(merge_attendee_preview(participant, ep))

        preview_event = serialize_event(event_obj)
        if event_obj:
            preview_event["start_date"] = date_to_iso(event_obj.start_date, tzinfo=EU_TZ)
            preview_event["end_date"] = date_to_iso(event_obj.end_date, tzinfo=EU_TZ)

        payload = {
            "event": event_obj.model_dump() if event_obj else {},
            "attendees": attendees,
            "objects": custom_bundle,
            "preview": {
                "event": preview_event,
                "participants": [serialize_participant(p) for p in participants],
                "participant_events": [serialize_participant_event(ep) for ep in participant_events],
            },
        }

        if DEBUG_PRINT:
            print("[CUSTOM-XML] Parsed event", payload["preview"]["event"].get("eid"))
            print(f"[CUSTOM-XML] Participants: {len(participants)} | Participant events: {len(participant_events)}")

        return payload

    cache = WorkbookCache(path)
    participant_lookup_enabled = not preview_only

    # --------------------------------------------------------------------------
    # 1. Event Header
    # --------------------------------------------------------------------------
    eid, title, start_date, end_date, place, country, cost = _read_event_header_block(path, cache)
    if DEBUG_PRINT:
        print(
            "[STEP] Event header:",
            {"eid": eid, "title": title, "start_date": start_date,
             "end_date": end_date, "place": place, "country": country, "cost": cost},
        )

    # --------------------------------------------------------------------------
    # 2. Table Discovery & Lookups
    # --------------------------------------------------------------------------
    tables = list_tables(path)
    idx = _index_tables(tables)

    plist = _find_table_exact(idx, "ParticipantsLista")
    ponl = _find_table_exact(idx, "ParticipantsList")

    if not plist:
        raise RuntimeError("Required table 'ParticipantsLista' not found")
    if REQUIRE_PARTICIPANTS_LIST and not ponl:
        raise RuntimeError("Required table 'ParticipantsList' (MAIN ONLINE) not found")

    df_positions = _read_table_df(path, plist, cache)
    df_online = _read_table_df(path, ponl, cache) if ponl else pd.DataFrame()

    positions_lookup = _build_lookup_participantslista(df_positions)
    online_lookup = _build_lookup_main_online(df_online) if not df_online.empty else {}

    if DEBUG_PRINT:
        print(f"[STEP] Positions lookup entries: {len(positions_lookup)}")
        print(f"[STEP] Online lookup entries: {len(online_lookup)}")

    # --------------------------------------------------------------------------
    # 3. Collect Attendees from Country Tables
    # --------------------------------------------------------------------------
    attendees: List[dict] = []
    initial_attendees: List[dict] = []

    for key, country_label in COUNTRY_TABLE_MAP.items():
        table = _find_table_exact(idx, key)
        if not table:
            continue

        df = _read_table_df(path, table, cache)
        if df.empty:
            continue

        # Use the Excel matrix to resolve headers for this country table
        # Matrix shape: {excel_header -> target_field}
        m = get_mapping("Participants", key)  # key is 'tableAlb', 'tableBih', etc.

        # Invert once to target->excel_header for quick lookups
        inv = {t: h for h, t in m.items()}

        # Pick headers from the inverted map. Return None if missing (we'll guard later).
        nm_col = inv.get("name_full")  # was "Name and Last Name"
        trans_col = inv.get("travel")  # was "Travel"
        from_col = inv.get("traveling_from")  # was "Traveling from"
        grade_col = inv.get("grade")  # was "Grade (0 - BL, 1 - Pass, 2 - Excel)"

        # Defensive: if the workbook renamed a header unexpectedly, drop to None.
        if nm_col not in df.columns:
            nm_col = None
        if trans_col not in df.columns:
            trans_col = None
        if from_col not in df.columns:
            from_col = None
        if grade_col not in df.columns:
            grade_col = None

        if not nm_col:
            continue

        prefer_online_transport = trans_col is None

        for _, row in df.iterrows():
            name_cell = row.get(nm_col)
            if name_cell is None or pd.isna(name_cell):
                continue

            if isinstance(name_cell, str) and not name_cell.strip():
                continue

            raw_name = _normalize(str(name_cell))
            if not raw_name or raw_name.upper() == "TOTAL":
                continue

            def _normalized_cell(col_name: Optional[str]) -> str:
                if not col_name:
                    return ""
                value = row.get(col_name)
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    return ""
                return _normalize(str(value))

            transportation = _normalized_cell(trans_col)
            traveling_from = _normalized_cell(from_col)
            grade_val = row.get(grade_col, None)
            grade = None
            if isinstance(grade_val, (int, float)) and not pd.isna(grade_val):
                try:
                    grade = int(grade_val)
                except Exception:
                    pass

            # --- Match lookups ---
            variants = list(_split_name_variants(raw_name))
            p_list, p_comp = {}, {}
            for f, m, l in variants:
                key_a = _name_key(l, " ".join([f, m]).strip())
                key_b = _name_key(l, f) if f else None
                cand_list = (online_lookup.get(key_a) or (online_lookup.get(key_b) if key_b else None)) or {}
                cand_comp = (positions_lookup.get(key_a) or (positions_lookup.get(key_b) if key_b else None)) or {}
                if cand_list or cand_comp:
                    p_list, p_comp = cand_list, cand_comp
                    break

            # --- Base attendee record ---
            ordered = _normalize(raw_name)
            if "," in ordered:
                last_part, first_part = [x.strip() for x in ordered.split(",", 1)]
                ordered = f"{first_part} {last_part}".strip()
            base_name = _to_app_display_name(ordered)

            country_cid = get_country_cid_by_name(country_label) or country_label
            if transportation:
                transportation_value = transportation
            elif prefer_online_transport:
                transportation_value = p_list.get("transportation_declared") or ""
            else:
                transportation_value = ""
            transport_other_value = (str(p_list.get("transport_other", "")) or "").strip()
            traveling_from_value = traveling_from or p_list.get("traveling_from_declared") or ""
            grade_value = grade if grade is not None else int(Grade.NORMAL)

            base_record = {
                "name": base_name,
                "representing_country": country_cid,
                "transportation": transportation_value,
                "transport_other": transport_other_value,
                "traveling_from": traveling_from_value,
                "grade": grade_value,
            }
            initial_attendees.append(base_record)

            # --- Enrich with ParticipantsLista info ---
            record = {
                **base_record,
                "position": p_comp.get("position") or "",
                "phone": normalize_phone(p_comp.get("phone")) or "",
                "email": p_comp.get("email") or "",
            }

            # --- MAIN ONLINE enrichment ---
            online = p_list or {}
            _fill_if_missing(record, "position", online, "position_online")
            _fill_if_missing(record, "phone", online, "phone_list")
            _fill_if_missing(record, "email", online, "email_list")
            _fill_if_missing(record, "traveling_from", online, "traveling_from_declared")
            if not record.get("transportation") and prefer_online_transport:
                record["transportation"] = online.get("transportation_declared") or None
            _fill_if_missing(record, "transport_other", online, "transport_other")

            # --- Country & citizenship normalization ---
            birth_country_value = online.get("birth_country", "")
            birth_res = resolve_country_flexible(str(birth_country_value))
            birth_country_cid = birth_res["cid"] if birth_res else country_cid
            citizenships_raw = online.get("citizenships", [])
            if isinstance(citizenships_raw, str):
                citizenships_raw = [citizenships_raw]

            citizenships_clean: list[str] = []
            for tok in _split_multi_country(citizenships_raw):
                res = resolve_country_flexible(tok)
                if res and res.get("cid"):
                    cid = res["cid"]
                    if cid not in citizenships_clean:
                        citizenships_clean.append(cid)

            if DEBUG_PRINT:
                print("[TOKENS]", _split_multi_country(online.get("citizenships", [])))
            for tok in _split_multi_country(online.get("citizenships", [])):
                r = resolve_country_flexible(tok)
                print("   ->", tok, "=>", (r and r.get("cid"), r and r.get("country")))
            if DEBUG_PRINT:
                print("[OUT] citizenships:", citizenships_clean)


            # --- Final enrichment ---
            record.update({
                "gender": online.get("gender", ""),
                "dob": date_to_iso(online.get("dob"), tzinfo=EU_TZ),
                "pob": online.get("pob", ""),
                "birth_country": birth_country_cid,
                "citizenships": citizenships_clean,
                "travel_doc_type": online.get("travel_doc_type"),
                "travel_doc_number": online.get("travel_doc_number", ""),
                "travel_doc_issue_date": date_to_iso(online.get("travel_doc_issue"), tzinfo=EU_TZ),
                "travel_doc_expiry_date": date_to_iso(online.get("travel_doc_expiry"), tzinfo=EU_TZ),
                "travel_doc_issued_by": online.get("travel_doc_issued_by", ""),
                "returning_to": online.get("returning_to", ""),
                "diet_restrictions": online.get("diet_restrictions", ""),
                "organization": online.get("organization", ""),
                "unit": online.get("unit", ""),
                "rank": online.get("rank", ""),
                "intl_authority": _parse_bool_value(online.get("intl_authority", "")) or False,
                "bio_short": online.get("bio_short", ""),
                "bank_name": online.get("bank_name", ""),
                "iban": online.get("iban", ""),
                "iban_type": online.get("iban_type"),
                "swift": online.get("swift", ""),
            })
            if DEBUG_PRINT:
                print(f"[DEBUG] citizenships_in={online.get('citizenships')} → {record['citizenships']}")

            participant = None
            if participant_lookup_enabled:
                participant = lookup(
                    name_display=record.get("name", ""),
                    country_name=country_label,
                    dob_source=online.get("dob"),
                    representing_country=country_cid,
                )

            if participant:
                record["pid"] = participant.pid

            record["phone"] = normalize_phone(record.get("phone")) or ""
            attendees.append(record)

    # --------------------------------------------------------------------------
    # 4. Assemble Final Payload
    # --------------------------------------------------------------------------
    if DEBUG_PRINT:
        print("[STEP] Initial participant list:")
        for rec in initial_attendees:
            print("  ", rec)

    payload = {
        "event": {
            "eid": eid,
            "title": title,
            "start_date": start_date,
            "end_date": end_date,
            "place": place,
            "country": country,
            "type": "Training",
            "cost": cost,
        },
        "attendees": attendees,
        "objects": None,
        "preview": {
            "event": {
                "eid": eid,
                "title": title,
                "start_date": date_to_iso(start_date, tzinfo=EU_TZ),
                "end_date": date_to_iso(end_date, tzinfo=EU_TZ),
                "place": place,
                "country": country,
                "type": "Training",
                "cost": cost,
            },
            "participants": attendees,
            "participant_events": [],
        },
    }

    if DEBUG_PRINT:
        payload["initial_attendees"] = initial_attendees

    return payload

# ==============================================================================
# 11. Workbook / Table Utilities
# ==============================================================================

def _index_tables(tables: List[TableRef]) -> Dict[str, List[TableRef]]:
    """Group tables by normalized name for quick lookup."""
    idx: Dict[str, List[TableRef]] = {}
    for t in tables:
        idx.setdefault(t.name_norm, []).append(t)
    return idx


def _find_table_exact(idx: Dict[str, List[TableRef]], desired: str) -> Optional[TableRef]:
    """Find the first table matching `desired` by normalized name."""
    target = _norm_tablename(desired)
    group = idx.get(target)
    return group[0] if group else None


def _read_table_df(path: str, table: TableRef, cache: WorkbookCache | None = None) -> pd.DataFrame:
    """
    Read a ListObject range (e.g. 'A4:K7') into a DataFrame.
    Uses the header row as columns.
    """
    def _build_df(ws) -> pd.DataFrame:
        min_col, min_row, max_col, max_row = range_boundaries(table.ref)
        rows = list(
            ws.iter_rows(
                min_row=min_row,
                max_row=max_row,
                min_col=min_col,
                max_col=max_col,
                values_only=True,
            )
        )
        return _dataframe_from_rows(rows)

    if cache:
        return cache.get_table_df(table, _build_df)

    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb[table.sheet_title]
    return _build_df(ws)


def _dataframe_from_rows(rows: List[tuple]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    header = [_normalize(str(h)) if h is not None else "" for h in rows[0]]
    df = pd.DataFrame(rows[1:], columns=header).dropna(how="all")

    empty_cols = [col for col in df.columns if not str(col).strip()]
    if empty_cols:
        df = df.drop(columns=empty_cols)

    return df


# ==============================================================================
# 12. Event-Header Parsing
# ==============================================================================

def _parse_event_header(a1: str, a2: str, year: int):
    """
    Parse event metadata from:
      A1 → 'PFE25M2 TITLE OF EVENT'
      A2 → 'JUNE 23 - 27 - Opatija, CROATIA'
    """
    a1 = _normalize(a1)
    sp = a1.find(" ")
    eid, title = (a1, "") if sp == -1 else (a1[:sp], a1[sp + 1:])

    a2 = _normalize(a2)
    parts = [p.strip() for p in a2.split(" - ")]
    start_date = end_date = None
    location = ""

    if len(parts) >= 3:
        month_and_start, end_day_str, location = parts[0], parts[1], parts[2]
        m = re.match(r"([A-Z]+)\s+(\d{1,2})", month_and_start.upper())
        if m:
            month_num = MONTHS.get(m.group(1))
            start_day = int(m.group(2))
            if month_num:
                end_day = int(re.sub(r"\D", "", end_day_str))
                start_date = datetime(year, month_num, start_day, tzinfo=UTC)
                end_date = datetime(year, month_num, end_day, tzinfo=UTC)

    place = location
    country_value: str | None = None
    if location:
        loc_parts = [p.strip() for p in location.split(",", 1)]
        place = loc_parts[0]
        raw_country = loc_parts[1] if len(loc_parts) > 1 else ""
        if raw_country:
            normalized_country = _normalize(raw_country)
            lookup = get_country_cid_by_name(normalized_country) or get_country_cid_by_name(normalized_country.title())
            country_value = lookup or normalized_country

    return eid, title, start_date, end_date, place, country_value


def _read_event_header_block(
    path: str,
    cache: WorkbookCache | None = None,
) -> tuple[str, str, datetime, datetime, str, Optional[str], Optional[float]]:
    """Read event header data from the Participants and COST Overview sheets."""
    wb = cache.get_workbook() if cache else openpyxl.load_workbook(path, data_only=True)
    if "Participants" not in wb.sheetnames:
        raise RuntimeError("Sheet 'Participants' not found")
    if "COST Overview" not in wb.sheetnames:
        raise RuntimeError("Sheet 'COST Overview' not found")

    ws = wb["Participants"]
    a1 = ws["A1"].value or ""
    a2 = ws["A2"].value or ""
    year = _filename_year_from_eid(os.path.basename(path))
    eid, title, start_date, end_date, place, country = _parse_event_header(a1, a2, year)

    wws = wb["COST Overview"]
    cost_overview_b15 = str(wws["B15"].value or "").strip()
    cost = float(cost_overview_b15) if cost_overview_b15 else None
    return eid, title, start_date, end_date, place, country, cost


# ==============================================================================
# 13. Public Validation / Preview API
# ==============================================================================

def validate_excel_file_for_import(path: str) -> tuple[bool, list[str], dict]:
    """
    Validate Excel workbook structure and required elements.
    Checks:
      - Sheets: Participants, COST Overview
      - Cells: A1/A2, B15
      - Tables: ParticipantsLista, ≥1 country table
    """
    custom_bundle = _load_custom_xml_objects(path)
    if custom_bundle:
        event_obj = custom_bundle.get("event")
        participants = custom_bundle.get("participants", [])
        participant_events = custom_bundle.get("participant_events", [])
        seen = {
            "custom_xml": True,
            "events": len(custom_bundle.get("events", [])),
            "participants": len(participants),
            "participant_events": len(participant_events),
        }
        if event_obj:
            seen["event_eid"] = event_obj.eid
        return True, [], seen

    missing: list[str] = []

    wb = openpyxl.load_workbook(path, data_only=True)
    if "Participants" not in wb.sheetnames:
        missing.append("Sheet 'Participants'")
        return False, missing, {}
    if "COST Overview" not in wb.sheetnames:
        missing.append("Sheet 'COST Overview'")
        return False, missing, {}

    ws, wws = wb["Participants"], wb["COST Overview"]
    a1 = (ws["A1"].value or "").strip()
    a2 = (ws["A2"].value or "").strip()
    cost_overview_b15 = str(wws["B15"].value or "").strip()
    if not a1:
        missing.append("Participants!A1 (eid + title)")
    if not a2:
        missing.append("Participants!A2 (dates + location)")
    if not cost_overview_b15:
        missing.append("Cost Overview!B15 (Total Cost)")

    tables = list_tables(path)
    idx = _index_tables(tables)

    if not _find_table_exact(idx, "ParticipantsLista"):
        missing.append("Table 'ParticipantsLista'")
    if not any(_find_table_exact(idx, k) for k in COUNTRY_TABLE_MAP.keys()):
        missing.append("At least one country table (tableAlb, tableBih, tableCro, etc.)")

    ok = len(missing) == 0
    seen = {t.name_norm: (t.name, t.sheet_title, t.ref) for t in tables}

    if DEBUG_PRINT:
        print("[VALIDATE] OK:", ok)
        if missing:
            print("[VALIDATE] Missing:", missing)
        print("[VALIDATE] Tables seen:", seen)

    return ok, missing, seen


def inspect_and_preview_uploaded(path: str, *, preview_only: bool = True) -> None:
    """
    Dry-run inspection: prints existing/new event info and attendee summaries.
    No DB writes.

    Args:
        path: Filesystem path to the uploaded workbook.
        preview_only: Skip participant DB lookups when True (default).
    """
    cache = WorkbookCache(path)
    participant_lookup_enabled = not preview_only
    eid, title, start_date, end_date, place, country, cost = _read_event_header_block(path, cache)

    existing = mongodb.collection("events").find_one({"eid": eid})
    if existing:
        print(f"[EVENT] EXIST {eid} title='{existing.get('title','')}' "
              f"start_date={existing.get('start_date')} place='{existing.get('place','')}' "
              f"country='{existing.get('country')}'")
    else:
        print(f"[EVENT] NEW {eid} title='{title}' start_date={start_date} "
              f"end_date={end_date} place='{place}' country='{country}'")

    tables = list_tables(path)
    idx = _index_tables(tables)

    plist = _find_table_exact(idx, "ParticipantsLista")
    if not plist:
        raise RuntimeError("Required table 'ParticipantsLista' not found (any sheet)")

    df_positions = _read_table_df(path, plist, cache)
    positions_lookup_full = _build_lookup_participantslista(df_positions)

    print("[ATTENDEES]")
    for key, country_label in COUNTRY_TABLE_MAP.items():
        t = _find_table_exact(idx, key)
        if not t:
            continue
        df = _read_table_df(path, t, cache)
        if df.empty:
            continue

        nm_col = next((c for c in df.columns if "name" in c.lower()), None)
        grade_col = next((c for c in df.columns if "grade" in c.lower()), None)

        if not nm_col:
            continue

        for _, row in df.iterrows():
            name_cell = row.get(nm_col)
            if name_cell is None or pd.isna(name_cell):
                continue

            if isinstance(name_cell, str) and not name_cell.strip():
                continue

            raw_name = _normalize(str(name_cell))
            if not raw_name:
                continue
            grade = _normalize(str(row.get(grade_col, ""))) if grade_col else ""
            key_lookup = _name_key_from_raw(raw_name)
            pos = positions_lookup_full.get(key_lookup, {}).get("position", "")

            participant = None
            if participant_lookup_enabled:
                participant = lookup(
                    name_display=raw_name,
                    country_name=country_label,
                )

            norm = _to_app_display_name(raw_name)
            star = "*" if not participant else " "
            pid = participant.pid if participant else "NEW"

            print(f"{star} {'NEW' if star=='*' else 'EXIST'} {pid:>6} {norm} "
                  f"({grade}, {country_label}) {'pos='+pos if pos else ''}")


