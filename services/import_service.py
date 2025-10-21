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
import zipfile
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple, Iterator, Any

import pandas as pd
import openpyxl
from openpyxl.utils import range_boundaries
from datetime import datetime, UTC, date, time

from config.database import mongodb
from domain.models.event import Event, EventType
from domain.models.event_participant import (
    DocType,
    EventParticipant,
    IbanType,
    Transport,
)
from domain.models.participant import Grade, Gender, Participant
from services.xlsx_tables_inspector import (
    list_sheets,
    list_tables,
    TableRef,
)
from utils.translation import translate
from utils.country_resolver import (
    COUNTRY_TABLE_MAP,
    normalize_citizenships,
    resolve_birth_country_cid,
    resolve_country,
)

# ============================
# Configuration / Constants
# ============================

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


# ============================
# Custom XML extraction helpers
# ============================

def _strip_xml_tag(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def _element_to_flat_dict(elem: ET.Element, prefix: str = "") -> Dict[str, str]:
    children = list(elem)
    if not children:
        key = prefix or _strip_xml_tag(elem.tag)
        return {key: (elem.text or "").strip()}

    data: Dict[str, str] = {}
    for child in children:
        child_tag = _strip_xml_tag(child.tag)
        child_prefix = f"{prefix}_{child_tag}" if prefix else child_tag
        child_data = _element_to_flat_dict(child, child_prefix)
        for key, value in child_data.items():
            if not value:
                continue
            if key in data and data[key]:
                data[key] = f"{data[key]}; {value}"
            else:
                data[key] = value
    return data


def _collect_custom_xml_records(path: str) -> Optional[Dict[str, List[Dict[str, str]]]]:
    try:
        with zipfile.ZipFile(path) as zf:
            names = [
                name
                for name in zf.namelist()
                if name.startswith("customXml/") and name.endswith(".xml")
            ]
            if not names:
                return None

            collected: Dict[str, List[Dict[str, str]]] = {
                "participant": [],
                "event": [],
                "participant_event": [],
            }

            for name in names:
                try:
                    root = ET.fromstring(zf.read(name))
                except ET.ParseError:
                    if DEBUG_PRINT:
                        print(f"[CUSTOM-XML] Failed to parse {name}")
                    continue

                stack = [root]
                while stack:
                    node = stack.pop()
                    tag = _strip_xml_tag(node.tag)
                    if tag in collected:
                        collected[tag].append(_element_to_flat_dict(node))
                    stack.extend(list(node))

            if not any(collected.values()):
                return None

            return {
                "participants": collected["participant"],
                "events": collected["event"],
                "participant_events": collected["participant_event"],
            }
    except (zipfile.BadZipFile, FileNotFoundError):
        return None


def _parse_datetime_value(value: object) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return datetime.combine(dt.date(), time.min)
        except ValueError:
            continue
    return None


def _parse_date_value(value: object) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_bool_value(value: object) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    s = str(value).strip().lower()
    if not s:
        return None
    if s in {"true", "1", "yes"}:
        return True
    if s in {"false", "0", "no"}:
        return False
    return None


def _coerce_event_type(value: object) -> Optional[EventType]:
    if value is None:
        return None
    candidates = []
    if isinstance(value, EventType):
        return value
    s = str(value).strip()
    if not s:
        return None
    candidates.append(s)
    candidates.append(s.title())
    candidates.append(s.upper())
    for cand in candidates:
        try:
            return EventType(cand)
        except ValueError:
            continue
    return None


def _coerce_grade_value(value: object) -> int:
    if isinstance(value, (int, float)) and not pd.isna(value):
        return int(value)
    if value is None:
        return int(Grade.NORMAL)
    s = str(value).strip()
    if not s:
        return int(Grade.NORMAL)
    try:
        return int(float(s))
    except ValueError:
        pass
    try:
        return int(Grade[s.upper()].value)
    except Exception:
        return int(Grade.NORMAL)


def _build_event_from_record(record: Dict[str, str]) -> Event:
    start_date = _parse_datetime_value(record.get("start_date"))
    end_date = _parse_datetime_value(record.get("end_date"))
    cost_raw = record.get("cost")
    cost_val: Optional[float] = None
    if cost_raw not in (None, ""):
        try:
            cost_val = float(str(cost_raw).strip())
        except ValueError:
            cost_val = None

    event_type = _coerce_event_type(record.get("type"))

    return Event(
        eid=str(record.get("eid", "")),
        title=str(record.get("title", "")),
        start_date=start_date,
        end_date=end_date,
        place=str(record.get("place", "")),
        country=record.get("country") or None,
        type=event_type,
        cost=cost_val,
    )


def _build_participant_from_record(record: Dict[str, str]) -> Optional[Participant]:
    data: Dict[str, Any] = dict(record)
    data["grade"] = _coerce_grade_value(data.get("grade"))
    dob = _parse_datetime_value(data.get("dob"))
    if dob:
        data["dob"] = dob
    bool_fields = ["intl_authority"]
    for field in bool_fields:
        if field in data:
            val = _parse_bool_value(data[field])
            if val is not None:
                data[field] = val
    try:
        return Participant.model_validate(data)
    except Exception as exc:
        if DEBUG_PRINT:
            print(f"[CUSTOM-XML] Failed to build Participant: {exc}")
        return None


def _build_participant_event_from_record(record: Dict[str, str]) -> Optional[EventParticipant]:
    data: Dict[str, Any] = dict(record)
    bool_fields = ["requires_visa_hr"]
    for field in bool_fields:
        if field in data:
            val = _parse_bool_value(data[field])
            if val is not None:
                data[field] = val
    for key in ("travel_doc_issue_date", "travel_doc_expiry_date"):
        if key in data:
            data[key] = _parse_date_value(data.get(key))
    try:
        return EventParticipant.model_validate(data)
    except Exception as exc:
        if DEBUG_PRINT:
            print(f"[CUSTOM-XML] Failed to build EventParticipant: {exc}")
        return None


def _serialize_event_for_preview(event: Optional[Event]) -> Dict[str, Any]:
    if not event:
        return {}
    data = event.model_dump()
    preview: Dict[str, Any] = {}
    for key, value in data.items():
        if isinstance(value, (datetime, date)):
            preview[key] = _date_to_iso(value)
        elif isinstance(value, EventType):
            preview[key] = value.value
        else:
            preview[key] = value
    return preview


def _serialize_participant_for_preview(participant: Participant) -> Dict[str, Any]:
    data = participant.model_dump(exclude_none=True)
    if "dob" in data:
        data["dob"] = _date_to_iso(participant.dob)
    data["grade"] = int(participant.grade)
    if isinstance(data.get("gender"), Gender):
        data["gender"] = data["gender"].value
    return data


def _serialize_participant_event_for_preview(ep: EventParticipant) -> Dict[str, Any]:
    data = ep.model_dump(exclude_none=True)
    for key in ("travel_doc_issue_date", "travel_doc_expiry_date"):
        if key in data:
            data[key] = _date_to_iso(data[key])
    if isinstance(data.get("transportation"), Transport):
        data["transportation"] = data["transportation"].value
    if isinstance(data.get("travel_doc_type"), DocType):
        data["travel_doc_type"] = data["travel_doc_type"].value
    if isinstance(data.get("iban_type"), IbanType):
        data["iban_type"] = data["iban_type"].value
    return data


def _merge_attendee_preview(participant: Participant, ep: EventParticipant) -> Dict[str, Any]:
    transportation = (
        ep.transportation.value
        if isinstance(ep.transportation, Transport)
        else ep.transportation
    )
    travel_doc_type = (
        ep.travel_doc_type.value
        if isinstance(ep.travel_doc_type, DocType)
        else ep.travel_doc_type
    )
    iban_type = (
        ep.iban_type.value if isinstance(ep.iban_type, IbanType) else ep.iban_type
    )

    attendee: Dict[str, Any] = {
        "pid": participant.pid,
        "name": participant.name,
        "representing_country": participant.representing_country,
        "gender": participant.gender.value if isinstance(participant.gender, Gender) else participant.gender,
        "grade": int(participant.grade),
        "dob": _date_to_iso(participant.dob),
        "pob": participant.pob,
        "birth_country": participant.birth_country,
        "citizenships": participant.citizenships or [],
        "email": participant.email,
        "phone": participant.phone,
        "diet_restrictions": participant.diet_restrictions,
        "organization": participant.organization,
        "unit": participant.unit,
        "position": participant.position,
        "rank": participant.rank,
        "intl_authority": participant.intl_authority,
        "bio_short": participant.bio_short,
    }

    attendee.update(
        {
            "event_id": ep.event_id,
            "participant_id": ep.participant_id,
            "transportation": transportation,
            "transport_other": ep.transport_other,
            "requires_visa_hr": ep.requires_visa_hr,
            "traveling_from": ep.traveling_from,
            "returning_to": ep.returning_to,
            "travel_doc_type": travel_doc_type,
            "travel_doc_type_other": ep.travel_doc_type_other,
            "travel_doc_issue_date": _date_to_iso(ep.travel_doc_issue_date),
            "travel_doc_expiry_date": _date_to_iso(ep.travel_doc_expiry_date),
            "travel_doc_issued_by": ep.travel_doc_issued_by,
            "bank_name": ep.bank_name,
            "iban": ep.iban,
            "iban_type": iban_type,
            "swift": ep.swift,
        }
    )

    return attendee


def _load_custom_xml_objects(path: str) -> Optional[Dict[str, Any]]:
    records = _collect_custom_xml_records(path)
    if not records:
        return None

    events = []
    for rec in records.get("events", []):
        try:
            events.append(_build_event_from_record(rec))
        except Exception as exc:
            if DEBUG_PRINT:
                print(f"[CUSTOM-XML] Failed to build Event: {exc}")

    participants: List[Participant] = []
    for rec in records.get("participants", []):
        participant = _build_participant_from_record(rec)
        if participant:
            participants.append(participant)

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
        first_middle = " ".join(part for part in [first, middle] if part).strip()
        key = _name_key(last, first_middle)
        full_name = " ".join([first, middle, last]).strip()
        gender_col = col("Gender")
        gender_raw = (str(r.get(gender_col, "")) if gender_col else "").strip()
        gender = gender_raw
        if gender_raw:
            try:
                gender = Gender(gender_raw).value
            except ValueError:
                try:
                    gender = Gender(gender_raw.title()).value
                except ValueError:
                    gender = gender_raw.title()
        birth_country_raw = _normalize(str(r.get(col("Country of Birth"), "")))
        birth_country_en = translate(birth_country_raw, "en")
        birth_country_en = re.sub(r",\s*world$", "", birth_country_en, flags=re.IGNORECASE)

        travel_doc_type_col = col("Traveling document type")
        travel_doc_type_raw = (
            str(r.get(travel_doc_type_col, "")) if travel_doc_type_col else ""
        ).strip()
        travel_doc_type_value = (travel_doc_type_raw if travel_doc_type_raw else "").strip()
        if not travel_doc_type_value:
            travel_doc_type_value = travel_doc_type_raw
        travel_doc_type_other_col = col("Traveling document type (Other)")
        travel_doc_type_other_value = (
            str(r.get(travel_doc_type_other_col, "")) if travel_doc_type_other_col else ""
        ).strip()

        transportation_col = col("Transportation")
        transportation_value = (
            str(r.get(transportation_col, "")) if transportation_col else ""
        ).strip()
        transport_other_col = col("Transportation (Other)")
        transport_other_value = (
            str(r.get(transport_other_col, "")) if transport_other_col else ""
        ).strip()

        iban_type_col = col("IBAN Type")
        iban_type_value = (
            str(r.get(iban_type_col, "")) if iban_type_col else ""
        ).strip()

        entry = {
            "name": full_name,
            "gender": gender,
            "dob": r.get(col("Date of Birth (DOB)")),
            "pob": _normalize(str(r.get(col("Place Of Birth (POB)"), ""))),
            "birth_country": birth_country_en,
            "citizenships": [
                _normalize(x)
                for x in re.split(r"[;,]", str(r.get(col("Citizenship(s)"), "")))
                if _normalize(x)
            ],
            "email_list": _normalize(str(r.get(col("Email address"), ""))),
            "phone_list": _normalize(str(r.get(col("Phone number"), ""))),
            "travel_doc_type": travel_doc_type_value,
            "travel_doc_type_other": travel_doc_type_other_value,
            "travel_doc_number": _normalize(str(r.get(col("Traveling document number"), ""))),
            "travel_doc_issue": r.get(col("Traveling document issuance date")),
            "travel_doc_expiry": r.get(col("Traveling document expiration date")),
            "travel_doc_issued_by": _normalize(str(r.get(col("Traveling document issued by"), ""))),
            "transportation_declared": transportation_value,
            "transport_other": transport_other_value,
            "traveling_from_declared": _normalize(str(r.get(col("Traveling from"), ""))),
            "returning_to": _normalize(str(r.get(col("Returning to"), ""))),
            "diet_restrictions": _normalize(str(r.get(col("Diet restrictions"), ""))),
            "organization": translate(_normalize(str(r.get(col("Organization"), ""))), "en"),
            "unit": _normalize(str(r.get(col("Unit"), ""))),
            # "position_online": _normalize(str(r.get(col("Position"), ""))),
            "rank": translate(_normalize(str(r.get(col("Rank"), ""))), "en"),
            "intl_authority": _normalize(str(r.get(col("Authority"), ""))),
            "bio_short": translate(_normalize(str(r.get(col("Short professional biography"), ""))), "en"),
            "bank_name": _normalize(str(r.get(col("Bank name"), ""))),
            "iban": _normalize(str(r.get(col("IBAN"), ""))),
            "iban_type": iban_type_value,
            "swift": _normalize(str(r.get(col("SWIFT"), ""))),
        }
        keys = [key]
        if middle and first:
            keys.append(_name_key(last, first))

        for idx, name_key in enumerate(keys):
            if not name_key:
                continue
            if idx == 0 or name_key not in look:
                look[name_key] = entry
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
    custom_bundle = _load_custom_xml_objects(path)
    if custom_bundle:
        event_obj: Optional[Event] = custom_bundle.get("event")
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
        if wl == "transport" and (cl == "travel" in cl):
            return c
        if wl == "from" and ("Traveling from" in cl):
            return c
        if wl == "grade" and "grade" in cl:
            return c
    return None

# --- ADD: the new public API for full parse (NO DB WRITES) ---
def parse_for_commit(path: str) -> dict:
    """
    Returns a dict with:
      - event: {eid, title, start_date, end_date, place, country, type, cost}
      - attendees: [ {name,
                      representing_country, transportation, traveling_from, grade,
                      position, phone, email, ...plus MAIN ONLINE fields when present} ]
    The raw attendee records (``initial_attendees``) are collected only when
    ``DEBUG_PRINT`` is enabled and otherwise omitted from the returned payload.
    """
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
            attendees.append(_merge_attendee_preview(participant, ep))

        payload = {
            "event": event_obj.model_dump() if event_obj else {},
            "attendees": attendees,
            "objects": custom_bundle,
            "preview": {
                "event": _serialize_event_for_preview(event_obj),
                "participants": [
                    _serialize_participant_for_preview(p) for p in participants
                ],
                "participant_events": [
                    _serialize_participant_event_for_preview(ep)
                    for ep in participant_events
                ],
            },
        }

        if DEBUG_PRINT:
            print("[CUSTOM-XML] Parsed event", payload["preview"]["event"].get("eid"))
            print(
                f"[CUSTOM-XML] Participants: {len(participants)} | Participant events: {len(participant_events)}"
            )

        return payload

    # 1) Event header
    wb = openpyxl.load_workbook(path, data_only=True)
    if "Participants" not in wb.sheetnames:
        raise RuntimeError("Sheet 'Participants' not found")
    ws = wb["Participants"]
    a1 = ws["A1"].value or ""
    a2 = ws["A2"].value or ""
    year = _filename_year_from_eid(os.path.basename(path))
    eid, title, start_date, end_date, place, country = _parse_event_header(a1, a2, year)

    if DEBUG_PRINT:
        print(
            "[STEP] Event header:",
            {
                "eid": eid,
                "title": title,
                "start_date": start_date,
                "end_date": end_date,
                "place": place,
                "country": country,
            },
        )

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
            traveling_from = _normalize(str(row.get(from_col, ""))) if from_col else ""
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

            # Compose attendee record
            country_cid = _country_cid(country_label) or country_label
            transportation_value = transportation or ""
            if (not transportation_value) and p_list.get("transportation_declared"):
                transportation_value = (str(p_list.get("transportation_declared")) or "").strip()
            transportation_value = transportation_value or None
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

            record = {
                **base_record,
                "position": p_comp.get("position") or "", # or p_list.get("position_online")
                "phone":    p_comp.get("phone")  or  "", # or p_list.get("phone_list")
                "email":    p_comp.get("email")    or "", # or p_list.get("email_list")
            }

            # add remaining MAIN ONLINE fields – always include the keys so the
            # payload schema is consistent even when we cannot enrich the
            # attendee from the ParticipantsList table.
            online = p_list or {}
            birth_country_value = online.get("birth_country", "")
            birth_country_cid = resolve_birth_country_cid(
                birth_country_value,
                country_cid,
                country_label,
                lookup=_country_cid,
            )

            record.update({
                "gender": online.get("gender", ""),
                "dob": _date_to_iso(online.get("dob")),
                "pob": online.get("pob", ""),
                "birth_country": birth_country_cid,
                "citizenships": normalize_citizenships(
                    online.get("citizenships", []),
                    lookup=_country_cid,
                ),
                "travel_doc_type": online.get("travel_doc_type"),
                "travel_doc_type_other": online.get("travel_doc_type_other", ""),
                "travel_doc_number": online.get("travel_doc_number", ""),
                "travel_doc_issue_date": _date_to_iso(online.get("travel_doc_issue")),
                "travel_doc_expiry_date": _date_to_iso(online.get("travel_doc_expiry")),
                "travel_doc_issued_by": online.get("travel_doc_issued_by", ""),
                "returning_to": online.get("returning_to", ""),
                "diet_restrictions": online.get("diet_restrictions", ""),
                "organization": online.get("organization", ""),
                "unit": online.get("unit", ""),
                "rank": online.get("rank", ""),
                "intl_authority": str(online.get("intl_authority", "")).lower() in ("yes", "true", "1"),
                "bio_short": online.get("bio_short", ""),
                "bank_name": online.get("bank_name", ""),
                "iban": online.get("iban", ""),
                "iban_type": online.get("iban_type"),
                "swift": online.get("swift", ""),
                "transport_other": online.get("transport_other", ""),
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
            "start_date": start_date,
            "end_date": end_date,
            "place": place,
            "country": country,
            "type": None,
            "cost": None,
        },
        "attendees": attendees,
    }

    payload["objects"] = None
    payload["preview"] = {
        "event": {
            "eid": eid,
            "title": title,
            "start_date": _date_to_iso(start_date),
            "end_date": _date_to_iso(end_date),
            "place": place,
            "country": country,
            "type": None,
            "cost": None,
        },
        "participants": attendees,
        "participant_events": [],
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


def _date_to_iso(val: object) -> str:
    """Format datetime/date values to ISO-8601 strings; return empty string otherwise."""
    if isinstance(val, (datetime, date)):
        return val.date().isoformat() if isinstance(val, datetime) else val.isoformat()
    return str(val).strip() if val else ""


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
    A2: 'JUNE 23 - 27 - Opatija, CROATIA' -> (start_date, end_date, place, country)
    Month for end date assumed same as start (per your spec).
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
            normalized_country = translate(_normalize(raw_country), "en")
            lookup = _country_cid(normalized_country) or _country_cid(normalized_country.title())
            country_value = lookup or normalized_country

    return eid, title, start_date, end_date, place, country_value


# ============================
# DB helpers (read-only)
# ============================

def _country_cid(name: str) -> Optional[str]:
    """Return the country ``cid`` for ``name``, or ``None`` if not found."""
    if not name:
        return None
    try:
        doc = mongodb.collection('countries').find_one({'country': name})
        return doc.get('cid') if doc else None
    except Exception:
        # Avoid raising during preview; treat missing/DB errors as not found
        return None

def _participant_exists(name_display: str, country_name: str):
    """Existence check by normalized app name and representing country CID."""
    q = {"name": _to_app_display_name(name_display)}
    cid = _country_cid(country_name)
    if cid is not None:
        q["representing_country"] = cid
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
    custom_bundle = _load_custom_xml_objects(path)
    if custom_bundle:
        event_obj: Optional[Event] = custom_bundle.get("event")
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
    eid, title, start_date, end_date, place, country = _parse_event_header(a1, a2, year)

    # Event exist check (read-only)
    existing = mongodb.collection('events').find_one({"eid": eid})
    if existing:
        existing_start = existing.get('start_date') or existing.get('dateFrom')
        existing_place = existing.get('place') or existing.get('location', '')
        existing_country = existing.get('country')
        print(
            f"[EVENT] EXIST {eid}  title='{existing.get('title','')}' "
            f"start_date={existing_start} place='{existing_place}' country='{existing_country}'"
        )
    else:
        print(
            f"[EVENT] NEW   {eid}  title='{title}' start_date={start_date} "
            f"end_date={end_date} place='{place}' country='{country}'"
        )

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