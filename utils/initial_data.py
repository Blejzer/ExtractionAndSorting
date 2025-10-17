"""Utility for importing initial data from the Excel workbook.

This script was originally written for a much smaller participant model that
only tracked a name, position, grade and a MongoDB ``country_id`` reference.
The participant domain model has since evolved to expect country ``cid``
references and additional personal/contact fields. The import logic below now
normalises those columns and validates each participant row using a reduced
Pydantic model before inserting into MongoDB.
"""

from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any, Optional, Tuple

import pandas as pd

from config.database import mongodb
from domain.models.participant import Gender, Grade, Participant
from domain.models.event_participant import (
    DocType,
    EventParticipant,
    IbanType,
    Transport,
)

def as_dt_utc_midnight(v):
    if pd.isna(v):
        return datetime(1900, 1, 1, tzinfo=timezone.utc)   # or None if you prefer
    ts = pd.to_datetime(v, errors="coerce")
    if pd.isna(ts):
        return datetime(1900, 1, 1, tzinfo=timezone.utc)
    # ts is a pandas.Timestamp → convert to python datetime and make tz-aware
    dt = ts.to_pydatetime()
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def as_utc_or_none(value):
    if pd.isna(value):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    dt = ts.to_pydatetime()
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def as_date_or_none(value):
    dt = as_utc_or_none(value)
    return dt.date() if dt else None

def _split_location(value: str) -> Tuple[str, Optional[str]]:
    """Split a raw location string into place and country hint components."""

    if not value:
        return "", None

    text = str(value).strip()
    if not text:
        return "", None

    # Look for trailing country codes like "C033" or names after a comma/ dash.
    code_match = re.search(r"^(?P<place>.*?)[\s,;\-]*(?P<code>[A-Za-z]\d{3})$", text)
    if code_match:
        place = code_match.group("place").strip(" ,;-\t")
        country_hint = code_match.group("code").upper()
        return place or "", country_hint

    for separator in (",", " - ", " / ", "\\"):
        if separator in text:
            place_part, country_part = text.split(separator, 1)
            place = place_part.strip()
            country_hint = country_part.strip()
            return place, country_hint or None

    return text, None


def _normalize_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_gender(value: Any) -> Optional[Gender]:
    text = _normalize_str(value)
    if not text:
        return None
    lowered = text.lower()
    if lowered.startswith("m"):
        return Gender.male
    if lowered.startswith("f"):
        return Gender.female
    return None


def _normalize_grade(value: Any) -> Grade:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return Grade.NORMAL
    try:
        return Grade(int(value))
    except Exception:
        return Grade.NORMAL


def _match_enum_value(enum_cls, value: Any):
    text = _normalize_str(value)
    if not text:
        return None
    lowered = text.lower()
    for member in enum_cls:  # type: ignore[call-arg]
        if lowered == member.value.lower():
            return member
    return None


def _normalize_transport(value: Any) -> Optional[Transport]:
    return _match_enum_value(Transport, value)


def _normalize_doc_type(value: Any) -> Optional[DocType]:
    return _match_enum_value(DocType, value)


def _normalize_iban_type(value: Any) -> Optional[IbanType]:
    return _match_enum_value(IbanType, value)


def _normalize_bool(value: Any) -> Optional[bool]:
    text = _normalize_str(value).lower()
    if not text:
        return None
    if text in {"yes", "true", "1", "y"}:
        return True
    if text in {"no", "false", "0", "n"}:
        return False
    return None


def _split_multi_value(value: Any) -> list[str]:
    text = _normalize_str(value)
    if not text:
        return []
    parts = re.split(r"[;,]", text)
    return [part.strip() for part in parts if part.strip()]


def check_and_import_data():
    print("🔍 Checking for existing data...")

    try:
        db_conn = mongodb.db()
    except AttributeError:
        print("⚠️ Database connection not available. Skipping import.")
        return

    participants_col = db_conn['participants']
    events_col = db_conn['events']
    countries_col = db_conn['countries']
    participant_events_col = db_conn['participant_events']

    # Check if data already exists FIRST
    event_count_db = events_col.count_documents({})
    participant_count_db = participants_col.count_documents({})
    country_count_db = countries_col.count_documents({})

    print(
        f"Found {event_count_db} events, {participant_count_db} participants, and {country_count_db} countries in database")

    # If we have reasonable amounts of data, assume import is complete
    if event_count_db > 5 and participant_count_db > 10 and country_count_db > 5:
        print("✅ Data already exists. Skipping import.")
        return

    print("🚀 Starting data import...")

    # Only now try to load the Excel file
    try:
        xl = pd.ExcelFile("FILES/final_results.xlsx")
        df_participants = xl.parse("Participant")
        df_countries = xl.parse("Country")
        df_events = xl.parse("Events")

        # === Check if all events are already uploaded ===
        event_count_excel = df_events["Event"].nunique()
        if event_count_db >= event_count_excel:
            print("ℹ️ All events already exist. Skipping data import.")
            return

        print("📦 Importing fresh data...")

        # Clean slate
        participants_col.delete_many({})
        events_col.delete_many({})
        countries_col.delete_many({})
        participant_events_col.delete_many({})

        # === Load Events and Index by ID ===
        now = datetime.now(timezone.utc)
        event_lookup: dict[str, pd.Timestamp] = {}
        event_docs: dict[str, dict] = {}
        for idx, row in df_events.iterrows():
            eid = str(row.get("Event", "")).strip()
            if not eid:
                continue
            title = str(row.get("Title", "")).strip()
            location_value = row.get("Location", "")
            location_raw = "" if pd.isna(location_value) else str(location_value).strip()
            place, country_hint = _split_location(location_raw)
            start_dt = as_utc_or_none(row.get("Date From"))
            end_dt = as_utc_or_none(row.get("Date To"))

            country_value = country_hint
            if "Country Code" in df_events.columns and pd.notna(row.get("Country Code")):
                raw_country_code = str(row.get("Country Code")).strip()
                country_value = raw_country_code or country_value
            elif "Country" in df_events.columns and pd.notna(row.get("Country")):
                raw_country_name = str(row.get("Country")).strip()
                country_value = raw_country_name or country_value

            if country_value and re.fullmatch(r"[A-Za-z]\d{3}", country_value):
                country_value = country_value.upper()

            event_type = "Training"

            cost_value = None
            if "Cost" in df_events.columns:
                cost_value = pd.to_numeric(row.get("Cost"), errors="coerce")
                cost_value = float(cost_value) if pd.notna(cost_value) else None

            event_docs[eid] = {
                "eid": eid,
                "title": title,
                "start_date": start_dt,
                "end_date": end_dt,
                "place": place,
                "country": country_value,
                "type": event_type,
                "cost": cost_value,
                "participants": [],
                "created_at": now,
                "updated_at": now,
                "_audit": [
                    {
                        "ts": now,
                        "actor": "initial_import",
                        "field": "eid",
                        "from": None,
                        "to": eid,
                        "source": {"sheet": "Events", "row": int(idx) + 2},
                    }
                ],
            }

            if start_dt is not None:
                event_lookup[eid] = pd.Timestamp(start_dt)
            else:
                event_lookup[eid] = pd.Timestamp.min

        # === Insert Countries with stable CIDs ===
        country_lookup: dict[str, str] = {}
        for idx, row in df_countries.iterrows():
            name_value = row.get("Country", "")
            name = "" if pd.isna(name_value) else str(name_value).strip()

            cid_sources = (
                row.get("CID"),
                row.get("Country ID"),
                row.get("Country Code"),
            )
            cid_value = next((val for val in cid_sources if pd.notna(val)), "")
            cid = str(cid_value).strip().upper()

            if not cid:
                cid = f"C{idx + 1:03d}"

            country_doc = {"cid": cid, "country": name}

            iso_sources = (row.get("ISO"), row.get("ISO3"), row.get("ISO 3"))
            iso_value = next((val for val in iso_sources if pd.notna(val)), "")
            iso_val = str(iso_value).strip()
            if iso_val:
                country_doc["iso"] = iso_val

            countries_col.insert_one(country_doc)

            for key in {name.lower(), cid.lower(), iso_val.lower() if iso_val else ""}:
                if key:
                    country_lookup[key] = cid

        # Resolve event country codes if the lookup knows about them
        for eid, doc in event_docs.items():
            country_key = (doc.get("country") or "").lower()
            if not country_key:
                continue
            doc["country"] = country_lookup.get(country_key, doc.get("country"))

        # === Normalize Name (First + LAST) ===
        def normalize_name(full_name):
            name = full_name.strip()
            if name.isupper():
                return name  # Already normalized
            parts = name.split()
            if len(parts) == 1:
                return name  # Single name
            first_names = " ".join(parts[:-1])
            last_name = parts[-1].upper()
            return f"{first_names} {last_name}"

        next_country_number = len(df_countries) + 1

        def ensure_country_entry(value: Any) -> Optional[str]:
            nonlocal next_country_number
            text = _normalize_str(value)
            if not text:
                return None
            upper = text.upper()
            if re.fullmatch(r"[A-Za-z]\d{3}", upper):
                if upper.lower() not in country_lookup:
                    countries_col.insert_one({"cid": upper, "country": text})
                    country_lookup[upper.lower()] = upper
                return upper
            key = text.lower()
            cid = country_lookup.get(key)
            if cid:
                return cid
            cid = f"C{next_country_number:03d}"
            next_country_number += 1
            countries_col.insert_one({"cid": cid, "country": text})
            country_lookup[key] = cid
            country_lookup[cid.lower()] = cid
            return cid

        # === Deduplicate Participants Based on Normalized Name + Country ===
        participant_data: dict[tuple[str, str], dict[str, Any]] = {}
        participant_id_counter = 1
        event_participant_rows: dict[tuple[str, str], dict[str, Any]] = {}
        event_participant_sources: dict[tuple[str, str], dict[str, Any]] = {}

        def generate_pid(n: int) -> str:
            return f"P{n:04d}"

        def _merge_event_participant(
            existing: Optional[dict[str, Any]], new: dict[str, Any]
        ) -> dict[str, Any]:
            if existing is None:
                return new
            for key, value in new.items():
                if key in {"participant_id", "event_id"}:
                    existing[key] = value
                    continue
                if value is None:
                    continue
                if isinstance(value, str) and not value.strip():
                    continue
                existing[key] = value
            return existing

        for row_index, row in df_participants.iterrows():
            row_number = int(row_index) + 2
            raw_name = _normalize_str(row.get("Name"))
            if not raw_name:
                raise ValueError(f"Row {row_number}: missing mandatory field 'Name'")
            name = normalize_name(raw_name)

            rep_country_raw = (
                _normalize_str(row.get("Representing Country"))
                or _normalize_str(row.get("Country"))
                or _normalize_str(row.get("Country Code"))
            )
            rep_cid = ensure_country_entry(rep_country_raw)
            if not rep_cid:
                raise ValueError(
                    f"Row {row_number}: missing mandatory field 'Representing Country'"
                )

            gender = _normalize_gender(row.get("Gender"))
            if not gender:
                raise ValueError(f"Row {row_number}: missing mandatory field 'Gender'")

            grade = _normalize_grade(row.get("Grade"))
            dob = as_dt_utc_midnight(row.get("DOB"))
            pob_raw = (
                _normalize_str(row.get("POB"))
                or _normalize_str(row.get("Place of Birth"))
            )
            pob = pob_raw or rep_country_raw or "Unknown"

            birth_country_raw = (
                _normalize_str(row.get("Birth Country"))
                or _normalize_str(row.get("Birth Country Code"))
            )
            birth_cid = ensure_country_entry(birth_country_raw) or rep_cid

            email = _normalize_str(row.get("Email"))
            phone = _normalize_str(row.get("Phone"))
            position = _normalize_str(row.get("Position"))
            organization = _normalize_str(row.get("Organization"))
            unit = _normalize_str(row.get("Unit"))
            rank = _normalize_str(row.get("Rank"))
            diet = _normalize_str(row.get("Diet Restrictions") or row.get("Dietary Restrictions"))
            bio = _normalize_str(row.get("Bio") or row.get("Bio Short") or row.get("Biography"))
            bank_name = _normalize_str(row.get("Bank Name"))
            iban = _normalize_str(row.get("IBAN"))
            iban_type = _normalize_iban_type(row.get("IBAN Type"))
            swift = _normalize_str(row.get("SWIFT"))
            intl_authority = _normalize_bool(row.get("International Authority") or row.get("Intl Authority"))

            transportation = _normalize_transport(row.get("Transportation"))
            transport_other = _normalize_str(row.get("Transportation Other"))

            travelling_from = _normalize_str(row.get("Travelling From") or row.get("Traveling From"))
            returning_to = _normalize_str(row.get("Returning To"))
            travelling_from_value = (
                travelling_from or rep_country_raw or pob or "Unknown"
            )
            returning_to_value = (
                returning_to or travelling_from_value or rep_country_raw or "Unknown"
            )

            travel_doc_type = _normalize_doc_type(
                row.get("Travel Doc Type") or row.get("Travel Document Type")
            )
            travel_doc_type_other = _normalize_str(row.get("Travel Doc Type Other"))

            travel_doc_issue_date = as_date_or_none(
                row.get("Travel Doc Issue Date") or row.get("Travel Document Issue Date")
            )
            travel_doc_expiry_date = as_date_or_none(
                row.get("Travel Doc Expiry Date") or row.get("Travel Document Expiry Date")
            )
            travel_doc_issued_by = _normalize_str(
                row.get("Travel Doc Issued By") or row.get("Travel Document Issued By")
            ) or None

            requires_visa_hr = _normalize_bool(
                row.get("Requires Visa HR")
                or row.get("Visa Required")
                or row.get("Visa HR")
                or row.get("Requires Visa")
                or row.get("Do you require Visa to travel to Croatia")
            )
            if requires_visa_hr is None:
                requires_visa_hr = False

            citizenship_tokens = _split_multi_value(
                row.get("Citizenships") or row.get("Citizenship")
            )
            citizenships: list[str] = []
            for token in citizenship_tokens:
                cid_value = ensure_country_entry(token)
                if cid_value and cid_value not in citizenships:
                    citizenships.append(cid_value)
            if not citizenships and rep_cid:
                citizenships.append(rep_cid)

            event_id = _normalize_str(row.get("Event"))
            event_date = event_lookup.get(event_id, pd.Timestamp.min)

            source_table = _normalize_str(row.get("Table"))
            audit_source = {"sheet": "Participants", "row": int(row_index) + 2}
            if source_table:
                audit_source["table"] = source_table

            dedup_key = (name.lower(), rep_cid)

            participant_record = {
                "pid": "",
                "name": name,
                "position": position or None,
                "grade": grade,
                "representing_country": rep_cid,
                "gender": gender,
                "dob": dob,
                "pob": pob,
                "birth_country": birth_cid,
                "citizenships": citizenships,
                "email": email or None,
                "phone": phone or None,
                "diet_restrictions": diet or None,
                "organization": organization or None,
                "unit": unit or None,
                "rank": rank or None,
                "intl_authority": intl_authority,
                "bio_short": bio or None,
                "created_at": now,
                "updated_at": now,
                "audit_source": audit_source,
                "latest_date": event_date,
            }

            if dedup_key not in participant_data:
                pid = generate_pid(participant_id_counter)
                participant_id_counter += 1
                participant_record["pid"] = pid
                participant_data[dedup_key] = participant_record
            else:
                entry = participant_data[dedup_key]
                for cid_value in participant_record.get("citizenships", []):
                    if cid_value and cid_value not in entry.get("citizenships", []):
                        entry.setdefault("citizenships", []).append(cid_value)
                if event_date > entry.get("latest_date", pd.Timestamp.min):
                    entry.update({
                        "position": participant_record["position"],
                        "grade": participant_record["grade"],
                        "gender": participant_record["gender"],
                        "dob": participant_record["dob"],
                        "pob": participant_record["pob"],
                        "birth_country": participant_record["birth_country"],
                        "email": participant_record["email"],
                        "phone": participant_record["phone"],
                        "diet_restrictions": participant_record["diet_restrictions"],
                        "organization": participant_record["organization"],
                        "unit": participant_record["unit"],
                        "rank": participant_record["rank"],
                        "intl_authority": participant_record["intl_authority"],
                        "bio_short": participant_record["bio_short"],
                    })
                    entry["latest_date"] = event_date
                    entry["audit_source"] = audit_source
                entry["updated_at"] = now

            pid = participant_data[dedup_key]["pid"]

            if event_id:
                payload = {
                    "event_id": event_id,
                    "participant_id": pid,
                    "transportation": transportation,
                    "transport_other": transport_other or None,
                    "requires_visa_hr": requires_visa_hr,
                    "travelling_from": travelling_from_value,
                    "returning_to": returning_to_value,
                    "travel_doc_type": travel_doc_type,
                    "travel_doc_type_other": travel_doc_type_other or None,
                    "travel_doc_issue_date": travel_doc_issue_date,
                    "travel_doc_expiry_date": travel_doc_expiry_date,
                    "travel_doc_issued_by": travel_doc_issued_by,
                    "bank_name": bank_name or None,
                    "iban": iban or None,
                    "iban_type": iban_type,
                    "swift": swift or None,
                }
                key = (pid, event_id)
                event_participant_rows[key] = _merge_event_participant(
                    event_participant_rows.get(key), payload
                )
                event_participant_sources[key] = audit_source

        # === Insert Participants and Events ===
        event_participants: dict[str, set[str]] = {eid: set() for eid in event_docs}

        for pdata in participant_data.values():
            pdata.pop("latest_date", None)
            audit_source = pdata.pop("audit_source", {}) or {}

            if not pdata.get("citizenships"):
                pdata["citizenships"] = None
            else:
                seen_citizenships: list[str] = []
                for cid_value in pdata["citizenships"]:
                    if cid_value and cid_value not in seen_citizenships:
                        seen_citizenships.append(cid_value)
                pdata["citizenships"] = seen_citizenships or None

            try:
                participant = Participant(**pdata)
            except Exception as exc:
                raise ValueError(
                    f"Row {audit_source.get('row', '?')}: unable to create participant {pdata.get('pid')}: {exc}"
                ) from exc

            participant_doc = participant.to_mongo()

            grade_value = participant_doc.get("grade")
            source_meta = {k: v for k, v in audit_source.items() if v not in (None, "")}
            audit_entry = {
                "ts": participant_doc.get("created_at", now),
                "actor": "import",
                "field": "grade",
                "from": None,
                "to": grade_value,
            }
            if source_meta:
                audit_entry["source"] = source_meta

            participant_doc.setdefault("_audit", []).append(audit_entry)
            participants_col.insert_one(participant_doc)

        for key, payload in event_participant_rows.items():
            pid, eid = key
            if not eid:
                continue

            event_payload = dict(payload)
            event_payload["transportation"] = event_payload.get("transportation") or Transport.other
            if (
                event_payload["transportation"] == Transport.other
                and not event_payload.get("transport_other")
            ):
                event_payload["transport_other"] = "Unspecified"

            event_payload["travel_doc_type"] = (
                event_payload.get("travel_doc_type") or DocType.passport
            )
            if (
                event_payload["travel_doc_type"] == DocType.other
                and not event_payload.get("travel_doc_type_other")
            ):
                event_payload["travel_doc_type_other"] = "Unspecified"

            event_payload["travelling_from"] = (
                event_payload.get("travelling_from") or "Unknown"
            )
            event_payload["returning_to"] = (
                event_payload.get("returning_to")
                or event_payload["travelling_from"]
                or "Unknown"
            )
            event_payload["requires_visa_hr"] = bool(
                event_payload.get("requires_visa_hr", False)
            )

            source_meta = event_participant_sources.get(key, {}) or {}

            try:
                event_participant = EventParticipant(**event_payload)
            except Exception as exc:
                raise ValueError(
                    f"Row {source_meta.get('row', '?')}: unable to create event participant {pid}/{eid}: {exc}"
                ) from exc

            participant_events_col.insert_one(event_participant.to_mongo())
            if eid in event_participants:
                event_participants[eid].add(pid)

        print(f"✅ Import complete: {len(participant_data)} unique participants added.")

        # Insert events with participant rosters
        for eid, doc in event_docs.items():
            doc["participants"] = sorted(event_participants.get(eid, set()))
            events_col.insert_one(doc)
        print(f"✅ Imported {len(event_docs)} events.")

    except FileNotFoundError:
        print("❌ Excel file not found at 'FILES/final_results.xlsx'")
        print("💡 Import skipped - using existing database data")
        return
    except Exception as e:
        print(f"❌ Error during data import: {e}")
        print("💡 Import failed - using existing database data")
        return
