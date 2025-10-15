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
from typing import Optional, Tuple, Any

import pandas as pd
from pydantic import BaseModel, EmailStr, Field, field_validator, ConfigDict

from config.database import mongodb
from domain.models.participant import Grade, Gender, Transport, DocType, IbanType

class ParticipantRow(BaseModel):
    """Light-weight validation model for imported participants."""

    model_config = ConfigDict(use_enum_values=True, populate_by_name=True)

    pid: str
    name: str
    representing_country: str
    gender: Gender

    grade: Grade = Grade.NORMAL
    position: str = ""
    dob: Optional[datetime] = None
    pob: str = ""
    birth_country: str = ""
    citizenships: Optional[list[str]] = None

    email: Optional[EmailStr] = None
    phone: Optional[str] = None

    transportation: Optional[Transport] = None
    transport_other: Optional[str] = None
    travel_doc_type: Optional[DocType] = None
    travel_doc_type_other: Optional[str] = None
    travel_doc_issue_date: Optional[datetime] = None
    travel_doc_expiry_date: Optional[datetime] = None
    travel_doc_issued_by: Optional[str] = None

    travelling_from: Optional[str] = None
    returning_to: Optional[str] = None

    diet_restrictions: Optional[str] = None
    organization: Optional[str] = None
    unit: Optional[str] = None
    rank: Optional[str] = None
    intl_authority: Optional[bool] = None
    bio_short: Optional[str] = None

    bank_name: Optional[str] = None
    iban: Optional[str] = None
    iban_type: Optional[IbanType] = None
    swift: Optional[str] = None

    created_at: datetime
    updated_at: datetime
    audit: list[dict[str, Any]] = Field(default_factory=list, alias="_audit")

    @field_validator("email", "phone", mode="before")
    @classmethod
    def _blank_to_none(cls, value):
        if value is None:
            return None
        if isinstance(value, float) and pd.isna(value):
            return None
        text = str(value).strip()
        return text or None

    def to_mongo(self) -> dict:
        return self.model_dump(by_alias=True, exclude_none=True)

def as_dt_utc_midnight(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    ts = pd.to_datetime(v, errors="coerce")
    if pd.isna(ts):
        return None
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


def _parse_enum(enum_cls, value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    for member in enum_cls:
        if text.lower() in {member.value.lower(), member.name.lower()}:
            return member
    return None


def _parse_bool(value) -> Optional[bool]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _parse_citizenships(value) -> Optional[list[str]]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, list):
        items = [str(item).strip() for item in value if str(item).strip()]
    else:
        parts = re.split(r"[;,]", str(value))
        items = [part.strip() for part in parts if part.strip()]
    return items or None

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


def _clean_str(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


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
        db_conn["participant_events"].delete_many({})

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

        # === Deduplicate Participants Based on Normalized Name + Country ===
        participant_data: dict[tuple[str, str], dict] = {}
        participant_id_counter = 1

        def generate_pid(n: int) -> str:
            return f"P{n:04d}"

        import_timestamp = datetime.now(timezone.utc)

        for idx, row in df_participants.iterrows():
            raw_name = str(row.get("Name", "")).strip()
            if not raw_name:
                continue
            name = normalize_name(raw_name)
            position = _clean_str(row.get("Position"))
            country_name = _clean_str(row.get("Country"))
            if not country_name:
                print(f"Skipping participant without representing country: {name}")
                continue
            gender_str = _clean_str(row.get("Gender")).lower()
            dob_val = row.get("DOB")
            pob = _clean_str(row.get("POB"))
            birth_country_name = _clean_str(row.get("Birth Country"))
            email = _clean_str(row.get("Email"))
            phone = _clean_str(row.get("Phone"))
            grade_val = row.get("Grade")
            event_id = _clean_str(row.get("Event"))
            event_date = event_lookup.get(event_id, pd.Timestamp.min)

            travel_doc_type = _parse_enum(DocType, row.get("Travel Document Type"))
            travel_doc_type_other = _clean_str(row.get("Travel Document Type Other"))
            travel_doc_issue_date = as_utc_or_none(row.get("Travel Document Issue Date"))
            travel_doc_expiry_date = as_utc_or_none(row.get("Travel Document Expiry Date"))
            travel_doc_issued_by = _clean_str(row.get("Travel Document Issued By"))

            transportation = _parse_enum(
                Transport,
                row.get("Transportation")
                if "Transportation" in row
                else row.get("Transport"),
            )
            transport_other = _clean_str(row.get("Transportation Other"))
            travelling_from = _clean_str(row.get("Travelling From"))
            returning_to = _clean_str(row.get("Returning To"))

            diet_restrictions = _clean_str(row.get("Diet Restrictions"))
            organization = _clean_str(row.get("Organization"))
            unit = _clean_str(row.get("Unit"))
            rank = _clean_str(row.get("Rank"))
            intl_authority = _parse_bool(row.get("Intl Authority"))
            bio_short = _clean_str(row.get("Bio"))

            bank_name = _clean_str(row.get("Bank Name"))
            iban = _clean_str(row.get("IBAN"))
            iban_type = _parse_enum(IbanType, row.get("IBAN Type"))
            swift = _clean_str(row.get("SWIFT"))

            citizenships = _parse_citizenships(row.get("Citizenships"))

            rep_cid = country_lookup.get(country_name.lower())
            if not rep_cid:
                rep_cid = f"c{len(country_lookup) + 1:03d}"
                countries_col.insert_one({"cid": rep_cid, "country": country_name})
                country_lookup[country_name.lower()] = rep_cid

            birth_cid = country_lookup.get(birth_country_name.lower()) if birth_country_name else None
            if birth_country_name and not birth_cid:
                birth_cid = f"c{len(country_lookup) + 1:03d}"
                countries_col.insert_one({"cid": birth_cid, "country": birth_country_name})
                country_lookup[birth_country_name.lower()] = birth_cid
            birth_cid = birth_cid or rep_cid

            try:
                gender = Gender(gender_str) if gender_str else None
            except ValueError:
                gender = None
            if gender is None:
                print(f"Skipping participant without valid gender: {name}")
                continue

            try:
                grade = Grade(int(grade_val)) if pd.notna(grade_val) else Grade.NORMAL
            except Exception:
                grade = Grade.NORMAL

            dedup_key = (name.lower(), rep_cid)

            if not citizenships:
                citizenships = [rep_cid]

            pob_value = pob or ""
            birth_country_value = birth_cid or rep_cid

            if dedup_key not in participant_data:
                pid = generate_pid(participant_id_counter)
                participant_id_counter += 1
                participant_data[dedup_key] = {
                    "pid": pid,
                    "name": name,
                    "position": position,
                    "grade": grade,
                    "representing_country": rep_cid,
                    "gender": gender,
                    "dob": as_dt_utc_midnight(dob_val),
                    "pob": pob_value,
                    "birth_country": birth_country_value,
                    "citizenships": citizenships,
                    "email": email,
                    "phone": phone,
                    "latest_date": event_date,
                    "events": [event_id],
                    "transportation": transportation,
                    "transport_other": transport_other,
                    "travel_doc_type": travel_doc_type,
                    "travel_doc_type_other": travel_doc_type_other,
                    "travel_doc_issue_date": travel_doc_issue_date,
                    "travel_doc_expiry_date": travel_doc_expiry_date,
                    "travel_doc_issued_by": travel_doc_issued_by,
                    "travelling_from": travelling_from,
                    "returning_to": returning_to,
                    "diet_restrictions": diet_restrictions,
                    "organization": organization,
                    "unit": unit,
                    "rank": rank,
                    "intl_authority": intl_authority,
                    "bio_short": bio_short,
                    "bank_name": bank_name,
                    "iban": iban,
                    "iban_type": iban_type,
                    "swift": swift,
                    "source": {"sheet": "Participant", "row": int(idx) + 2},
                }
            else:
                entry = participant_data[dedup_key]
                entry["events"].append(event_id)
                if event_date > entry["latest_date"]:
                    entry["latest_date"] = event_date
                    entry["position"] = position
                    entry["grade"] = grade
                    entry["transportation"] = transportation or entry.get("transportation")
                    entry["transport_other"] = transport_other or entry.get("transport_other")
                    entry["travel_doc_type"] = travel_doc_type or entry.get("travel_doc_type")
                    entry["travel_doc_type_other"] = travel_doc_type_other or entry.get("travel_doc_type_other")
                    entry["travel_doc_issue_date"] = travel_doc_issue_date or entry.get("travel_doc_issue_date")
                    entry["travel_doc_expiry_date"] = travel_doc_expiry_date or entry.get("travel_doc_expiry_date")
                    entry["travel_doc_issued_by"] = travel_doc_issued_by or entry.get("travel_doc_issued_by")
                    entry["travelling_from"] = travelling_from or entry.get("travelling_from")
                    entry["returning_to"] = returning_to or entry.get("returning_to")
                    entry["diet_restrictions"] = diet_restrictions or entry.get("diet_restrictions")
                    entry["organization"] = organization or entry.get("organization")
                    entry["unit"] = unit or entry.get("unit")
                    entry["rank"] = rank or entry.get("rank")
                    entry["intl_authority"] = (intl_authority if intl_authority is not None else entry.get("intl_authority"))
                    entry["bio_short"] = bio_short or entry.get("bio_short")
                    entry["bank_name"] = bank_name or entry.get("bank_name")
                    entry["iban"] = iban or entry.get("iban")
                    entry["iban_type"] = iban_type or entry.get("iban_type")
                    entry["swift"] = swift or entry.get("swift")

                if not entry.get("citizenships") and citizenships:
                    entry["citizenships"] = citizenships

        # === Insert Participants and Events ===
        event_participants: dict[str, set[str]] = {eid: set() for eid in event_docs}

        for pdata in participant_data.values():
            try:
                audit_entries = [
                    {
                        "ts": import_timestamp,
                        "actor": "initial_import",
                        "field": "grade",
                        "from": None,
                        "to": (pdata["grade"].value if isinstance(pdata["grade"], Grade) else pdata["grade"]),
                        "source": pdata.get("source"),
                    }
                ]

                participant_doc = ParticipantRow(
                    pid=pdata["pid"],
                    name=pdata["name"],
                    representing_country=pdata["representing_country"],
                    gender=pdata["gender"],
                    grade=pdata["grade"],
                    position=pdata.get("position", ""),
                    dob=pdata.get("dob"),
                    pob=pdata.get("pob", ""),
                    birth_country=pdata.get("birth_country", ""),
                    citizenships=pdata.get("citizenships"),
                    email=pdata.get("email"),
                    phone=pdata.get("phone"),
                    transportation=pdata.get("transportation"),
                    transport_other=pdata.get("transport_other"),
                    travel_doc_type=pdata.get("travel_doc_type"),
                    travel_doc_type_other=pdata.get("travel_doc_type_other"),
                    travel_doc_issue_date=pdata.get("travel_doc_issue_date"),
                    travel_doc_expiry_date=pdata.get("travel_doc_expiry_date"),
                    travel_doc_issued_by=pdata.get("travel_doc_issued_by"),
                    travelling_from=pdata.get("travelling_from"),
                    returning_to=pdata.get("returning_to"),
                    diet_restrictions=pdata.get("diet_restrictions"),
                    organization=pdata.get("organization"),
                    unit=pdata.get("unit"),
                    rank=pdata.get("rank"),
                    intl_authority=pdata.get("intl_authority"),
                    bio_short=pdata.get("bio_short"),
                    bank_name=pdata.get("bank_name"),
                    iban=pdata.get("iban"),
                    iban_type=pdata.get("iban_type"),
                    swift=pdata.get("swift"),
                    created_at=import_timestamp,
                    updated_at=import_timestamp,
                    audit=audit_entries,
                ).to_mongo()
            except Exception as exc:
                print(f"Skipping participant {pdata['pid']}: {exc}")
                continue

            participants_col.insert_one(participant_doc)

            for eid in set(pdata["events"]):
                db_conn["participant_events"].insert_one({
                    "participant_id": pdata["pid"],
                    "event_id": eid,
                })
                if eid in event_participants:
                    event_participants[eid].add(pdata["pid"])

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
