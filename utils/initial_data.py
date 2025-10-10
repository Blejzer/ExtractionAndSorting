"""Utility for importing initial data from the Excel workbook.

This script was originally written for a much smaller participant model that
only tracked a name, position, grade and a MongoDB ``country_id`` reference.
The participant domain model has since evolved to expect country ``cid``
references and additional personal/contact fields. The import logic below now
normalises those columns and validates each participant row using a reduced
Pydantic model before inserting into MongoDB.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd
from pydantic import BaseModel, EmailStr

from config.database import mongodb
from domain.models.participant import Grade, Gender
from datetime import datetime, timezone

class ParticipantRow(BaseModel):
    """Light-weight validation model for imported participants."""

    pid: str
    name: str
    position: str
    grade: Grade = Grade.NORMAL
    representing_country: str
    gender: Gender
    dob: datetime
    pob: str
    birth_country: str
    email: Optional[EmailStr] = None
    phone: str

    def to_mongo(self) -> dict:
        data = self.model_dump()
        # Store enum raw values for MongoDB
        data["grade"] = data["grade"].value
        data["gender"] = data["gender"].value
        return data

def as_dt_utc_midnight(v):
    if pd.isna(v):
        return datetime(1900, 1, 1, tzinfo=timezone.utc)   # or None if you prefer
    ts = pd.to_datetime(v, errors="coerce")
    if pd.isna(ts):
        return datetime(1900, 1, 1, tzinfo=timezone.utc)
    # ts is a pandas.Timestamp → convert to python datetime and make tz-aware
    dt = ts.to_pydatetime()
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

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
        event_lookup = {}
        for _, row in df_events.iterrows():
            eid = row["Event"].strip()
            title = row["Title"].strip() if pd.notna(row["Title"]) else ""
            location = row["Location"].strip() if pd.notna(row["Location"]) else ""
            date_from = pd.to_datetime(row["Date From"], errors="coerce")
            date_to = pd.to_datetime(row["Date To"], errors="coerce")

            event_doc = {
                "eid": eid,
                "title": title,
                "location": location,
            }
            if pd.notna(date_from): event_doc["dateFrom"] = date_from
            if pd.notna(date_to): event_doc["dateTo"] = date_to

            events_col.insert_one(event_doc)
            event_lookup[eid] = date_from if pd.notna(date_from) else pd.Timestamp.min

        # === Insert Countries with stable CIDs ===
        country_lookup: dict[str, str] = {}
        for idx, row in df_countries.iterrows():
            name = str(row["Country"]).strip()
            cid = f"c{idx + 1:03d}"
            countries_col.insert_one({"cid": cid, "country": name})
            country_lookup[name.lower()] = cid

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

        for _, row in df_participants.iterrows():
            raw_name = str(row.get("Name", "")).strip()
            if not raw_name:
                continue
            name = normalize_name(raw_name)
            position = str(row.get("Position", "")).strip()
            country_name = str(row.get("Country", "")).strip()
            gender_str = str(row.get("Gender", "")).strip().lower()
            dob_val = pd.to_datetime(row.get("DOB"), errors="coerce")
            pob = str(row.get("POB", "")).strip()
            birth_country_name = str(row.get("Birth Country", "")).strip()
            email = str(row.get("Email", "")).strip()
            phone = str(row.get("Phone", "")).strip()
            grade_val = row.get("Grade")
            event_id = str(row.get("Event", "")).strip()
            event_date = event_lookup.get(event_id, pd.Timestamp.min)

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
                gender = Gender(gender_str) if gender_str else Gender.male
            except ValueError:
                gender = Gender.male

            try:
                grade = Grade(int(grade_val)) if pd.notna(grade_val) else Grade.NORMAL
            except Exception:
                grade = Grade.NORMAL
            # before:
            # dob = dob_val.date() if pd.notna(dob_val) else date(1900, 1, 1)

            # after:
            dob = as_dt_utc_midnight(dob_val)

            dedup_key = (name.lower(), rep_cid)

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
                    "dob": dob,
                    "pob": pob,
                    "birth_country": birth_cid,
                    "email": email,
                    "phone": phone,
                    "latest_date": event_date,
                    "events": [event_id],
                }
            else:
                entry = participant_data[dedup_key]
                entry["events"].append(event_id)
                if event_date > entry["latest_date"]:
                    entry["latest_date"] = event_date
                    entry["position"] = position
                    entry["grade"] = grade

        # === Insert Participants and Events ===
        for pdata in participant_data.values():
            try:
                participant_doc = ParticipantRow(**{
                    "pid": pdata["pid"],
                    "name": pdata["name"],
                    "position": pdata["position"],
                    "grade": pdata["grade"],
                    "representing_country": pdata["representing_country"],
                    "gender": pdata["gender"],
                    "dob": pdata["dob"],
                    "pob": pdata["pob"],
                    "birth_country": pdata["birth_country"],
                    "email": pdata["email"] or None,
                    "phone": pdata["phone"],
                }).to_mongo()
            except Exception as exc:
                print(f"Skipping participant {pdata['pid']}: {exc}")
                continue

            participants_col.insert_one(participant_doc)

            for eid in set(pdata["events"]):
                db_conn["participant_events"].insert_one({
                    "participant_id": pdata["pid"],
                    "event_id": eid,
                })

        print(f"✅ Import complete: {len(participant_data)} unique participants added.")

    except FileNotFoundError:
        print("❌ Excel file not found at 'FILES/final_results.xlsx'")
        print("💡 Import skipped - using existing database data")
        return
    except Exception as e:
        print(f"❌ Error during data import: {e}")
        print("💡 Import failed - using existing database data")
        return