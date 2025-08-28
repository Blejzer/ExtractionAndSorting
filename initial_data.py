import pandas as pd
from config.database import mongodb_connection


def check_and_import_data():
    print("🚀 Starting data import...")

    participants_col = mongodb_connection.participants
    events_col = mongodb_connection.events
    countries_col = mongodb_connection.countries
    participant_events_col = mongodb_connection.db["participant_events"]

    # Load Excel
    xl = pd.ExcelFile("FILES/final_results.xlsx")
    df_participants = xl.parse("Participant")
    df_countries = xl.parse("Country")
    df_events = xl.parse("Events")

    # === Check if all events are already uploaded ===
    event_count_excel = df_events["Event"].nunique()
    event_count_db = events_col.count_documents({})
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

    # === Insert Countries ===
    country_lookup = {}
    for _, row in df_countries.iterrows():
        name = row["Country"].strip()
        doc = {"country": name}
        cid = countries_col.insert_one(doc).inserted_id
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
    participant_data = {}
    participant_id_counter = 1

    def generate_pid(n):
        return f"P{n:04d}"

    for _, row in df_participants.iterrows():
        raw_name = str(row["Name"]).strip()
        name = normalize_name(raw_name)
        position = str(row["Position"]).strip()
        country = str(row["Country"]).strip()
        grade = str(row["Grade"]).strip() if pd.notna(row["Grade"]) else ""
        event_id = str(row["Event"]).strip()
        event_date = event_lookup.get(event_id, pd.Timestamp.min)

        dedup_key = (name.lower(), country.lower())

        if dedup_key not in participant_data:
            pid = generate_pid(participant_id_counter)
            participant_id_counter += 1
            participant_data[dedup_key] = {
                "pid": pid,
                "name": name,
                "country": country,
                "latest_date": event_date,
                "position": position,
                "grade": grade,
                "events": [event_id]
            }
        else:
            entry = participant_data[dedup_key]
            entry["events"].append(event_id)
            if event_date > entry["latest_date"]:
                entry["latest_date"] = event_date
                entry["position"] = position
                entry["grade"] = grade

    # === Insert Participants and Events ===
    for (name_key, country_key), pdata in participant_data.items():
        country_id = country_lookup.get(country_key)
        if not country_id:
            country_id = countries_col.insert_one({"country": pdata["country"]}).inserted_id
            country_lookup[country_key] = country_id

        participant_doc = {
            "pid": pdata["pid"],
            "name": pdata["name"],
            "position": pdata["position"],
            "grade": pdata["grade"],
            "country_id": country_id
        }
        participants_col.insert_one(participant_doc)

        for eid in set(pdata["events"]):
            participant_events_col.insert_one({
                "participant_id": pdata["pid"],
                "event_id": eid
            })

    print(f"✅ Import complete: {len(participant_data)} unique participants added.")
