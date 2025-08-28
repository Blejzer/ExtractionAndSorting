# services/participants_service.py
from typing import Dict, Any, Iterable, List, Tuple
from flask_paginate import get_page_args
from config.database import mongodb_connection

def get_participants(search_query: str | None, sort_field: str = "pid", sort_direction: int = 1):
    query: Dict[str, Any] = {}
    if search_query:
        query = {
            "$or": [
                {"pid": {"$regex": search_query, "$options": "i"}},
                {"name": {"$regex": search_query, "$options": "i"}},
                {"position": {"$regex": search_query, "$options": "i"}},
                {"grade": {"$regex": search_query, "$options": "i"}}
            ]
        }
    return mongodb_connection.participants.find(query).sort(sort_field or "name", sort_direction)

def get_countries_map() -> Dict[Any, str]:
    countries_col = mongodb_connection.countries
    # your schema uses {"country": <name>}
    return {c["_id"]: c["country"] for c in countries_col.find()}

def attach_country_names(rows: Iterable[dict], countries_map: Dict[Any, str]) -> List[dict]:
    out: List[dict] = []
    for p in rows:
        p = dict(p)  # copy
        p["country"] = countries_map.get(p.get("country_id"), "")
        out.append(p)
    return out

def paginate_list(items: List[dict]) -> Tuple[List[dict], int, int, int]:
    page, per_page, offset = get_page_args(page_parameter="page", per_page_parameter="per_page")
    per_page = 10
    return items[offset:offset + per_page], page, per_page, len(items)

def get_participant_by_pid(pid: str) -> dict | None:
    # You saved participants with _id=pid; be consistent:
    return mongodb_connection.participants.find_one({"pid": pid})

def update_participant(pid: str, name: str, position: str, grade: str) -> int:
    res = mongodb_connection.participants.update_one(
        {"pid": pid},
        {"$set": {"name": name, "position": position, "grade": grade}}
    )
    return res.modified_count

def get_participant_country_name(participant: dict) -> str:
    countries = mongodb_connection.countries
    c = countries.find_one({"_id": participant["country_id"]})
    return c["country"] if c else "Unknown"

def get_events_for_participant(pid: str) -> List[dict]:
    pe = mongodb_connection.db["participant_events"]
    events_col = mongodb_connection.events
    links = pe.find({"participant_id": pid})
    event_ids = [e["event_id"] for e in links]
    evts = list(events_col.find({"eid": {"$in": event_ids}}))
    evts.sort(key=lambda e: e.get("dateFrom", ""), reverse=True)
    return evts
