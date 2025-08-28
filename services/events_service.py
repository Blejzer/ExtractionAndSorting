# services/events_service.py
from typing import Dict, Any, Iterable, List, Tuple
from flask_paginate import get_page_args
from config.database import mongodb_connection

def paginate_list(items: List[dict]) -> Tuple[List[dict], int, int, int]:
    page, per_page, offset = get_page_args(page_parameter="page", per_page_parameter="per_page")
    per_page = 10
    return items[offset:offset + per_page], page, per_page, len(items)

def get_events(search_query: str | None, sort_field: str = "dateFrom", sort_direction: int = -1):
    """
    Return a cursor of events with optional search and sort.
    sort_field can be one of: 'eid', 'title', 'location', 'dateFrom', 'dateTo'
    """
    query: Dict[str, Any] = {}
    if search_query:
        query = {
            "$or": [
                {"eid": {"$regex": search_query, "$options": "i"}},
                {"title": {"$regex": search_query, "$options": "i"}},
                {"location": {"$regex": search_query, "$options": "i"}},
            ]
        }

    # default safe fallback for sort_field
    sort_field = sort_field or "dateFrom"
    return mongodb_connection.events.find(query).sort(sort_field, sort_direction)

def get_participant_counts_for_events(event_ids: List[str]) -> Dict[str, int]:
    """
    Returns a dict {eid: count_of_participants}.
    Uses participant_events collection (event_id, participant_id).
    """
    if not event_ids:
        return {}

    pe = mongodb_connection.db["participant_events"]
    pipeline = [
        {"$match": {"event_id": {"$in": event_ids}}},
        {"$group": {"_id": "$event_id", "count": {"$sum": 1}}},
    ]
    counts: Dict[str, int] = {}
    for row in pe.aggregate(pipeline):
        counts[row["_id"]] = row["count"]
    return counts

def get_event_by_eid(eid: str) -> dict | None:
    return mongodb_connection.events.find_one({"eid": eid})

def get_event_participants(eid: str, sort_field: str = "country", sort_direction: int = 1) -> list[dict]:
    """
    Fetch participants who attended this event.
    """

    pe = mongodb_connection.db["participant_events"]
    part_col = mongodb_connection.participants
    countries_col = mongodb_connection.countries

    links = pe.find({"event_id": eid})
    pids = [link["participant_id"] for link in links]

    participants = list(part_col.find({"pid": {"$in": pids}}))

    # Attach country names
    countries_map = {c["_id"]: c["country"] for c in countries_col.find()}
    for p in participants:
        p["country"] = countries_map.get(p.get("country_id"), "")

    # Sort
    participants.sort(key=lambda p: p.get(sort_field, ""), reverse=(sort_direction == -1))
    return participants
