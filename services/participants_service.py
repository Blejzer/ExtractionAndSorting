# services/participants_service.py
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple, Optional
from flask_paginate import get_page_args
from config.database import mongodb_connection


def get_participants(
    search_query: Optional[str],
    sort_field: str = "pid",
    sort_direction: int = 1,
):
    """Return a MongoDB cursor over participants, filtered & sorted.

    Args:
        search_query: Case-insensitive text to match in pid, name, position, or grade.
        sort_field: Field name to sort by (e.g., "pid", "name", "position", "grade").
        sort_direction: 1 for ascending, -1 for descending.

    Returns:
        A PyMongo cursor (lazy) for the matching participant documents.
    """
    query: Dict[str, Any] = {}
    if search_query:
        query = {
            "$or": [
                {"pid": {"$regex": search_query, "$options": "i"}},
                {"name": {"$regex": search_query, "$options": "i"}},
                {"position": {"$regex": search_query, "$options": "i"}},
                {"grade": {"$regex": search_query, "$options": "i"}},
            ]
        }

    # Defensive default if an empty sort field sneaks in
    sort_field = sort_field or "name"
    return mongodb_connection.participants.find(query).sort(sort_field, sort_direction)


def get_countries_map() -> Dict[Any, str]:
    """Load all countries once and return an id→name mapping.

    Returns:
        Dict mapping a country document's `_id` to its human name string.
    """
    countries_col = mongodb_connection.countries
    return {c["_id"]: c["country"] for c in countries_col.find()}


def attach_country_names(
    rows: Iterable[dict],
    countries_map: Dict[Any, str],
) -> List[dict]:
    """Add a `country` string to each participant row using `country_id`.

    Args:
        rows: Iterable of participant dicts (each with optional `country_id`).
        countries_map: Mapping from country `_id` to string name.

    Returns:
        A **new list** of participant dicts with `country` added (if known).
    """
    out: List[dict] = []
    for p in rows:
        p = dict(p)  # shallow copy to avoid mutating the original cursor doc
        p["country"] = countries_map.get(p.get("country_id"), "")
        out.append(p)
    return out


def paginate_list(items: List[dict]) -> Tuple[List[dict], int, int, int]:
    """Perform simple in-memory pagination for a list of items.

    Note:
        We paginate **after** fetching and enriching; for very large collections,
        prefer server-side pagination with MongoDB skip/limit.

    Returns:
        (page_items, page, per_page, total)
    """
    page, per_page, offset = get_page_args(page_parameter="page", per_page_parameter="per_page")
    per_page = 10
    return items[offset : offset + per_page], page, per_page, len(items)


def get_participant_by_pid(pid: str) -> Optional[dict]:
    """Fetch a single participant by its `pid` field.

    Important:
        Your schema uses a custom `pid` (e.g., "P0001") as the **primary identifier**.
        Do **not** query by `_id` here.

    Args:
        pid: Participant ID string (e.g., "P0001").

    Returns:
        The participant document or `None` if not found.
    """
    return mongodb_connection.participants.find_one({"pid": pid})


def update_participant(pid: str, name: str, position: str, grade: str) -> int:
    """Update editable participant fields by `pid`.

    Args:
        pid: Participant ID string.
        name: New person name (keep your LASTNAME-in-caps convention externally).
        position: Current position (usually latest attended event).
        grade: Current grade string.

    Returns:
        Number of modified documents (0 or 1).
    """
    res = mongodb_connection.participants.update_one(
        {"pid": pid},
        {"$set": {"name": name, "position": position, "grade": grade}},
    )
    return res.modified_count


def get_participant_country_name(participant: dict) -> str:
    """Resolve a participant's country human name.

    Args:
        participant: Participant document with `country_id`.

    Returns:
        Country string if found; "Unknown" otherwise.
    """
    countries = mongodb_connection.countries
    c = countries.find_one({"_id": participant.get("country_id")})
    return c["country"] if c else "Unknown"


def get_events_for_participant(pid: str) -> List[dict]:
    """Return a participant's attended events, newest first.

    Args:
        pid: Participant ID string.

    Returns:
        A list of event documents sorted descending by `dateFrom`.
    """
    pe = mongodb_connection.db["participant_events"]
    events_col = mongodb_connection.events

    links = pe.find({"participant_id": pid})
    event_ids = [e["event_id"] for e in links]
    events = list(events_col.find({"eid": {"$in": event_ids}}))

    # Sort newest first (dateFrom may be None for malformed rows)
    events.sort(key=lambda e: e.get("dateFrom") or "", reverse=True)
    return events
