"""Service helpers for the application's landing page.

This module gathers lightweight statistics used by the home page dashboard:

* total attendances (one participant at one event)
* total number of events
* total number of countries
* the most recently added event based on ``start_date``

The functions are intentionally small and synchronous; if the application grows
larger these can be moved into dedicated repository/service classes.
"""

from __future__ import annotations

from typing import Any, Dict

from config.database import mongodb
from services.statistics_service import count_attendances


def fetch_main_stats() -> Dict[str, Any]:
    """Return basic counts and latest-event info for the dashboard.

    The database schema is simple enough that we query MongoDB collections
    directly here rather than via repository objects.
    """

    db = mongodb.db()
    participants_col = db["participants"]
    events_col = db["events"]
    countries_col = db["countries"]

    # Only identifiers and rosters are needed; biographies and contact details
    # are not loaded for the homepage attendance count.
    attendances = count_attendances(
        events_col.find({}, {"eid": 1, "event_id": 1, "participants": 1, "participant_ids": 1}),
        participants_col.find({}, {"pid": 1, "participant_id": 1}),
        db["participant_events"].find({}, {"_id": 0, "event_id": 1, "eid": 1, "participant_id": 1, "pid": 1}),
    )
    stats: Dict[str, Any] = {
        "attendances": attendances,
        "events": events_col.count_documents({}),
        "countries": countries_col.count_documents({}),
        "latest_event": None,
        "latest_event_date": None,
    }

    # Fetch newest event by start date
    latest_cursor = events_col.find().sort("start_date", -1).limit(1)
    latest_doc = next(latest_cursor, None)
    if latest_doc:
        stats["latest_event"] = latest_doc.get("title") or latest_doc.get("eid")
        stats["latest_event_date"] = latest_doc.get("start_date") or latest_doc.get("dateFrom")

    return stats


__all__ = ["fetch_main_stats"]

