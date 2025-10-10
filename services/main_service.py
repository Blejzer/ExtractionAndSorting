"""Service helpers for the application's landing page.

This module gathers lightweight statistics used by the home page dashboard:

* total number of participants (documents in ``participants``)
* total number of events
* total number of countries
* the most recently added event based on ``dateFrom``

The functions are intentionally small and synchronous; if the application grows
larger these can be moved into dedicated repository/service classes.
"""

from __future__ import annotations

from typing import Any, Dict

from config.database import mongodb


def fetch_main_stats() -> Dict[str, Any]:
    """Return basic counts and latest-event info for the dashboard.

    The database schema is simple enough that we query MongoDB collections
    directly here rather than via repository objects.
    """

    db = mongodb.db()
    participants_col = db["participants"]
    events_col = db["events"]
    countries_col = db["countries"]

    stats: Dict[str, Any] = {
        "participants": participants_col.count_documents({}),
        "events": events_col.count_documents({}),
        "countries": countries_col.count_documents({}),
        "latest_event": None,
        "latest_event_date": None,
    }

    # Fetch newest event by start date
    latest_cursor = events_col.find().sort("dateFrom", -1).limit(1)
    latest_doc = next(latest_cursor, None)
    if latest_doc:
        stats["latest_event"] = latest_doc.get("title") or latest_doc.get("eid")
        stats["latest_event_date"] = latest_doc.get("dateFrom")

    return stats


__all__ = ["fetch_main_stats"]

