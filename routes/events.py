# routes/events.py
from flask import Blueprint, render_template, request
from flask_paginate import Pagination
from services.events_service import (
    get_events, paginate_list, get_participant_counts_for_events,
    get_event_by_eid, get_event_participants
)

events_bp = Blueprint("events", __name__)

@events_bp.route("/events")
def show_events():
    # Query params
    search = request.args.get("search", "")
    sort_field = request.args.get("sort", "dateFrom")  # default newest first
    sort_direction = int(request.args.get("direction", -1))

    # Fetch and listify
    cursor = get_events(search, sort_field, sort_direction)
    events = list(cursor)

    # Participant counts (bulk)
    eids = [e["eid"] for e in events]
    counts_map = get_participant_counts_for_events(eids)
    for e in events:
        e["participant_count"] = counts_map.get(e["eid"], 0)

    # Pagination
    paginated, page, per_page, total = paginate_list(events)
    pagination = Pagination(page=page, per_page=per_page, total=total, css_framework="bootstrap5")

    return render_template(
        "events.html",
        events=paginated,
        search=search,
        sort=sort_field,
        direction=sort_direction,
        pagination=pagination,
        page=page
    )

@events_bp.route("/event/<eid>")
def event_detail(eid):
    # pass-through nav context from list
    sort_field = request.args.get("sort", "country")
    sort_direction = int(request.args.get("direction", 1))

    event = get_event_by_eid(eid)
    if not event:
        return "Event not found", 404

    participants = get_event_participants(eid, sort_field, sort_direction)

    return render_template(
        "event_detail.html",
        event=event,
        participants=participants,
        sort=sort_field,
        direction=sort_direction
    )
