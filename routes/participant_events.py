"""API routes for participant-event associations and statistics."""

from __future__ import annotations

from flask import Blueprint, jsonify, request

from services.participant_event_service import (
    register_participant_event,
    list_events_for_participant,
    event_participants_with_scores,
)

participant_events_bp = Blueprint(
    "participant_events", __name__, url_prefix="/api/participant-events"
)


@participant_events_bp.post("/")
def api_register_participant_event():
    data = request.get_json() or {}
    register_participant_event(data)
    return "", 201


@participant_events_bp.get("/participant/<pid>")
def api_events_for_participant(pid: str):
    events = list_events_for_participant(pid)
    return jsonify([e.model_dump() for e in events])


@participant_events_bp.get("/event/<eid>")
def api_participants_for_event(eid: str):
    info = event_participants_with_scores(eid)
    participants = [p.model_dump() for p in info["participants"]]
    return jsonify(
        {
            "participants": participants,
            "avg_pre": info["avg_pre"],
            "avg_post": info["avg_post"],
        }
    )
