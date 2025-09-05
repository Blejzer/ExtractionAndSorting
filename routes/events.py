"""API routes for event management."""

from __future__ import annotations

from flask import Blueprint, jsonify, request, abort

from services.events_service import (
    list_events,
    get_event,
    create_event,
    update_event,
    delete_event,
)

events_bp = Blueprint("events", __name__, url_prefix="/api/events")


@events_bp.get("/")
def api_list_events():
    events = list_events()
    return jsonify([e.model_dump(by_alias=True) for e in events])


@events_bp.post("/")
def api_create_event():
    data = request.get_json() or {}
    event = create_event(data)
    return jsonify(event.model_dump(by_alias=True)), 201


@events_bp.get("/<eid>")
def api_get_event(eid: str):
    event = get_event(eid)
    if not event:
        abort(404)
    return jsonify(event.model_dump(by_alias=True))


@events_bp.put("/<eid>")
def api_update_event(eid: str):
    data = request.get_json() or {}
    event = update_event(eid, data)
    if not event:
        abort(404)
    return jsonify(event.model_dump(by_alias=True))


@events_bp.delete("/<eid>")
def api_delete_event(eid: str):
    if not delete_event(eid):
        abort(404)
    return "", 204
