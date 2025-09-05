"""API routes for participant management."""

from __future__ import annotations

from flask import Blueprint, jsonify, request, abort

from services.participant_service import (
    list_participants,
    get_participant,
    create_participant,
    bulk_create_participants,
    update_participant,
    delete_participant,
)

participants_bp = Blueprint("participants", __name__, url_prefix="/api/participants")


@participants_bp.get("/")
def api_list_participants():
    participants = list_participants()
    return jsonify([p.model_dump() for p in participants])


@participants_bp.post("/")
def api_create_participant():
    data = request.get_json() or {}
    participant = create_participant(data)
    return jsonify(participant.model_dump()), 201


@participants_bp.post("/bulk")
def api_bulk_create_participants():
    data = request.get_json() or []
    participants = bulk_create_participants(data)
    return jsonify([p.model_dump() for p in participants]), 201


@participants_bp.get("/<pid>")
def api_get_participant(pid: str):
    participant = get_participant(pid)
    if not participant:
        abort(404)
    return jsonify(participant.model_dump())


@participants_bp.put("/<pid>")
def api_update_participant(pid: str):
    data = request.get_json() or {}
    participant = update_participant(pid, data)
    if not participant:
        abort(404)
    return jsonify(participant.model_dump())


@participants_bp.delete("/<pid>")
def api_delete_participant(pid: str):
    if not delete_participant(pid):
        abort(404)
    return "", 204
