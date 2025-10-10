"""API routes for training test scores."""

from __future__ import annotations

from flask import Blueprint, jsonify, request, abort

from services.tests_service import (
    record_test_score,
    get_test_score,
    list_event_tests,
)

tests_bp = Blueprint("tests", __name__, url_prefix="/api/tests")


@tests_bp.post("/")
def api_record_test():
    data = request.get_json() or {}
    test = record_test_score(data)
    return jsonify(test.model_dump()), 201


@tests_bp.get("/<eid>")
def api_list_tests_for_event(eid: str):
    tests = list_event_tests(eid)
    return jsonify([t.model_dump() for t in tests])


@tests_bp.get("/<eid>/<pid>/<attempt>")
def api_get_test(eid: str, pid: str, attempt: str):
    test = get_test_score(eid, pid, attempt)
    if not test:
        abort(404)
    return jsonify(test.model_dump())
