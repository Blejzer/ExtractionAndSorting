"""Authenticated country, training-area, and participant-profile reporting."""

from datetime import date
import os

from flask import Blueprint, jsonify, render_template, request
from pymongo.errors import PyMongoError

from domain.reporting import COUNTRIES, TRAINING_AREAS
from middleware.auth import login_required
from services.statistics_service import fetch_statistics


statistics_bp = Blueprint("statistics", __name__)


@statistics_bp.get("/api/statistics")
@statistics_bp.get("/statistics")
@login_required
def show_statistics():
    values = {key: request.args.get(key, "").strip() for key in ("year", "area")}
    values["policy_change"] = request.args.get("policy_change", os.getenv("STATISTICS_POLICY_CHANGE_DATE", "")).strip()
    errors = []
    year = None
    policy_change = None
    if values["year"]:
        try:
            year = int(values["year"])
            if not 1900 <= year <= 2100:
                raise ValueError
        except ValueError:
            errors.append("Select a valid event year.")
    if values["area"] and values["area"] not in TRAINING_AREAS and values["area"] != "unclassified":
        errors.append("Select a valid training area.")
    if values["policy_change"]:
        try:
            policy_change = date.fromisoformat(values["policy_change"])
        except ValueError:
            errors.append("Enter the invitation-policy change date as YYYY-MM-DD.")
    report = None
    status = 400 if errors else 200
    if not errors:
        try:
            report = fetch_statistics(year=year, area=values["area"] or None, policy_change=policy_change)
        except PyMongoError:
            errors.append("Statistics are temporarily unavailable. Please try again later.")
            status = 503
    if request.path == "/api/statistics":
        return jsonify({"errors": errors} if errors else report), status
    return render_template("statistics.html", report=report, filters=values, errors=errors,
                           country_choices=COUNTRIES, training_area_choices=TRAINING_AREAS), status
