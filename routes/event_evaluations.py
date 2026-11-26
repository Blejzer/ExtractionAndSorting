"""Authenticated evaluation paste, preview, save and analysis exports."""

import hashlib
import hmac
from io import BytesIO
import secrets

from flask import Blueprint, abort, current_app, flash, redirect, render_template, request, send_file, session, url_for
from itsdangerous import BadData, URLSafeTimedSerializer
from pymongo.errors import PyMongoError
from werkzeug.exceptions import HTTPException

from middleware.auth import login_required
from repositories.event_evaluation_repository import EventEvaluationRepository, EvaluationConflict
from services.events_service import get_event
from services.event_evaluation_service import EvaluationParseError, parse_evaluation, evaluation_analysis, evaluation_csv

event_evaluations_bp = Blueprint("event_evaluations", __name__)


@event_evaluations_bp.before_request
def limits():
    request.max_content_length = 2_000_000
    request.max_form_memory_size = 1_000_000


@event_evaluations_bp.after_request
def no_cache(response):
    response.headers["Cache-Control"] = "no-store"
    return response


@event_evaluations_bp.errorhandler(HTTPException)
def page_error(error):
    return render_template("event_evaluation_error.html", message=error.description), error.code


def _csrf():
    if "event_evaluation_csrf" not in session:
        session["event_evaluation_csrf"] = secrets.token_urlsafe(32)
    return session["event_evaluation_csrf"]


def _verify_csrf():
    if not session.get("event_evaluation_csrf") or not hmac.compare_digest(
            request.form.get("csrf_token", "").encode("utf-8"), _csrf().encode("utf-8")):
        abort(400, "The form session expired. Open the evaluation page again.")


def _signature(text, eid, version):
    return dict(event_id=eid, version=version, source_hash=hashlib.sha256(text.encode()).hexdigest(),
                binding=hashlib.sha256(_csrf().encode()).hexdigest())


def _serializer():
    return URLSafeTimedSerializer(current_app.secret_key, salt="event-evaluation-v1")


def _load(eid):
    try:
        event = get_event(eid)
        if not event:
            abort(404, "Event not found.")
        repo = EventEvaluationRepository()
        return event, repo, repo.get(eid)
    except PyMongoError:
        abort(503, "Evaluations are temporarily unavailable. Retry when the database connection is restored.")


def _render(event, saved, text, report=None, version=0, token=None, error=None, status=200):
    return render_template(
        "event_evaluations.html", event=event, saved=saved, raw_text=text,
        preview=report, preview_analysis=evaluation_analysis(report) if report else None,
        saved_analysis=evaluation_analysis(saved["report"]) if saved else None,
        expected_version=version, preview_token=token, csrf_token=_csrf(), error=error,
    ), status


@event_evaluations_bp.route("/events/<eid>/evaluations", methods=["GET", "POST"])
@login_required
def show(eid):
    if request.method == "POST":
        _verify_csrf()
    event, repo, saved = _load(eid)
    current_version = saved.get("version", 0) if saved else 0
    if request.method == "GET":
        return _render(event, saved, saved.get("raw_text", "") if saved else "", version=current_version)
    text = request.form.get("raw_text", "")
    try:
        version = int(request.form.get("expected_version", ""))
        if version < 0:
            raise ValueError
    except ValueError:
        abort(400, "Invalid evaluation version. Reload the evaluation page.")
    action = request.form.get("action")
    if action not in ("preview", "save"):
        abort(400, "Choose Preview or Save evaluation.")
    try:
        report = parse_evaluation(text)
    except EvaluationParseError as exc:
        return _render(event, saved, text, version=version, error=str(exc), status=400)
    token = _serializer().dumps(_signature(text, eid, version))
    if report["errors"]:
        return _render(event, saved, text, report, version,
                       error="Correct the errors in the pasted text and preview again.", status=400)
    if action == "preview":
        return _render(event, saved, text, report, version, token)
    try:
        reviewed = _serializer().loads(request.form.get("preview_token", ""), max_age=1800)
    except BadData:
        return _render(event, saved, text, report, version,
                       error="Preview the evaluation before saving. Previews expire after 30 minutes.", status=400)
    if reviewed != _signature(text, eid, version):
        return _render(event, saved, text, report, version,
                       error="The text or event changed. Preview it again before saving.", status=400)
    try:
        repo.save(eid, text, report, version, session.get("username", "user"))
    except EvaluationConflict as exc:
        return _render(event, saved, text, report, version, error=str(exc), status=409)
    except PyMongoError:
        return _render(event, saved, text, report, version, token,
                       error="The evaluation was not saved. Retry when the database connection is restored.", status=503)
    flash("Evaluation saved for this event.", "success")
    return redirect(url_for("event_evaluations.show", eid=eid))


@event_evaluations_bp.get("/events/<eid>/evaluations.csv")
@login_required
def download_csv(eid):
    _, _, saved = _load(eid)
    if not saved:
        abort(404, "No evaluation has been saved for this event.")
    return send_file(BytesIO(evaluation_csv(eid, saved["report"])), mimetype="text/csv; charset=utf-8",
                     as_attachment=True, download_name=f"{eid}_evaluations.csv", max_age=0)


@event_evaluations_bp.get("/events/<eid>/evaluations/source.txt")
@login_required
def download_source(eid):
    _, _, saved = _load(eid)
    if not saved:
        abort(404, "No evaluation has been saved for this event.")
    return send_file(BytesIO(saved["raw_text"].encode("utf-8")), mimetype="text/plain; charset=utf-8",
                     as_attachment=True, download_name=f"{eid}_evaluation_source.txt", max_age=0)
