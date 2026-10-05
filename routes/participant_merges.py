"""Authenticated selection, conflict resolution, final review, and merge confirmation."""

from datetime import datetime
import hashlib
import hmac
import json
import secrets

from flask import Blueprint, abort, current_app, flash, redirect, render_template, request, session, url_for
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from pymongo.errors import PyMongoError
from werkzeug.exceptions import HTTPException

from middleware.auth import login_required
from services.participant_merge_service import MergeError, ParticipantMergeService, fingerprint
from services.participant_service import get_country_lookup


participant_merges_bp = Blueprint("participant_merges", __name__)
_service = ParticipantMergeService()


@participant_merges_bp.errorhandler(HTTPException)
def merge_error(error):
    # The application's generic exception handler otherwise changes aborts to 500.
    return render_template("participant_merge_error.html", message=error.description), error.code


def _csrf():
    if "participant_merge_csrf" not in session:
        session["participant_merge_csrf"] = secrets.token_urlsafe(32)
    return session["participant_merge_csrf"]


def _verify_csrf():
    supplied = request.form.get("csrf_token", "")
    if not session.get("participant_merge_csrf") or not hmac.compare_digest(supplied, _csrf()):
        abort(400, "Your merge session expired. Open the merge page again.")


def _serializer():
    return URLSafeTimedSerializer(current_app.secret_key, salt="participant-merge-v1")


def _binding():
    return hashlib.sha256((_csrf() + str(session.get("username", ""))).encode()).hexdigest()


def _token(payload):
    return _serializer().dumps({**payload, "binding": _binding()})


def _load_token(stage):
    try:
        data = _serializer().loads(request.form.get("preview_token", ""), max_age=1800)
    except (SignatureExpired, BadSignature):
        abort(400, "The preview expired or is invalid. Review the participants again.")
    if data.get("binding") != _binding() or data.get("stage") != stage:
        abort(400, "This preview belongs to a different session or review step.")
    return data


@participant_merges_bp.app_template_filter("merge_value")
def merge_value(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str, ensure_ascii=False, indent=2)
    if value is None or value == "":
        return "—"
    return str(value)


def _render(stage, **context):
    return render_template("participant_merge.html", stage=stage, csrf_token=_csrf(),
                           countries=get_country_lookup(), **context)


@participant_merges_bp.get("/participants/merge")
@login_required
def select_duplicates():
    search = request.args.get("search", "").strip()
    return _render("select", candidates=_service.candidates(search), search=search)


@participant_merges_bp.post("/participants/merge/preview")
@login_required
def preview_merge():
    _verify_csrf()
    pids = request.form.getlist("pids")
    target = request.form.get("target_pid", "")
    try:
        state = _service.snapshot(pids)
        plan = _service.plan(state, target)
    except MergeError as exc:
        abort(400, str(exc))
    token = _token({"stage": "resolve", "pids": sorted(pids), "target_pid": target,
                    "fingerprint": fingerprint(state)})
    return _render("resolve", plan=plan, preview_token=token)


@participant_merges_bp.post("/participants/merge/review")
@login_required
def review_merge():
    _verify_csrf()
    data = _load_token("resolve")
    try:
        state = _service.snapshot(data["pids"])
        if fingerprint(state) != data["fingerprint"]:
            raise MergeError("The records changed since your preview. Select and review them again.")
        choices = {key: int(value) for key, value in request.form.items() if key.startswith("choice_")}
        plan = _service.plan(state, data["target_pid"], choices, require_choices=True)
    except (MergeError, ValueError) as exc:
        abort(400, str(exc))
    token = _token({**data, "stage": "commit", "choices": choices})
    return _render("confirm", plan=plan, preview_token=token)


@participant_merges_bp.post("/participants/merge/confirm")
@login_required
def confirm_merge():
    _verify_csrf()
    data = _load_token("commit")
    if request.form.get("same_person") != "yes":
        abort(400, "Confirm that the selected records belong to the same person.")
    try:
        _service.merge(data["pids"], data["target_pid"], data["fingerprint"],
                       data["choices"], session.get("username", "authenticated user"))
    except MergeError as exc:
        abort(409, str(exc))
    except PyMongoError:
        # Do not expose database URIs or archived personal details in error responses.
        abort(503, "The database could not complete the merge. Reopen the page to check the records before retrying. MongoDB transactions are required.")
    session.pop("participant_merge_csrf", None)
    flash(f"Merged {len(data['pids'])} records into {data['target_pid']}. Original records are archived.", "success")
    return redirect(url_for("participants.participant_detail", pid=data["target_pid"]))
