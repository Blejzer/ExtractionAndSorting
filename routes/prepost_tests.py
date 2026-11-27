"""Review workbook scores, link event participants and save a complete import."""

import hashlib
import hmac
from io import BytesIO
import os
from pathlib import Path
import secrets
import tempfile

from flask import Blueprint, abort, current_app, flash, redirect, render_template, request, send_file, session, url_for
from itsdangerous import BadData, URLSafeTimedSerializer
from pymongo.errors import PyMongoError
from werkzeug.exceptions import HTTPException
from werkzeug.utils import secure_filename

from middleware.auth import login_required
from repositories.prepost_import_repository import PrePostImportRepository, fingerprint
from services.prepost_import_service import MAX_UPLOAD_BYTES, TestImportError, UnsupportedTestLayout, interpret_test_results, match_test_rows, parse_prepost_workbook, prepost_csv, score_table
from services.prepost_ai_service import AIImportUnavailable, analyze_prepost_with_ai
from services.prepost_pending_uploads import PendingWorkbooks, PendingUploadError, PENDING_TTL

prepost_tests_bp = Blueprint("prepost_tests", __name__)


@prepost_tests_bp.before_request
def limits():
    request.max_content_length = MAX_UPLOAD_BYTES + 1_000_000
    request.max_form_memory_size = 1_000_000
    try:
        _pending_store().cleanup()
    except OSError:
        current_app.logger.warning("Pending test workbook cleanup is temporarily unavailable.")


@prepost_tests_bp.after_request
def no_cache(response):
    response.headers["Cache-Control"] = "no-store"
    return response


@prepost_tests_bp.errorhandler(HTTPException)
def page_error(error):
    return render_template("prepost_test_error.html", message=error.description), error.code


def _csrf():
    if "prepost_csrf" not in session:
        session["prepost_csrf"] = secrets.token_urlsafe(32)
    return session["prepost_csrf"]


def _binding():
    return hashlib.sha256(_csrf().encode()).hexdigest()


def _serializer():
    return URLSafeTimedSerializer(current_app.secret_key, salt="participant-prepost-percent-v3")


def _pending_store():
    directory = current_app.config.get("PREPOST_PENDING_DIR") or os.getenv("PREPOST_PENDING_DIR") or Path(tempfile.gettempdir()) / "pfe_prepost_pending"
    return PendingWorkbooks(directory)


def _consent_serializer():
    return URLSafeTimedSerializer(current_app.secret_key, salt="participant-prepost-ai-consent-v1")


def _discard_pending(eid):
    pending = dict(session.get("prepost_pending_uploads", {}))
    upload_id = pending.pop(eid, None)
    if upload_id:
        _pending_store().discard(upload_id)
    session["prepost_pending_uploads"] = pending


def _stage_for_consent(eid, data, source):
    _discard_pending(eid)
    pending = dict(session.get("prepost_pending_uploads", {}))
    while len(pending) >= 3:
        _pending_store().discard(pending.pop(next(iter(pending))))
    upload_id = _pending_store().put(data)
    pending[eid] = upload_id
    session["prepost_pending_uploads"] = pending
    return _consent_serializer().dumps(dict(eid=eid, binding=_binding(), upload_id=upload_id, source=source))


def _consented_upload(eid):
    try:
        pending = _consent_serializer().loads(request.form.get("consent_token", ""), max_age=PENDING_TTL)
    except BadData:
        abort(400, "The AI permission request expired. Upload the workbook again.")
    if pending.get("eid") != eid or pending.get("binding") != _binding():
        abort(400, "The event or form session changed. Upload the workbook again.")
    if session.get("prepost_pending_uploads", {}).get(eid) != pending.get("upload_id"):
        abort(400, "This upload was already used, cancelled or replaced. Upload the workbook again.")
    return pending


def _review(state, eid, rows, source):
    rows = match_test_rows(rows, state["roster"])
    source["sheets"] = sorted({row[a]["sheet"] for row in rows for a in ("pre", "post") if row[a] is not None})
    token = _serializer().dumps(dict(eid=eid, binding=_binding(), state=fingerprint(state), rows=rows, source=source))
    return _render(state, rows, token, source)


def _load(eid):
    try:
        repo = PrePostImportRepository()
        state = repo.snapshot(eid)
        if not state:
            abort(404, "Event not found.")
        return repo, state
    except TestImportError as exc:
        abort(409, str(exc))
    except PyMongoError:
        abort(503, "Test scores are temporarily unavailable. Retry when the database connection is restored.")


def _render(state, rows=None, token=None, source=None, error=None, status=200, replace=False, scoring="partial", consent=None):
    table = score_table(state["roster"], state["tests"])
    return render_template("prepost_tests.html", event=state, table=table,
                           rows=rows, preview_token=token, source=source, csrf_token=_csrf(),
                           preview_interpretation=interpret_test_results(rows) if rows else None,
                           saved_interpretation=interpret_test_results(table),
                           ai_consent=consent,
                           error=error, replace=replace, scoring=source.get("scoring", scoring) if source else scoring), status


@prepost_tests_bp.route("/events/<eid>/test-scores", methods=["GET", "POST"])
@login_required
def show(eid):
    if request.method == "POST" and (not session.get("prepost_csrf") or not hmac.compare_digest(
            request.form.get("csrf_token", "").encode(), _csrf().encode())):
        abort(400, "The form session expired. Open the test scores page again.")
    action = request.form.get("action") if request.method == "POST" else None
    if action == "decline_ai":
        _consented_upload(eid)
        try:
            _discard_pending(eid)
        except OSError:
            abort(503, "The pending workbook could not be removed. No data was sent to OpenAI.")
        flash("AI analysis cancelled. No workbook data was sent to OpenAI.", "info")
        return redirect(url_for("prepost_tests.show", eid=eid))
    repo, state = _load(eid)
    if request.method == "GET":
        return _render(state)
    if action == "preview":
        scoring = request.form.get("scoring", "partial")
        file = request.files.get("workbook")
        if not file or not file.filename or not file.filename.lower().endswith(".xlsx"):
            return _render(state, error="Choose an Excel .xlsx workbook.", status=400)
        data = file.read(MAX_UPLOAD_BYTES + 1)
        source = dict(filename=secure_filename(file.filename) or "test-scores.xlsx", sha256=hashlib.sha256(data).hexdigest(),
                      scoring=scoring, engine="Standard workbook parser", notes=[])
        try:
            _discard_pending(eid)
            rows = parse_prepost_workbook(data, scoring=scoring)
        except UnsupportedTestLayout as exc:
            try:
                token = _stage_for_consent(eid, data, source)
            except OSError:
                return _render(state, error="The workbook could not be held for your AI decision. Please upload again.", status=503, scoring=scoring)
            return _render(state, scoring=scoring, consent=dict(token=token, filename=source["filename"], reason=str(exc)))
        except TestImportError as exc:
            return _render(state, error=str(exc), status=400, scoring=scoring)
        except OSError:
            return _render(state, error="Temporary workbook storage is unavailable. Please try again.", status=503, scoring=scoring)
        return _review(state, eid, rows, source)
    if action == "approve_ai":
        pending = _consented_upload(eid)
        source = pending["source"]
        try:
            data = _pending_store().consume(pending["upload_id"])
            _discard_pending(eid)
            if hashlib.sha256(data).hexdigest() != source["sha256"]:
                raise PendingUploadError("The pending workbook changed. Upload it again.")
            rows, notes = analyze_prepost_with_ai(data, scoring=source["scoring"])
        except AIImportUnavailable as exc:
            return _render(state, error=str(exc), status=503, scoring=source["scoring"])
        except TestImportError as exc:
            return _render(state, error=str(exc), status=400, scoring=source["scoring"])
        except OSError:
            return _render(state, error="The pending workbook is unavailable. Upload it again.", status=503, scoring=source["scoring"])
        source.update(engine="AI-assisted interpretation · source values read locally", notes=notes)
        return _review(state, eid, rows, source)
    if action != "save":
        abort(400, "Choose Preview workbook or Save scores.")
    token = request.form.get("preview_token", "")
    try:
        reviewed = _serializer().loads(token, max_age=1800)
    except BadData:
        abort(400, "Upload and preview the workbook again. Previews expire after 30 minutes.")
    if reviewed.get("eid") != eid or reviewed.get("binding") != _binding():
        abort(400, "The event or form session changed. Upload and preview again.")
    rows, source = reviewed["rows"], reviewed["source"]
    selections = [request.form.get(f"participant_{i}", "") for i in range(len(rows))]
    replace = request.form.get("replace_scores") == "1"
    for row, selection in zip(rows, selections):
        row["selected_pid"] = selection
    if any(not pid and (row["pre"] is not None or row["post"] is not None) for row, pid in zip(rows, selections)):
        return _render(state, rows, token, source, "Choose a participant or explicitly skip each unmatched entry.", 400, replace)
    try:
        count = repo.save(eid, rows, ["" if pid == "__skip__" else pid for pid in selections],
                          reviewed["state"], source, session.get("username", "user"), replace)
    except TestImportError as exc:
        return _render(state, rows, token, source, str(exc), 409, replace)
    except PyMongoError:
        return _render(state, rows, token, source,
                       "No scores were saved. Retry when the database connection is restored. Imports require MongoDB transaction support.", 503, replace)
    flash(f"Saved {count} test scores linked to this event's participants.", "success")
    return redirect(url_for("prepost_tests.show", eid=eid))


@prepost_tests_bp.get("/events/<eid>/test-scores.csv")
@login_required
def download_csv(eid):
    _, state = _load(eid)
    return send_file(BytesIO(prepost_csv(eid, score_table(state["roster"], state["tests"]))),
                     mimetype="text/csv; charset=utf-8", as_attachment=True,
                     download_name=f"{secure_filename(eid)}_test_scores.csv", max_age=0)
