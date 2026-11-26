"""Review workbook scores, link event participants and save a complete import."""

import hashlib
import hmac
from io import BytesIO
import secrets

from flask import Blueprint, abort, current_app, flash, redirect, render_template, request, send_file, session, url_for
from itsdangerous import BadData, URLSafeTimedSerializer
from pymongo.errors import PyMongoError
from werkzeug.exceptions import HTTPException
from werkzeug.utils import secure_filename

from middleware.auth import login_required
from repositories.prepost_import_repository import PrePostImportRepository, fingerprint
from services.prepost_import_service import MAX_UPLOAD_BYTES, TestImportError, match_test_rows, parse_prepost_workbook, prepost_csv, score_table

prepost_tests_bp = Blueprint("prepost_tests", __name__)


@prepost_tests_bp.before_request
def limits():
    request.max_content_length = MAX_UPLOAD_BYTES + 1_000_000
    request.max_form_memory_size = 1_000_000


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
    return URLSafeTimedSerializer(current_app.secret_key, salt="participant-prepost-v1")


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


def _render(state, rows=None, token=None, source=None, error=None, status=200, replace=False):
    return render_template("prepost_tests.html", event=state, table=score_table(state["roster"], state["tests"]),
                           rows=rows, preview_token=token, source=source, csrf_token=_csrf(),
                           error=error, replace=replace), status


@prepost_tests_bp.route("/events/<eid>/test-scores", methods=["GET", "POST"])
@login_required
def show(eid):
    if request.method == "POST" and (not session.get("prepost_csrf") or not hmac.compare_digest(
            request.form.get("csrf_token", "").encode(), _csrf().encode())):
        abort(400, "The form session expired. Open the test scores page again.")
    repo, state = _load(eid)
    if request.method == "GET":
        return _render(state)
    action = request.form.get("action")
    if action == "preview":
        file = request.files.get("workbook")
        if not file or not file.filename or not file.filename.lower().endswith(".xlsx"):
            return _render(state, error="Choose an Excel .xlsx workbook.", status=400)
        data = file.read(MAX_UPLOAD_BYTES + 1)
        try:
            rows = match_test_rows(parse_prepost_workbook(data), state["roster"])
        except TestImportError as exc:
            return _render(state, error=str(exc), status=400)
        source = dict(filename=secure_filename(file.filename) or "test-scores.xlsx", sha256=hashlib.sha256(data).hexdigest())
        token = _serializer().dumps(dict(eid=eid, binding=_binding(), state=fingerprint(state), rows=rows, source=source))
        return _render(state, rows, token, source)
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
