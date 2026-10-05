"""Temporary, read-only Word extraction and participant database checks."""

from collections import Counter
from io import BytesIO
import hmac
import os
import secrets

from flask import Blueprint, abort, current_app, flash, redirect, render_template, request, send_file, session, url_for
from pymongo.errors import PyMongoError
from werkzeug.exceptions import HTTPException

from middleware.auth import login_required
from services.word_draft_store import delete_draft, load_draft, save_draft
from services.word_export_service import WordExportError, export_csv, export_xlsx
from services.word_extraction_service import FIELDS, MAX_FILE_BYTES, WordExtractionError, extract_files, parse_date
from services.word_matching_service import load_match_context


word_extraction_bp = Blueprint("word_extraction", __name__, url_prefix="/imports/word")
CORE_FIELDS = ("name", "representing_country", "dob", "gender", "organization", "position", "rank")


@word_extraction_bp.record_once
def configure(state):
    state.app.config.setdefault("WORD_EXTRACTION_ENABLED", os.getenv("WORD_EXTRACTION_ENABLED", "1").casefold() not in ("0", "false", "no"))


@word_extraction_bp.before_request
def limits():
    if not current_app.config.get("WORD_EXTRACTION_ENABLED", True):
        abort(404, "The temporary Word extraction tool is disabled.")
    request.max_content_length = 50 * 1024 * 1024
    request.max_form_memory_size = 15 * 1024 * 1024


@word_extraction_bp.after_request
def no_cache(response):
    response.headers["Cache-Control"] = "no-store"
    return response


@word_extraction_bp.errorhandler(HTTPException)
def page_error(error):
    return render_template("word_extraction.html", error=error.description, batch=None, csrf_token=_csrf()), error.code


def _csrf():
    if "word_extraction_csrf" not in session:
        session["word_extraction_csrf"] = secrets.token_urlsafe(32)
    return session["word_extraction_csrf"]


def _verify_csrf():
    if not session.get("word_extraction_csrf") or not hmac.compare_digest(request.form.get("csrf_token", ""), _csrf()):
        abort(400, "The form session expired. Open the Word extraction page again.")


def _check(batch):
    try:
        context = load_match_context()
    except PyMongoError:
        batch["database_error"] = "The database check is unavailable. Extraction succeeded; retry the check when the database is accessible."
        batch["countries"] = {}
        for record in batch["records"]:
            record.update(matches=[], match_status="Not checked", match_notes=[])
        return
    batch.pop("database_error", None)
    batch["countries"] = context.countries
    for record in batch["records"]:
        context.check(record)


def _render(batch=None, batch_id=None):
    counts = Counter(r["match_status"] for r in batch["records"]) if batch else {}
    return render_template("word_extraction.html", batch=batch, batch_id=batch_id, csrf_token=_csrf(),
                           fields=FIELDS, core_fields=CORE_FIELDS, counts=counts)


@word_extraction_bp.get("/", strict_slashes=False)
@login_required
def upload_page():
    return _render()


@word_extraction_bp.post("/", strict_slashes=False)
@login_required
def extract_upload():
    _verify_csrf()
    files = request.files.getlist("files")
    if not 1 <= len(files) <= 100:
        abort(400, "Select between one and 100 Word documents.")
    inputs = []
    for file in files:
        content = file.stream.read(MAX_FILE_BYTES + 1)
        inputs.append((file.filename or "Unnamed.docx", content))
    try:
        batch = extract_files(inputs, request.form.get("slash_order", "auto"))
    except WordExtractionError as exc:
        abort(400, str(exc))
    if len(batch["records"]) > 1000:
        abort(413, "The files contain more than 1,000 participant entries. Use smaller batches.")
    _check(batch)
    batch_id = save_draft(batch)
    return redirect(url_for("word_extraction.review_page", batch_id=batch_id))


@word_extraction_bp.get("/<batch_id>")
@login_required
def review_page(batch_id):
    return _render(load_draft(batch_id), batch_id)


def _edited_batch(batch_id):
    _verify_csrf()
    batch = load_draft(batch_id)
    fallback_country = request.form.get("missing_country", "").strip()
    for index, record in enumerate(batch["records"]):
        for field in FIELDS:
            key = f"r{index}_{field}"
            if key in request.form:
                value = request.form[key].strip()
                if len(value) > 20000 and value != record["fields"][field]:
                    abort(400, "An edited field exceeds 20,000 characters.")
                record["fields"][field] = value
        if fallback_country and not record["fields"]["representing_country"] and not record["fields"]["country_label"]:
            record["fields"]["representing_country"] = fallback_country
        dob = record["fields"]["dob"]
        if dob and not parse_date(dob):
            abort(400, f"Row {index + 1}: enter a valid DOB as YYYY-MM-DD, or leave it blank for review.")
        record["fields"]["dob"] = parse_date(dob)
    _check(batch)
    save_draft(batch, batch_id)
    return batch


@word_extraction_bp.post("/<batch_id>/check")
@login_required
def recheck(batch_id):
    _edited_batch(batch_id)
    return redirect(url_for("word_extraction.review_page", batch_id=batch_id))


@word_extraction_bp.post("/<batch_id>/export.<format>")
@login_required
def download(batch_id, format):
    if format not in ("xlsx", "csv"):
        abort(404)
    batch = _edited_batch(batch_id)
    try:
        data = export_xlsx(batch["records"]) if format == "xlsx" else export_csv(batch["records"])
    except WordExportError as exc:
        abort(400, str(exc))
    return send_file(BytesIO(data), mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if format == "xlsx" else "text/csv; charset=utf-8",
                     as_attachment=True, download_name=f"word_participants.{format}", max_age=0)


@word_extraction_bp.post("/<batch_id>/clear")
@login_required
def clear(batch_id):
    _verify_csrf()
    delete_draft(batch_id)
    flash("The extracted batch has been cleared.", "success")
    return redirect(url_for("word_extraction.upload_page"))
