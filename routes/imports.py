# routes/imports.py
from __future__ import annotations

import json
import os
from datetime import datetime, date
from flask import Blueprint, current_app, request, render_template, flash, redirect, url_for
from werkzeug.utils import secure_filename

from middleware.auth import login_required
from services.import_service import (
    validate_excel_file_for_import,
    parse_for_commit,   # heavy parse happens only in /import/proceed
)

imports_bp = Blueprint("imports", __name__, url_prefix="/import")
ALLOWED_EXTENSIONS = {".xlsx", ".xls"}


def _allowed_file(filename: str) -> bool:
    _, ext = os.path.splitext(filename.lower())
    return ext in ALLOWED_EXTENSIONS


def _upload_dir() -> str:
    # Fallback to ./uploads if not configured
    d = current_app.config.get("UPLOADS_DIR", os.path.join(os.getcwd(), "uploads"))
    os.makedirs(d, exist_ok=True)
    return d


@imports_bp.get("/", strict_slashes=False)
@login_required
def upload_form():
    # Simple file chooser form
    return render_template("import_upload.html")


@imports_bp.post("/", strict_slashes=False)
@login_required
def upload_check():
    """
    FAST PATH: Upload + VALIDATE ONLY.
    - Saves the file to UPLOAD_FOLDER
    - Validates structure (A1/A2, ParticipantsLista, ParticipantsList, country tables)
    - If OK -> render confirmation page with hidden filename
    - If BAD -> render errors and delete the uploaded file
    """
    f = request.files.get("file")
    if not f or not f.filename:
        flash("No file selected.", "warning")
        return redirect(url_for("imports.upload_form"))

    if not _allowed_file(f.filename):
        flash("Unsupported file type. Please upload .xlsx or .xls.", "danger")
        return redirect(url_for("imports.upload_form"))

    upload_dir = _upload_dir()
    filename = secure_filename(f.filename)
    dest = os.path.join(upload_dir, filename)
    f.save(dest)

    ok, missing, _seen = validate_excel_file_for_import(dest)
    if ok:
        # Do NOT parse here—only confirm we can proceed.
        return render_template("import_check.html", status="ok", filename=filename)
    else:
        # Clean up invalid file
        try:
            os.remove(dest)
        except OSError:
            pass
        return render_template("import_check.html", status="bad", missing=missing)


@imports_bp.post("/proceed")
@login_required
def proceed_parse():
    """
    HEAVY PATH: Perform full parse now (no DB writes yet).
    - Reads the previously uploaded file by name
    - Runs parse_for_commit
    - Stores a small preview JSON (optional) and flashes a summary
    """
    filename = request.form.get("filename", "")
    if not filename:
        flash("Missing filename; please re-upload.", "warning")
        return redirect(url_for("imports.upload_form"))

    upload_dir = _upload_dir()
    path = os.path.join(upload_dir, secure_filename(filename))
    if not os.path.exists(path):
        flash("Uploaded file not found; please re-upload.", "danger")
        return redirect(url_for("imports.upload_form"))

    try:
        payload = parse_for_commit(path)

        # Optional: write a compact preview JSON next to the upload
        preview_name = f"{os.path.splitext(filename)[0]}.preview.json"
        preview_path = os.path.join(upload_dir, preview_name)
        event_raw = payload.get("event", {})
        event_clean = {
            k: v.isoformat() if isinstance(v, (datetime, date)) else v
            for k, v in event_raw.items()
        }
        participants_raw = payload.get("attendees", [])
        participants = [
            {
                k: v.isoformat() if isinstance(v, (datetime, date)) else v
                for k, v in attendee.items()
            }
            for attendee in participants_raw
        ]
        initial_raw = payload.get("initial_attendees", [])
        initial_attendees = [
            {
                k: v.isoformat() if isinstance(v, (datetime, date)) else v
                for k, v in attendee.items()
            }
            for attendee in initial_raw
        ]
        with open(preview_path, "w", encoding="utf-8") as fh:
            json.dump(
                {
                    "event": event_clean,
                    "participants": participants,
                    "participants_count": len(participants),
                    "initial_attendees": initial_attendees,
                    "generated_at": datetime.utcnow().isoformat() + "Z",
                },
                fh,
                ensure_ascii=False,
                indent=2,
            )

        eid = event_raw.get("eid", "UNKNOWN")
        count = len(participants)
        flash(
            f"Parsed event {eid} with {count} attendees. Preview saved as {preview_name}.",
            "success",
        )

        return redirect(url_for("imports.preview", preview_name=preview_name))

    except Exception as e:
        current_app.logger.exception("Import parse failed")
        flash(f"Parse failed: {e}", "danger")
        return redirect(url_for("imports.upload_form"))


@imports_bp.post("/discard")
@login_required
def discard_file():
    """
    Delete the uploaded file if the user cancels.
    """
    filename = request.form.get("filename", "")
    if not filename:
        flash("No file to discard.", "warning")
        return redirect(url_for("imports.upload_form"))

    upload_dir = _upload_dir()
    path = os.path.join(upload_dir, secure_filename(filename))
    try:
        os.remove(path)
        flash("File discarded.", "info")
    except OSError:
        flash("Could not remove file (it may have been removed already).", "warning")

    return redirect(url_for("imports.upload_form"))


@imports_bp.get("/preview/<preview_name>")
@login_required
def preview(preview_name: str):
    """Display event and participants from the generated preview JSON."""
    upload_dir = _upload_dir()
    safe_name = secure_filename(preview_name)
    path = os.path.join(upload_dir, safe_name)
    if not os.path.exists(path):
        flash("Preview file not found; please re-upload.", "danger")
        return redirect(url_for("imports.upload_form"))

    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    event = data.get("event", {})
    participants = data.get("participants", [])
    return render_template("import_preview.html", event=event, participants=participants)
