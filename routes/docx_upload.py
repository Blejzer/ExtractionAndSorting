from __future__ import annotations

import json
import os
from datetime import datetime

from flask import Blueprint, current_app, flash, redirect, render_template, request, url_for
from werkzeug.utils import secure_filename

from middleware.auth import login_required
from services.docx_import_service import DocxImportService


docx_upload_bp = Blueprint("docx_upload", __name__, url_prefix="/upload/docx")


@docx_upload_bp.route("", methods=["GET", "POST"], strict_slashes=False)
@login_required
def upload_docx():
    if request.method == "GET":
        return render_template("docx_upload.html")

    eid = (request.form.get("eid") or "").strip()
    files = request.files.getlist("files")
    if not eid:
        flash("Missing event id (eid).", "danger")
        return redirect(url_for("docx_upload.upload_docx"))

    if not files:
        flash("Please choose at least one DOCX file.", "warning")
        return redirect(url_for("docx_upload.upload_docx"))

    upload_root = current_app.config.get("UPLOADS_DIR", os.path.join(os.getcwd(), "uploads"))
    upload_dir = os.path.join(upload_root, "docx")
    os.makedirs(upload_dir, exist_ok=True)

    saved_files: list[str] = []
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    for file in files:
        if not file.filename:
            continue
        filename = secure_filename(file.filename)
        if not filename.lower().endswith(".docx"):
            continue
        full_name = f"{timestamp}_{filename}"
        path = os.path.join(upload_dir, full_name)
        file.save(path)
        saved_files.append(path)

    if not saved_files:
        flash("No valid .docx files were provided.", "danger")
        return redirect(url_for("docx_upload.upload_docx"))

    service = DocxImportService()
    try:
        bundle = service.extract_participants(saved_files, eid)
    except Exception as exc:
        flash(f"DOCX extraction failed: {exc}", "danger")
        return redirect(url_for("docx_upload.upload_docx"))

    warnings = bundle.get("warnings", [])
    for message in warnings:
        flash(message, "warning")

    return render_template(
        "docx_review.html",
        participants=bundle["participants"],
        warnings=warnings,
        eid=eid,
        stage="review",
    )


@docx_upload_bp.post("/compare")
@login_required
def compare_docx():
    service = DocxImportService()
    eid = request.form.get("eid", "")
    participants_blob = request.form.get("participants_json", "[]")
    participants = json.loads(participants_blob)

    compared = []
    for participant in participants:
        result = service.compare_with_db(participant)
        compared.append({
            "extracted": result.extracted,
            "existing": result.existing,
            "score": result.score,
        })

    return render_template("docx_review.html", stage="compare", compared=compared, eid=eid)


@docx_upload_bp.post("/commit")
@login_required
def commit_docx():
    service = DocxImportService()
    eid = request.form.get("eid", "")
    participants_blob = request.form.get("participants_json", "[]")
    participants = json.loads(participants_blob)

    saved = 0
    for participant in participants:
        if participant.get("_skip"):
            continue
        service.save_participant(participant, eid)
        saved += 1

    flash(f"Saved {saved} participant(s) and linked to event {eid}.", "success")
    return redirect(url_for("docx_upload.upload_docx"))
