# routes/imports.py
"""Blueprint routes for importing Excel files.

This module provides a small flow:
- GET  /import          → show the upload page
- POST /import/upload   → accept a file, validate required tables, show confirmation
- POST /import/parse    → (dry-run) parse after confirmation; prints to logs / shows result

Notes:
- No database writes should occur here (dry-run / preview only), unless your version already does.
- Keep endpoint names aligned with templates: use `url_for('imports.import_upload')` and
  `url_for('imports.import_parse')` in forms.
"""

import os
from flask import Blueprint, render_template, request, current_app, flash, redirect, url_for, abort
from werkzeug.utils import secure_filename
from routes.auth import login_required
from services.excel_import_service import validate_excel_file_for_import, inspect_and_preview_uploaded

imports_bp = Blueprint("imports", __name__, url_prefix="/import")

ALLOWED_EXTENSIONS = {"xlsx", "xls"}

def allowed_file(filename: str) -> bool:
    """Return True if filename has an allowed spreadsheet extension."""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def _uploads_path(filename: str) -> str:
    """Return absolute path in UPLOAD_FOLDER for filename; prevents path traversal."""
    safe = secure_filename(filename)
    folder = current_app.config["UPLOAD_FOLDER"]
    fullpath = os.path.abspath(os.path.join(folder, safe))
    if not fullpath.startswith(os.path.abspath(folder) + os.sep):
        abort(400, "Invalid filename")
    return fullpath

@imports_bp.route("", methods=["GET"], endpoint="upload_form")
@login_required
def upload_form():
    """Render the upload/validation entry page.

    Returns
    -------
    str
        The rendered 'import_upload.html' template containing the file upload form.

    Expected Template Behavior
    --------------------------
    The form on this page should submit via:
        method="POST"
        action="{{ url_for('imports.import_upload') }}"
        enctype="multipart/form-data"
    and include a single file input named 'file'.
    """

    return render_template("import_upload.html")


@imports_bp.route("", methods=["POST"], endpoint="upload_check")
@login_required
def upload_check():
    """Handle upload: save file, verify required Excel tables, and show confirmation."""

    # 1) receive file
    if "file" not in request.files:
        flash("No file part", "warning")
        return redirect(url_for("imports.upload_form"))

    file = request.files["file"]
    if file.filename == "":
        flash("No file selected", "warning")
        return redirect(url_for("imports.upload_form"))

    if not allowed_file(file.filename):
        flash("Invalid file type. Please upload .xlsx or .xls", "danger")
        return redirect(url_for("imports.upload_form"))

    # 2) save temporarily
    safe_name = secure_filename(file.filename)
    dest = _uploads_path(safe_name)
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    file.save(dest)

    # 3) validate content
    ok, missing, _tables = validate_excel_file_for_import(dest)

    if ok:
        # Ask to proceed; show a confirm screen
        return render_template("import_check.html", filename=safe_name, status="ok")
    else:
        # Remove the file (optional), or keep for debugging
        try:
            os.remove(dest)
        except Exception:
            pass
        # Show what’s missing
        return render_template("import_check.html", filename=safe_name, status="bad", missing=missing)


@imports_bp.route("/proceed", methods=["POST"], endpoint="proceed_parse")
@login_required
def proceed_parse():
    """Parse the previously uploaded file (dry run) and log findings; no DB writes."""
    filename = request.form.get("filename", "")
    if not filename:
        flash("Missing filename for parsing.", "warning")
        return redirect(url_for("imports.upload_form"))

    path = _uploads_path(filename)
    if not os.path.exists(path):
        flash("Uploaded file not found.", "warning")
        return redirect(url_for("imports.upload_form"))

    try:
        print(f"[INFO] Parsing uploaded Excel: {path}")
        inspect_and_preview_uploaded(path)  # logs output; no writes
        flash("Parsed successfully. Check server logs for details.", "success")
    except Exception as e:
        flash(f"Parsing failed: {e}", "danger")

    # keep the file around, or delete if you prefer
    return redirect(url_for("imports.upload_form"))


@imports_bp.route("/discard", methods=["POST"], endpoint="discard_file")
@login_required
def discard_file():
    """Delete a previously uploaded file from the uploads folder."""
    filename = request.form.get("filename", "")
    if not filename:
        flash("Nothing to discard.", "info")
        return redirect(url_for("imports.upload_form"))
    path = _uploads_path(filename)
    try:
        if os.path.exists(path):
            os.remove(path)
            flash(f"Discarded file: {filename}", "info")
    except Exception as e:
        flash(f"Failed to discard: {e}", "danger")
    return redirect(url_for("imports.upload_form"))