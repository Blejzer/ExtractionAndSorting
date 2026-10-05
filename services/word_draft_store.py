"""Short-lived, session-owned drafts outside MongoDB and browser cookies."""

import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import tempfile
import time

from flask import abort, current_app, session


TTL_SECONDS = 1800


def _directory():
    default = Path(current_app.config.get("UPLOADS_DIR", current_app.instance_path)) / "word-extraction"
    directory = Path(current_app.config.get("WORD_EXTRACTION_DIR", default))
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    for path in directory.glob("*.json"):
        try:
            if re.fullmatch(r"[0-9a-f]{32}\.json", path.name) and path.stat().st_mtime < time.time() - TTL_SECONDS:
                path.unlink(missing_ok=True)
        except FileNotFoundError:
            pass  # Another worker may already have removed an expired draft.
    return directory


def _owner():
    if "word_extraction_owner" not in session:
        session["word_extraction_owner"] = secrets.token_urlsafe(32)
    return hashlib.sha256((session["word_extraction_owner"] + str(session.get("username", ""))).encode()).hexdigest()


def save_draft(batch, batch_id=None):
    batch_id = batch_id or secrets.token_hex(16)
    if not re.fullmatch(r"[0-9a-f]{32}", batch_id):
        abort(400, "Invalid Word extraction batch.")
    payload = {"owner": _owner(), "updated_at": time.time(), "data": batch}
    text = json.dumps(payload, ensure_ascii=False)
    if len(text.encode()) > 20 * 1024 * 1024:
        abort(413, "The extracted batch is too large. Upload fewer documents together.")
    directory = _directory()
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=directory, delete=False, prefix="draft-") as temp:
        try:
            os.chmod(temp.name, 0o600)
            temp.write(text)
            temp.flush()
            os.replace(temp.name, directory / f"{batch_id}.json")
        finally:
            Path(temp.name).unlink(missing_ok=True)
    return batch_id


def load_draft(batch_id):
    if not re.fullmatch(r"[0-9a-f]{32}", batch_id):
        abort(404, "Word extraction batch not found.")
    path = _directory() / f"{batch_id}.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        abort(410, "This extraction has expired or was cleared. Upload the documents again.")
    if not hmac.compare_digest(payload.get("owner", ""), _owner()):
        abort(404, "Word extraction batch not found.")
    if payload["updated_at"] < time.time() - TTL_SECONDS:
        path.unlink(missing_ok=True)
        abort(410, "This extraction has expired. Upload the documents again.")
    return payload["data"]


def delete_draft(batch_id):
    load_draft(batch_id)
    (_directory() / f"{batch_id}.json").unlink(missing_ok=True)
