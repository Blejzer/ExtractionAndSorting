"""Review extracted spreadsheets before linking participants to an event."""

from flask import Blueprint, abort, flash, redirect, render_template, request, url_for
from pymongo.errors import PyMongoError
from werkzeug.exceptions import HTTPException

from domain.models.event_participant import Transport
from middleware.auth import login_required
from repositories.event_repository import EventRepository
from repositories.participant_repository import ParticipantRepository
from routes.word_extraction import _csrf, _verify_csrf, configure, limits, no_cache
from services.import_validation import EDITABLE_FIELDS, SNAPSHOT_FIELDS, event_errors, error_message
from services.imports.participant_review import PROFILE_FIELDS, ReviewMatchError
from services.upload_service import UploadError, upload_preview_data
from services.word_draft_store import import_draft_lock, load_draft, save_draft
from services.word_extraction_service import MAX_FILE_BYTES, parse_date
from services.word_import_service import WordImportError, _country, convert_record, read_export, review_records, row_errors
from services.word_matching_service import load_match_context


word_import_bp = Blueprint("word_import", __name__, url_prefix="/imports/word/import")
word_import_bp.record_once(configure)
word_import_bp.before_request(limits)
word_import_bp.after_request(no_cache)


@word_import_bp.errorhandler(HTTPException)
def import_error(error):
    return render_template("word_import_upload.html", error=error.description, events=[], csrf_token=_csrf()), error.code


def _load(batch_id):
    batch = load_draft(batch_id)
    if batch.get("kind") != "word_import":
        abort(404, "Import preview not found.")
    return batch


def _database():
    try:
        return load_match_context(), ParticipantRepository(), EventRepository()
    except PyMongoError:
        abort(503, "The database is unavailable. Retry the import when the connection is restored.")


@word_import_bp.get("/", strict_slashes=False)
@login_required
def upload_form():
    try:
        events = sorted(EventRepository().find_all(), key=lambda event: event.eid, reverse=True)
    except PyMongoError:
        abort(503, "The event list is unavailable. Retry when the database connection is restored.")
    return render_template("word_import_upload.html", events=events, csrf_token=_csrf())


@word_import_bp.post("/", strict_slashes=False)
@login_required
def upload_file():
    _verify_csrf()
    file = request.files.get("file")
    if not file or not file.filename:
        abort(400, "Select an extracted Excel or CSV file.")
    context, participants, events = _database()
    try:
        records = read_export(file.stream.read(MAX_FILE_BYTES + 1), file.filename)
        event_id = request.form.get("event_id", "").strip()
        if not event_id:
            raise WordImportError("Select an existing event or choose Create a new event.")
        existing_id = None
        if event_id == "__new__":
            references = {record["fields"]["event_reference"] for record in records if record["fields"]["event_reference"]}
            new_id = request.form.get("new_event_id", "").strip() or (next(iter(references)) if len(references) == 1 else "")
            if not new_id:
                raise WordImportError("Enter the new event ID. This file does not have a single event reference.")
            if events.find_by_eid(new_id):
                raise WordImportError("That event already exists. Select it from the existing events instead.")
            event = {"eid": new_id, "title": "", "start_date": "", "end_date": "", "place": "", "country": None, "type": None, "cost": None}
        else:
            selected = events.find_by_eid(event_id)
            if not selected:
                raise WordImportError("The selected event was not found. Select it again.")
            existing_id = selected.eid
            event = {key: value.isoformat()[:10] if hasattr(value, "isoformat") else value
                     for key, value in selected.model_dump().items() if key in ("eid", "title", "start_date", "end_date", "place", "country", "type", "cost")}
        rows = review_records([convert_record(record, context) for record in records], participants, context)
        for row in rows:
            row["_include"] = not row["_source_event"] or row["_source_event"] == event["eid"]
        batch = {"kind": "word_import", "event": event, "existing_event_id": existing_id, "participants": rows}
        batch_id = save_draft(batch)
        return redirect(url_for("word_import.preview", batch_id=batch_id))
    except (WordImportError, ReviewMatchError) as exc:
        abort(400, str(exc))
    except PyMongoError:
        abort(503, "The database check failed. Retry when the connection is restored.")


def _edit(batch, context):
    if not batch["existing_event_id"]:
        for field in batch["event"]:
            key = f"event[{field}]"
            if key in request.form:
                batch["event"][field] = request.form[key].strip() or None
                if field in ("start_date", "end_date") and batch["event"][field]:
                    batch["event"][field] = parse_date(batch["event"][field]) or batch["event"][field]
    for index, row in enumerate(batch["participants"]):
        row["_include"] = request.form.get(f"include[{index}]") == "1"
        for field in EDITABLE_FIELDS:
            key = f"participants[{index}][{field}]"
            if key not in request.form:
                continue
            value = request.form[key].strip()
            if len(value) > 20000 and value != str(row.get(field) or ""):
                abort(400, "An edited field exceeds 20,000 characters.")
            if field in SNAPSHOT_FIELDS and not value:
                row.pop(field, None)
            elif field in ("representing_country", "birth_country"):
                row[field] = _country(value, context) or None
            elif field == "citizenships":
                whole = context.resolve_country(value)
                row[field] = [whole] if whole else [_country(v, context) for v in value.replace(",", ";").split(";") if v.strip()] or None
            elif value or field in row:
                row[field] = value or None
        old_pid = (row.get("_review") or {}).get("pid", "")
        choice = request.form.get(f"match[{index}]", row.get("_match_choice", ""))
        if choice != old_pid and row.get("_review"):
            row.pop("_review", None)
            row.pop("pid", None)
        row["_match_choice"] = choice
        if row.get("_review"):
            row["_review"]["accepted_fields"] = sorted(field for field in PROFILE_FIELDS if request.form.get(f"accept[{index}][{field}]") == "1")


def _event_errors(batch):
    if batch["existing_event_id"]:
        return {}
    errors = event_errors(batch["event"])
    for field in ("start_date", "end_date"):
        if batch["event"].get(field) and not parse_date(batch["event"][field]):
            errors[field] = "Enter a valid, unambiguous event date as YYYY-MM-DD."
    return errors


def _render(batch, batch_id, context, repo):
    for row in batch["participants"]:
        try:
            row["_field_errors"] = row_errors(row, repo, context)
        except ReviewMatchError as exc:
            row["_field_errors"] = {"name": str(exc)}
    return render_template("import_preview.html", event=batch["event"], participants=batch["participants"], participant_events=[],
                           preview_name=batch_id, profile_fields=PROFILE_FIELDS, transport_choices=[transport.value for transport in Transport],
                           event_errors=_event_errors(batch), word_import=True,
                           existing_event=bool(batch["existing_event_id"]), country_choices=context.countries, csrf_token=_csrf(),
                           preview_url=url_for("word_import.preview", batch_id=batch_id), back_url=url_for("word_import.upload_form"))


@word_import_bp.route("/<batch_id>", methods=["GET", "POST"])
@login_required
def preview(batch_id):
    # GET also saves refreshed comparisons. Keep it from overwriting a commit
    # result while another request imports the same draft.
    with import_draft_lock():
        return _preview(batch_id)


def _preview(batch_id):
    if request.method == "POST":
        _verify_csrf()
    batch = _load(batch_id)
    if batch.get("committed"):
        return render_template("word_import_success.html", batch=batch)
    context, repo, events = _database()
    try:
        if batch["existing_event_id"]:
            selected = events.find_by_eid(batch["existing_event_id"])
            if selected is None:
                raise WordImportError("The selected event no longer exists. Select an event again.")
        previous = [(row.get("_review") or {}).get("pid") for row in batch["participants"]]
        if request.method == "POST":
            _edit(batch, context)
        batch["participants"] = review_records(batch["participants"], repo, context)
        save_draft(batch, batch_id)
        if request.method == "POST" and request.form.get("import_now") == "1":
            rows = [row for row in batch["participants"] if row["_include"]]
            if not rows:
                raise WordImportError("Select at least one participant to import.")
            if any(previous[index] != (row.get("_review") or {}).get("pid") for index, row in enumerate(batch["participants"]) if row["_include"]):
                raise WordImportError("Participant matches changed. Review the updated profile comparisons, then import again.")
            errors = _event_errors(batch)
            if errors:
                raise WordImportError("Event: " + error_message(errors))
            for index, row in enumerate(rows, 1):
                errors = row_errors(row, repo, context)
                if errors:
                    raise WordImportError(f"{row.get('name') or f'Row {index}'}: " + error_message(errors))
            result = upload_preview_data({"event": batch["event"], "participants": rows}, event_repo=events, participant_repo=repo,
                                         existing_event_id=batch["existing_event_id"], partial_snapshots=True)
            batch["committed"] = True
            batch["result"] = {"event_id": result["event"].eid, "participants": [{"pid": person.pid, "name": person.name} for person in result["participants"]]}
            # The original row evidence is now stored with the event links.
            # Retain only a small receipt so resubmissions stay idempotent.
            batch["participants"] = []
            save_draft(batch, batch_id)
            flash(f"Imported {len(result['participants'])} participants for event {result['event'].eid}.", "success")
            return redirect(url_for("word_import.preview", batch_id=batch_id))
    except (WordImportError, ReviewMatchError, UploadError) as exc:
        flash(str(exc), "danger")
    except PyMongoError:
        flash("The database check failed. No import was completed; retry when the connection is restored.", "danger")
    if request.method == "POST":
        return redirect(url_for("word_import.preview", batch_id=batch_id))
    return _render(batch, batch_id, context, repo)
