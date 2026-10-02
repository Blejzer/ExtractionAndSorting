"""Resolve returning participants and compare editable profile fields."""

from datetime import date, datetime
from typing import Any
from pydantic import ValidationError

from domain.models.participant import Participant
from utils.dates import normalize_dob
from utils.names import _to_app_display_name

PROFILE_FIELDS = set(Participant.model_fields) - {"pid", "audit", "created_at", "updated_at"}


class ReviewMatchError(ValueError):
    """A saved review no longer resolves to the same participant."""


def identity_from_record(record: dict) -> dict:
    return {
        "name": _to_app_display_name(record.get("name", "")),
        "dob": record.get("dob"),
        "representing_country": record.get("representing_country", ""),
    }


def find_returning_participant(record: dict, repo) -> Participant | None:
    # After review, resolve using the original identity, even if a selected
    # correction changes an identity field. Never trust a submitted PID alone.
    review = record.get("_review") or {}
    identity = review.get("identity") or identity_from_record(record)
    existing = repo.find_by_name_dob_and_representing_country_cid(
        name=_to_app_display_name(identity.get("name", "")),
        dob=normalize_dob(identity.get("dob")),
        representing_country=identity.get("representing_country", ""),
    )
    if review.get("pid") and (existing is None or existing.pid != review["pid"]):
        raise ReviewMatchError("The returning participant match changed. Re-upload the file to review it again.")
    return existing


def display_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:10]
    if isinstance(value, list):
        return [display_value(item) for item in value]
    return value


def annotate_participant_reviews(participants: list[dict], repo) -> list[dict]:
    annotated = []
    for source in participants:
        record = dict(source)
        record.pop("_conflict", None)
        existing = find_returning_participant(record, repo)
        if existing:
            review = dict(record.get("_review") or {})
            review.update(pid=existing.pid, identity=review.get("identity") or identity_from_record(record))
            record["pid"] = existing.pid
            # Normalize both sides with the domain model so equivalent date,
            # phone and enum representations are not shown as changes.
            probe = {**existing.model_dump(), **{key: value for key, value in record.items() if key in PROFILE_FIELDS}}
            try:
                incoming = Participant.model_validate(probe, context={"allow_missing_dob": True})
                new = incoming.model_dump()
            except ValidationError:
                # Keep invalid file values editable in the preview. Accepted
                # values will be validated by the uploader before any writes.
                new = probe
            old = existing.model_dump()
            changes = {}
            for field in PROFILE_FIELDS.intersection(record):
                before, after = display_value(old.get(field)), display_value(new.get(field))
                if field == "email":
                    before = before.strip().lower() if isinstance(before, str) else before
                    after = after.strip().lower() if isinstance(after, str) else after
                if before != after:
                    changes[field] = {"stored": before, "file": after}
            record["_review"] = review
            record["_changes"] = changes
        annotated.append(record)
    return annotated
