# services/upload_service.py

"""Helpers for uploading parsed imports previews into MongoDB."""

from __future__ import annotations

import json
from typing import Any, Dict, Mapping, MutableMapping, Optional, Sequence

from config.database import mongodb
from domain.models.event import Event, EventType
from domain.models.event_participant import EventParticipant
from domain.models.participant import Participant
from repositories.event_repository import EventRepository
from repositories.participant_event_repository import ParticipantEventRepository
from repositories.participant_repository import ParticipantRepository
from utils.participants import refresh as refresh_participant_cache
from services.imports.participant_review import find_returning_participant, PROFILE_FIELDS, ReviewMatchError
from services.import_validation import event_errors, participant_errors, snapshot_errors, error_message
from utils.costs import parse_cost
from utils.dates import coerce_datetime


class UploadError(ValueError):
    """Raised when the preview payload cannot be persisted."""


def upload_preview_file(
    path: str,
    *,
    event_repo: Optional[EventRepository] = None,
    participant_repo: Optional[ParticipantRepository] = None,
    participant_event_repo: Optional[ParticipantEventRepository] = None,
) -> Dict[str, Any]:
    """Load a preview JSON file and persist its contents."""

    with open(path, "r", encoding="utf-8") as fh:
        bundle = json.load(fh)

    return upload_preview_data(
        bundle,
        event_repo=event_repo,
        participant_repo=participant_repo,
        participant_event_repo=participant_event_repo,
    )


def upload_preview_data(
    bundle: Mapping[str, Any],
    *,
    event_repo: Optional[EventRepository] = None,
    participant_repo: Optional[ParticipantRepository] = None,
    participant_event_repo: Optional[ParticipantEventRepository] = None,
) -> Dict[str, Any]:
    """Persist the event, participants, and event snapshots contained in ``bundle``."""

    if not bundle:
        raise UploadError("Preview payload is empty")

    event_repo = event_repo or EventRepository()
    participant_repo = participant_repo or ParticipantRepository()
    participant_event_repo = participant_event_repo or ParticipantEventRepository()

    event_source = bundle.get("event")
    if not event_source:
        raise UploadError("Event data is missing from the preview payload")

    event_payload = _ensure_mapping(event_source)
    errors = event_errors(event_payload)
    if errors:
        raise UploadError(f"Event: {error_message(errors)}")
    event = _build_event(event_payload)
    if not event.eid:
        raise UploadError("Event is missing an eid")

    if event_repo.find_by_eid(event.eid):
        raise UploadError(f"Event '{event.eid}' has already been uploaded")

    participants_source = bundle.get("participants") or []
    participant_events_source = bundle.get("participant_events") or []

    participant_snapshot_index = _index_event_snapshots(participant_events_source)
    source_ids = {_ensure_mapping(source).get("pid") for source in participants_source}
    for source in participant_events_source:
        snapshot = _ensure_mapping(source)
        pid = snapshot.get("participant_id") or snapshot.get("pid")
        if not pid or pid not in source_ids:
            raise UploadError("Event snapshot must reference a participant in this preview.")
        if snapshot.get("event_id") not in (None, "", event.eid):
            raise UploadError("Event snapshot must reference the event in this preview.")
        errors = snapshot_errors(snapshot)
        if errors:
            raise UploadError(f"Participant {pid}: {error_message(errors)}")

    prepared_participants: list[dict[str, Any]] = []

    participant_ids: list[str] = []
    for participant_source in participants_source:
        participant_dict = _ensure_mapping(participant_source)
        snapshot_source = (
            participant_snapshot_index.get(participant_dict.get("pid"))
            or _extract_event_snapshot(participant_dict)
        )
        if snapshot_source:
            errors = snapshot_errors(snapshot_source)
            if errors:
                name = participant_dict.get("name") or "Unnamed participant"
                message = error_message(errors)
                raise UploadError(f"{name}: {message}")

        try:
            existing = find_returning_participant(participant_dict, participant_repo)
        except ReviewMatchError as exc:
            raise UploadError(str(exc)) from exc
        review = participant_dict.get("_review")
        accepted = set(review.get("accepted_fields", [])) & PROFILE_FIELDS if review else None

        if existing and review:
            participant_payload = existing.model_dump()
            participant_payload.update({key: participant_dict[key] for key in accepted if key in participant_dict})
        else:
            participant_payload = dict(participant_dict)
        participant_payload["pid"] = existing.pid if existing else "TEMP"
        errors = participant_errors(participant_payload, allow_missing_dob=bool(existing))
        if errors:
            raise UploadError(f"{participant_dict.get('name') or 'Unnamed participant'}: {error_message(errors)}")
        participant_model = Participant.model_validate(
            participant_payload, context={"allow_missing_dob": bool(existing)}
        )

        prepared_participants.append(
            {
                "model": participant_model,
                "existing": existing,
                "accepted_fields": accepted,
                "snapshot_source": snapshot_source,
            }
        )

    saved_participants: list[Participant] = []
    event_participants: list[EventParticipant] = []
    imported_identities: dict[tuple, Participant] = {}
    saved_indexes: dict[str, int] = {}
    snapshot_indexes: dict[str, int] = {}

    try:
        with mongodb.start_session() as session:
            with session.start_transaction():
                for entry in prepared_participants:
                    participant_model: Participant = entry["model"]
                    existing: Participant | None = entry["existing"]
                    snapshot_source: MutableMapping[str, Any] | None = entry["snapshot_source"]
                    identity = (
                        participant_model.name,
                        participant_model.dob,
                        participant_model.representing_country,
                    )
                    existing = existing or imported_identities.get(identity)
                    if existing:
                        accepted = entry["accepted_fields"]
                        if accepted is None:
                            update_payload = participant_model.to_mongo()
                            for field in ("pid", "_audit", "created_at", "updated_at"):
                                update_payload.pop(field, None)
                        else:
                            update_payload = {key: getattr(participant_model, key) for key in accepted}
                        updated = participant_repo.update(
                            existing.pid, update_payload, session=session
                        ) if update_payload else existing
                        saved_participant = updated or participant_model
                    else:
                        new_pid = participant_repo.generate_next_pid(session=session)
                        participant_model = participant_model.model_copy(update={"pid": new_pid})
                        participant_repo.save(participant_model, session=session)
                        saved_participant = participant_model

                    imported_identities[identity] = saved_participant
                    if saved_participant.pid not in saved_indexes:
                        saved_indexes[saved_participant.pid] = len(saved_participants)
                        participant_ids.append(saved_participant.pid)
                        saved_participants.append(saved_participant)
                    else:
                        saved_participants[saved_indexes[saved_participant.pid]] = saved_participant

                    if snapshot_source:
                        snapshot = EventParticipant.model_validate(
                            _prepare_event_snapshot(
                                snapshot_source,
                                event_id=event.eid,
                                participant_id=saved_participant.pid,
                            )
                        )
                        if saved_participant.pid not in snapshot_indexes:
                            snapshot_indexes[saved_participant.pid] = len(event_participants)
                            event_participants.append(snapshot)
                        else:
                            event_participants[snapshot_indexes[saved_participant.pid]] = snapshot

                if event_participants:
                    participant_event_repo.bulk_upsert(
                        event_participants,
                        session=session,
                    )

                event.participants = participant_ids
                event_repo.save(event, session=session)

        refresh_participant_cache()

        return {
            "event": event,
            "participants": saved_participants,
            "participant_events": event_participants,
        }
    except Exception as exc:  # pragma: no cover - defensive rollback
        if isinstance(exc, UploadError):
            raise
        raise UploadError(f"Failed to upload preview data: {exc}") from exc


def _build_event(source: Any) -> Event:
    if isinstance(source, Event):
        return source

    payload = _ensure_mapping(source)

    start_date = coerce_datetime(payload.get("start_date"))
    end_date = coerce_datetime(payload.get("end_date"))

    event_type = payload.get("type")
    if isinstance(event_type, EventType):
        parsed_type = event_type
    elif isinstance(event_type, str) and event_type:
        parsed_type = EventType(event_type)
    else:
        parsed_type = None

    cost_value: Optional[float]
    cost_raw = payload.get("cost")
    if cost_raw in (None, ""):
        cost_value = None
    else:
        cost_value = parse_cost(cost_raw)

    participants = list(payload.get("participants") or [])

    return Event(
        eid=str(payload.get("eid", "")),
        title=str(payload.get("title", "")),
        start_date=start_date,
        end_date=end_date,
        place=str(payload.get("place", "")),
        country=payload.get("country"),
        type=parsed_type,
        cost=cost_value,
        participants=participants,
    )


def _index_event_snapshots(snapshots: Sequence[Any]) -> dict[str, MutableMapping[str, Any]]:
    index: dict[str, MutableMapping[str, Any]] = {}
    for snapshot in snapshots:
        payload = _ensure_mapping(snapshot)
        pid = payload.get("participant_id") or payload.get("pid")
        if not pid:
            continue
        if "traveling_from" not in payload and "travelling_from" in payload:
            payload = dict(payload)
            payload["traveling_from"] = payload.pop("travelling_from")
        index[str(pid)] = payload  # type: ignore[assignment]
    return index


def _extract_event_snapshot(source: Mapping[str, Any]) -> Optional[MutableMapping[str, Any]]:
    candidate_keys = {
        "transportation",
        "transport_other",
        "traveling_from",
        "travelling_from",
        "returning_to",
        "requires_visa_hr",
        "travel_doc_type",
        "travel_doc_number",
        "travel_doc_issue_date",
        "travel_doc_expiry_date",
        "travel_doc_issued_by",
        "bank_name",
        "iban",
        "iban_type",
        "swift",
    }

    if not any(key in source for key in candidate_keys):
        return None

    snapshot = {
        key: value
        for key, value in source.items()
        if key in candidate_keys
    }
    return snapshot


def _prepare_event_snapshot(
    snapshot: Mapping[str, Any],
    *,
    event_id: str,
    participant_id: str,
) -> MutableMapping[str, Any]:
    payload = dict(snapshot)
    payload.setdefault("event_id", event_id)
    payload.setdefault("participant_id", participant_id)
    if "traveling_from" not in payload and "travelling_from" in payload:
        payload["traveling_from"] = payload.pop("travelling_from")
    return payload


def _ensure_mapping(source: Any) -> MutableMapping[str, Any]:
    if isinstance(source, MutableMapping):
        return dict(source)
    if isinstance(source, Mapping):
        return dict(source)
    if hasattr(source, "model_dump"):
        dumped = source.model_dump(mode="python")  # type: ignore[attr-defined]
        if isinstance(dumped, Mapping):
            return dict(dumped)
    if hasattr(source, "__dict__"):
        return dict(vars(source))
    raise TypeError("Expected a mapping-compatible object")
