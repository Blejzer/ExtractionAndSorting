# services/upload_service.py

"""Helpers for uploading parsed import previews into MongoDB."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Mapping, MutableMapping, Optional, Sequence

from domain.models.event import Event, EventType
from domain.models.event_participant import EventParticipant
from domain.models.participant import Participant
from repositories.event_repository import EventRepository
from repositories.participant_event_repository import ParticipantEventRepository
from repositories.participant_repository import ParticipantRepository


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

    event = _build_event(event_source)
    if not event.eid:
        raise UploadError("Event is missing an eid")

    if event_repo.find_by_eid(event.eid):
        raise UploadError(f"Event '{event.eid}' has already been uploaded")

    participants_source = bundle.get("participants") or []
    participant_events_source = bundle.get("participant_events") or []

    participant_snapshot_index = _index_event_snapshots(participant_events_source)
    prepared_snapshots: dict[str, MutableMapping[str, Any]] = {}

    saved_participants: list[Participant] = []
    participant_ids: list[str] = []

    for participant_source in participants_source:
        participant_dict = _ensure_mapping(participant_source)

        probe_payload = dict(participant_dict)
        probe_payload.setdefault("pid", participant_dict.get("pid") or "TEMP")
        participant_probe = Participant.model_validate(probe_payload)

        existing = participant_repo.find_by_name_dob_and_representing_country_cid(
            name=participant_probe.name,
            dob=participant_probe.dob,
            representing_country=participant_probe.representing_country,
        )

        pid = participant_dict.get("pid") or (existing.pid if existing else None)
        if not pid:
            pid = participant_repo.generate_next_pid()

        participant_payload = dict(participant_dict)
        participant_payload["pid"] = pid
        participant_model = Participant.model_validate(participant_payload)

        if existing:
            update_payload = participant_model.to_mongo()
            update_payload.pop("pid", None)
            updated = participant_repo.update(existing.pid, update_payload)
            saved_participant = updated or participant_model
        else:
            participant_repo.save(participant_model)
            saved_participant = participant_model

        saved_participants.append(saved_participant)
        participant_ids.append(saved_participant.pid)

        snapshot_source = participant_snapshot_index.get(saved_participant.pid)
        if not snapshot_source:
            snapshot_source = _extract_event_snapshot(participant_dict)
        if snapshot_source:
            prepared_snapshots[saved_participant.pid] = _prepare_event_snapshot(
                snapshot_source,
                event_id=event.eid,
                participant_id=saved_participant.pid,
            )

    event_participants: list[EventParticipant] = []
    if prepared_snapshots:
        for payload in prepared_snapshots.values():
            event_participants.append(EventParticipant.model_validate(dict(payload)))
        participant_event_repo.bulk_upsert(event_participants)

    event.participants = participant_ids
    event_repo.save(event)

    return {
        "event": event,
        "participants": saved_participants,
        "participant_events": event_participants,
    }
def _build_event(source: Any) -> Event:
    if isinstance(source, Event):
        return source

    payload = _ensure_mapping(source)

    start_date = payload.get("start_date")
    if isinstance(start_date, str) and start_date:
        try:
            start_date = datetime.fromisoformat(start_date)
        except ValueError:
            pass

    end_date = payload.get("end_date")
    if isinstance(end_date, str) and end_date:
        try:
            end_date = datetime.fromisoformat(end_date)
        except ValueError:
            pass

    event_type = payload.get("type")
    if isinstance(event_type, EventType):
        parsed_type = event_type
    elif isinstance(event_type, str) and event_type:
        try:
            parsed_type = EventType(event_type)
        except ValueError:
            parsed_type = EventType.other
    else:
        parsed_type = None

    cost_value: Optional[float]
    cost_raw = payload.get("cost")
    if cost_raw is None:
        cost_value = None
    else:
        try:
            cost_value = float(cost_raw)
        except (TypeError, ValueError):
            cost_value = None

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
        "travel_doc_type_other",
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