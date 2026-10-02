import json
from pathlib import Path

import pytest
from flask import Flask

from domain.models.event_participant import Transport
from domain.models.participant import Participant
from services.upload_service import UploadError, upload_preview_data, upload_preview_file
from tests.test_upload_service import (
    FakeEventRepo, FakeParticipantRepo, FakeParticipantEventRepo,
    _base_event, _base_participant, isolated_upload_session,
)
from utils.transportation import transportation_errors


@pytest.mark.parametrize("value", ["Bus", "Train", "Boat", "", None])
def test_unsupported_transportation_requires_manual_correction(value):
    record = {"transportation": value}
    errors = transportation_errors(record)
    assert "transportation" in errors
    assert "select Other" in errors["transportation"]
    assert record["transportation"] == value


@pytest.mark.parametrize("value", [transport.value for transport in Transport])
def test_accepted_transportation_is_unchanged(value):
    record = {"transportation": value, "transport_other": "Bus"}
    assert transportation_errors(record) == {}
    assert record["transportation"] == value


@pytest.mark.parametrize("details", [None, "", "   "])
def test_other_requires_details(details):
    assert "transport_other" in transportation_errors({"transportation": "Other", "transport_other": details})


def test_error_names_participant_and_rejects_before_any_writes(monkeypatch):
    import services.upload_service as uploads
    monkeypatch.setattr(uploads.mongodb, "start_session", lambda: pytest.fail("Must validate before writing"), raising=False)
    events, participants, snapshots = FakeEventRepo(), FakeParticipantRepo(), FakeParticipantEventRepo()
    with pytest.raises(UploadError) as caught:
        upload_preview_data(
            {"event": _base_event(), "participants": [_base_participant(), _base_participant(name="Alex Traveller", transportation="Bus")]},
            event_repo=events, participant_repo=participants, participant_event_repo=snapshots,
        )
    message = str(caught.value)
    assert "Alex Traveller" in message and "Bus" in message and "select Other" in message
    assert "pydantic" not in message and "type=enum" not in message
    assert not events.events and not participants.participants and not snapshots.snapshots
    assert participants.counter == 1


@pytest.mark.parametrize("separate_snapshot", [False, True])
def test_preview_marks_bus_and_manual_other_correction_uploads(tmp_path, monkeypatch, separate_snapshot):
    from utils.participants import initialize_cache
    initialize_cache(None)
    import routes.imports as routes
    events, participants, snapshots = FakeEventRepo(), FakeParticipantRepo(), FakeParticipantEventRepo()
    existing = Participant.model_validate(_base_participant(pid="P1234"))
    participants.participants[existing.pid] = existing
    monkeypatch.setattr(routes, "ParticipantRepository", lambda: participants)
    monkeypatch.setattr(routes, "upload_preview_file", lambda path: upload_preview_file(
        path, event_repo=events, participant_repo=participants, participant_event_repo=snapshots,
    ))
    app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / "templates"))
    app.config.update(TESTING=True, LOGIN_DISABLED=True, UPLOADS_DIR=str(tmp_path), SECRET_KEY="test")
    app.register_blueprint(routes.imports_bp)
    for endpoint in ("main.show_home", "participants.show_participants", "events.show_events", "auth.login"):
        app.add_url_rule(f"/test/{endpoint}", endpoint=endpoint, view_func=lambda: "")
    participant = _base_participant(pid="P1234", transportation="Bus")
    snapshot = {key: participant[key] for key in ("transportation", "traveling_from", "returning_to", "travel_doc_type")}
    snapshot["participant_id"] = "P1234"
    bundle = {"event": _base_event(), "participants": [participant], "participant_events": [snapshot] if separate_snapshot else []}
    path = tmp_path / "transport.preview.json"
    path.write_text(json.dumps(bundle))
    with app.test_client() as client:
        html = client.get("/imports/preview/transport.preview.json").get_data(as_text=True)
        assert "Check transportation" in html and "select Other" in html
        assert 'border-warning bg-warning-subtle" id="participant_0_transportation"' in html
        assert 'value="Bus"' in html and 'list="transportChoices"' in html
        assert 'name="participants[0][transport_other]"' in html
        assert json.loads(path.read_text()) == bundle
        response = client.post("/imports/preview/transport.preview.json", data={
            "participants[0][transportation]": "Other", "participants[0][transport_other]": "Bus", "upload_now": "1",
        })
        assert response.status_code == 302
        assert response.location.endswith("/test/events.show_events")
    saved = json.loads(path.read_text())
    assert saved["participants"][0]["transportation"] == "Other"
    assert saved["participants"][0]["transport_other"] == "Bus"
    assert "_transport_errors" not in saved["participants"][0]
    if separate_snapshot:
        assert saved["participant_events"][0]["transportation"] == "Other"
        assert saved["participant_events"][0]["transport_other"] == "Bus"
    assert list(participants.participants) == ["P1234"] and participants.counter == 1
    assert events.events["EVT-001"].participants == ["P1234"]
    assert snapshots.snapshots[0].transportation == Transport.other
    assert snapshots.snapshots[0].transport_other == "Bus"
