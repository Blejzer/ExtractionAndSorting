from datetime import datetime

import pytest
from pydantic import ValidationError

from domain.models.event_participant import EventParticipant
from services.upload_service import UploadError, upload_preview_data
from tests.test_upload_service import FakeEventRepo, FakeParticipantRepo, FakeParticipantEventRepo, _base_event, _base_participant
from utils.document_dates import document_date_errors


@pytest.mark.parametrize("issue,expiry", [
    ("2034-03-28", "2024-03-28"),
    ("2024-03-28", "2024-03-28"),
    ("2024-03-28T00:00:00Z", "2024-03-28T23:00:00Z"),
])
def test_expiry_must_be_on_a_later_calendar_date(issue, expiry):
    errors = document_date_errors({"travel_doc_issue_date": issue, "travel_doc_expiry_date": expiry})
    assert set(errors) == {"travel_doc_issue_date", "travel_doc_expiry_date"}
    assert "must be after issue date" in errors["travel_doc_expiry_date"]
    with pytest.raises(ValidationError, match="must be after issue date"):
        EventParticipant.model_validate({"event_id": "EVT", "participant_id": "P0001", "transportation": "Air (Airplane)", "traveling_from": "Zagreb", "returning_to": "Zagreb", "travel_doc_type": "Passport", "travel_doc_issue_date": issue, "travel_doc_expiry_date": expiry})


@pytest.mark.parametrize("issue,expiry", [
    ("2024-03-28", "2034-03-28"),
    ("2024-03-28", "2024-03-29"),
    (None, "2034-03-28"),
    ("2024-03-28", None),
    ("", ""),
    (datetime(2024, 3, 28), datetime(2034, 3, 28)),
])
def test_valid_or_optional_dates_pass(issue, expiry):
    assert not document_date_errors({"travel_doc_issue_date": issue, "travel_doc_expiry_date": expiry})


def test_malformed_dates_are_not_silently_discarded():
    errors = document_date_errors({"travel_doc_issue_date": "2024-02-30"})
    assert "not a valid date" in errors["travel_doc_issue_date"]
    with pytest.raises(ValidationError, match="not a valid date"):
        EventParticipant.model_validate({"travel_doc_issue_date": "2024-02-30"})


def test_date_error_names_participant_and_prevents_all_writes(monkeypatch):
    import services.upload_service as uploads
    def no_session():
        pytest.fail("Invalid document dates must be rejected before opening a transaction")
    monkeypatch.setattr(uploads.mongodb, "start_session", no_session, raising=False)
    events = FakeEventRepo()
    participants = FakeParticipantRepo()
    snapshots = FakeParticipantEventRepo()
    bad = _base_participant(name="Safet Hrapo", travel_doc_issue_date="2034-03-28", travel_doc_expiry_date="2024-03-28")
    with pytest.raises(UploadError) as caught:
        upload_preview_data({"event": _base_event(), "participants": [_base_participant(), bad]}, event_repo=events, participant_repo=participants, participant_event_repo=snapshots)
    message = str(caught.value)
    assert "Safet Hrapo" in message
    assert "2034-03-28" in message and "2024-03-28" in message
    assert "Correct these dates in the preview" in message
    assert "pydantic" not in message
    assert not events.events and not participants.participants and not snapshots.snapshots
    assert participants.counter == 1


def test_checks_separate_snapshot_dates_before_upload():
    with pytest.raises(UploadError, match="must be after issue date"):
        upload_preview_data({"event": _base_event(), "participants": [_base_participant(pid="P1234")], "participant_events": [{"participant_id": "P1234", "travel_doc_issue_date": "2034-03-28", "travel_doc_expiry_date": "2024-03-28"}]}, event_repo=FakeEventRepo(), participant_repo=FakeParticipantRepo(), participant_event_repo=FakeParticipantEventRepo())


def test_preview_flags_bad_dates_and_saves_corrections_to_snapshot(tmp_path, monkeypatch):
    import json
    from pathlib import Path
    from flask import Flask
    from domain.models.event import Event
    from utils.participants import initialize_cache
    initialize_cache(None)
    import routes.imports as routes
    monkeypatch.setattr(routes, "ParticipantRepository", FakeParticipantRepo)
    app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / "templates"))
    app.config.update(TESTING=True, LOGIN_DISABLED=True, UPLOADS_DIR=str(tmp_path), SECRET_KEY="test")
    app.register_blueprint(routes.imports_bp)
    for endpoint in ("main.show_home", "participants.show_participants", "events.show_events", "auth.login"):
        app.add_url_rule(f"/test/{endpoint}", endpoint=endpoint, view_func=lambda: "")
    bad = _base_participant(pid="P1234", name="Safet Hrapo", travel_doc_issue_date="2034-03-28", travel_doc_expiry_date="2024-03-28")
    snapshot = {"participant_id": "P1234", "travel_doc_issue_date": "2034-03-28", "travel_doc_expiry_date": "2024-03-28"}
    path = tmp_path / "dates.preview.json"
    path.write_text(json.dumps({"event": _base_event(), "participants": [bad], "participant_events": [snapshot]}))
    with app.test_client() as client:
        response = client.get("/imports/preview/dates.preview.json")
        html = response.get_data(as_text=True)
        assert "Check document dates" in html
        assert "Safet Hrapo" in html
        assert "2034-03-28" in html and "2024-03-28" in html
        assert 'is-invalid" id="participant_0_travel_doc_issue_date"' in html
        assert 'is-invalid" id="participant_0_travel_doc_expiry_date"' in html
        response = client.post("/imports/preview/dates.preview.json", data={"participants[0][travel_doc_issue_date]": "2024-03-28", "participants[0][travel_doc_expiry_date]": "2034-03-28", "upload_now": "0"})
        assert response.status_code == 302
        html = client.get("/imports/preview/dates.preview.json").get_data(as_text=True)
        assert "Check document dates" not in html
    saved = json.loads(path.read_text())
    assert saved["participant_events"][0]["travel_doc_issue_date"] == "2024-03-28"
    assert saved["participant_events"][0]["travel_doc_expiry_date"] == "2034-03-28"
    assert "_date_errors" not in saved["participants"][0]


def test_valid_dates_are_preserved_on_event_snapshot():
    snapshot = EventParticipant.model_validate({"event_id": "EVT", "participant_id": "P0001", "transportation": "Air (Airplane)", "traveling_from": "Zagreb", "returning_to": "Zagreb", "travel_doc_type": "Passport", "travel_doc_issue_date": "2024-03-28", "travel_doc_expiry_date": "2034-03-28"})
    assert snapshot.travel_doc_issue_date.date().isoformat() == "2024-03-28"
    assert snapshot.travel_doc_expiry_date.date().isoformat() == "2034-03-28"
