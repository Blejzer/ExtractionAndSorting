from contextlib import nullcontext
from datetime import datetime
import json
from pathlib import Path

import pytest
from flask import Flask

from domain.models.participant import Participant
from services.imports.participant_review import annotate_participant_reviews
from services.upload_service import upload_preview_data
from tests.test_upload_service import FakeParticipantRepo, FakeEventRepo, FakeParticipantEventRepo, _base_participant, _base_event


class ReturningRepo(FakeParticipantRepo):
    def find_by_name_dob_and_representing_country_cid(self, *, name, dob, representing_country):
        for person in self.participants.values():
            if person.name == name and person.representing_country == representing_country:
                if not dob or not person.dob or person.dob == dob:
                    return person
        return None


@pytest.fixture
def returning_repo():
    repo = ReturningRepo()
    person = Participant.model_validate(_base_participant(
        pid="P0814", phone="+385999999", organization="Stored organization",
        created_at="2020-01-01", _audit=[{"field": "grade", "to": 1}],
    ))
    repo.participants[person.pid] = person
    return repo


@pytest.fixture(autouse=True)
def isolated_upload(monkeypatch):
    import services.upload_service as uploads

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def start_transaction(self):
            return nullcontext()

    monkeypatch.setattr(uploads.mongodb, "start_session", Session, raising=False)
    monkeypatch.setattr(uploads, "refresh_participant_cache", lambda: None)


def test_returning_participant_missing_stored_dob_is_reviewed_not_conflicted(returning_repo):
    person = returning_repo.participants["P0814"]
    returning_repo.participants[person.pid] = person.model_copy(update={"dob": None})
    record = annotate_participant_reviews([_base_participant()], returning_repo)[0]
    assert record["_review"]["pid"] == "P0814"
    assert record["_changes"]["dob"] == {"stored": None, "file": "1990-01-01"}
    assert "_conflict" not in record


def test_equal_dates_and_normalized_names_are_not_changes(returning_repo):
    record = annotate_participant_reviews([_base_participant(dob="1990-01-01T14:00:00+02:00")], returning_repo)[0]
    assert "dob" not in record["_changes"]
    assert "name" not in record["_changes"]
    assert record["_changes"]["phone"]["stored"] == "+385999999"


@pytest.mark.parametrize("overrides", [{"dob": "1991-01-01"}, {"representing_country": "BA"}, {"name": "Someone Else"}])
def test_different_identity_is_new_and_does_not_trust_pid(returning_repo, overrides):
    record = annotate_participant_reviews([_base_participant(pid="P0814", **overrides)], returning_repo)[0]
    assert "_review" not in record


def test_upload_applies_only_selected_fields_preserving_pid_and_history(returning_repo):
    old = returning_repo.participants["P0814"]
    record = annotate_participant_reviews([_base_participant(phone="+385111111", organization="File organization")], returning_repo)[0]
    record["_review"]["accepted_fields"] = ["phone"]
    snapshots = FakeParticipantEventRepo()
    from domain.models.event_participant import EventParticipant
    previous = EventParticipant.model_validate({"participant_id": "P0814", "event_id": "OLD", "travel_doc_type": "Passport", "transportation": "Air (Airplane)", "traveling_from": "Zagreb", "returning_to": "Zagreb"})
    snapshots.snapshots.append(previous)
    events = FakeEventRepo()
    result = upload_preview_data({"event": _base_event(), "participants": [record]}, event_repo=events, participant_repo=returning_repo, participant_event_repo=snapshots)
    updated = returning_repo.participants["P0814"]
    assert updated.phone == "+385111111"
    assert updated.organization == old.organization
    assert updated.audit == old.audit
    assert updated.created_at == old.created_at
    assert list(returning_repo.participants) == ["P0814"]
    assert returning_repo.counter == 1
    assert result["event"].participants == ["P0814"]
    assert snapshots.snapshots[0] is previous
    assert snapshots.snapshots[1].participant_id == "P0814"
    assert snapshots.snapshots[1].event_id == "EVT-001"


def test_no_selected_changes_still_adds_new_event_and_keeps_legacy_dob(returning_repo):
    person = returning_repo.participants["P0814"].model_copy(update={"dob": None})
    returning_repo.participants[person.pid] = person
    record = annotate_participant_reviews([_base_participant()], returning_repo)[0]
    record["_review"]["accepted_fields"] = []
    upload_preview_data({"event": _base_event(), "participants": [record]}, event_repo=FakeEventRepo(), participant_repo=returning_repo, participant_event_repo=FakeParticipantEventRepo())
    assert returning_repo.participants[person.pid] is person


def test_selected_identity_correction_keeps_internal_pid(returning_repo):
    record = annotate_participant_reviews([_base_participant()], returning_repo)[0]
    record["name"] = "Jane Smith"
    record["_review"]["accepted_fields"] = ["name"]
    result = upload_preview_data({"event": _base_event(), "participants": [record]}, event_repo=FakeEventRepo(), participant_repo=returning_repo, participant_event_repo=FakeParticipantEventRepo())
    assert result["participants"][0].pid == "P0814"
    assert result["participants"][0].name == "Jane SMITH"


def test_selected_empty_value_clears_field(returning_repo):
    record = annotate_participant_reviews([_base_participant(organization=None)], returning_repo)[0]
    record["_review"]["accepted_fields"] = ["organization"]
    upload_preview_data({"event": _base_event(), "participants": [record]}, event_repo=FakeEventRepo(), participant_repo=returning_repo, participant_event_repo=FakeParticipantEventRepo())
    assert returning_repo.participants["P0814"].organization is None


def test_saved_review_revalidates_original_match_before_writing(returning_repo):
    from services.upload_service import UploadError
    record = annotate_participant_reviews([_base_participant()], returning_repo)[0]
    record["_review"]["accepted_fields"] = ["phone"]
    returning_repo.participants.clear()
    events = FakeEventRepo()
    with pytest.raises(UploadError, match="match changed"):
        upload_preview_data({"event": _base_event(), "participants": [record]}, event_repo=events, participant_repo=returning_repo, participant_event_repo=FakeParticipantEventRepo())
    assert not events.events
    assert returning_repo.counter == 1


def test_invalid_unselected_file_value_can_be_reviewed_and_ignored(returning_repo):
    record = annotate_participant_reviews([_base_participant(email="invalid")], returning_repo)[0]
    record["_review"]["accepted_fields"] = []
    old_email = returning_repo.participants["P0814"].email
    upload_preview_data({"event": _base_event(), "participants": [record]}, event_repo=FakeEventRepo(), participant_repo=returning_repo, participant_event_repo=FakeParticipantEventRepo())
    assert returning_repo.participants["P0814"].email == old_email


def test_preview_highlights_changes_and_persists_individual_choices(tmp_path, monkeypatch, returning_repo):
    from utils.participants import initialize_cache
    initialize_cache(None)
    import routes.imports as routes
    monkeypatch.setattr(routes, "ParticipantRepository", lambda: returning_repo)
    app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / "templates"))
    app.config.update(TESTING=True, LOGIN_DISABLED=True, UPLOADS_DIR=str(tmp_path), SECRET_KEY="test")
    app.register_blueprint(routes.imports_bp)
    for endpoint in ("main.show_home", "participants.show_participants", "events.show_events", "auth.login"):
        app.add_url_rule(f"/test/{endpoint}", endpoint=endpoint, view_func=lambda: "")
    path = tmp_path / "returning.preview.json"
    path.write_text(json.dumps({"event": _base_event(), "participants": [_base_participant(phone="+385111111", organization="File organization")]}))
    with app.test_client() as client:
        response = client.get("/imports/preview/returning.preview.json")
        html = response.get_data(as_text=True)
        assert response.status_code == 200
        assert "Returning participant: P0814" in html
        assert "PID conflict" not in html
        assert "bg-warning-subtle" in html
        assert "+385999999" in html
        assert 'name="accept[0][phone]"' in html
        response = client.post("/imports/preview/returning.preview.json", data={"accept[0][phone]": "1", "participants[0][phone]": "+385111111", "upload_now": "0"})
        assert response.status_code == 302
    saved = json.loads(path.read_text())["participants"][0]
    assert saved["_review"]["accepted_fields"] == ["phone"]
    assert saved["_review"]["pid"] == "P0814"
    assert "_changes" not in saved
    assert "_conflict" not in saved
