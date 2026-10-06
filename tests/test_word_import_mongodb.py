"""Optional real transaction checks against a disposable local replica set."""

import os
from uuid import uuid4

import pytest
from pymongo import MongoClient

from domain.models.participant import Participant
from repositories.event_repository import EventRepository
from repositories.participant_event_repository import ParticipantEventRepository
from repositories.participant_repository import ParticipantRepository
from services.imports.participant_review import annotate_participant_reviews
from services.upload_service import UploadError, upload_preview_data
from tests.test_upload_service import _base_participant


@pytest.fixture
def mongo(monkeypatch):
    uri = os.getenv("WORD_IMPORT_TEST_MONGO_URI")
    if not uri:
        pytest.skip("Set WORD_IMPORT_TEST_MONGO_URI to a disposable local MongoDB replica set.")
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = client["word_import_test_" + uuid4().hex]
    class Connection:
        def collection(self, name):
            return db[name]
        def start_session(self):
            return client.start_session()
    import repositories.event_repository as event_module
    import repositories.participant_repository as people_module
    import repositories.participant_event_repository as snapshot_module
    import services.upload_service as uploads
    for module in (event_module, people_module, snapshot_module, uploads):
        monkeypatch.setattr(module, "mongodb", Connection())
    monkeypatch.setattr(uploads, "refresh_participant_cache", lambda: None)
    people, events, snapshots = ParticipantRepository(), EventRepository(), ParticipantEventRepository()
    people.ensure_indexes()
    events.ensure_indexes()
    snapshots.ensure_indexes()
    db.events.insert_one({"eid": "EXISTING", "title": "Retain this title", "participant_ids": ["POLD"], "training_areas": ["narcotics"], "custom_field": "retain"})
    old = Participant.model_validate(_base_participant(pid="P0814"))
    people.save(old)
    try:
        yield db, people, events, snapshots
    finally:
        client.drop_database(db.name)
        client.close()


def test_real_transaction_preserves_legacy_roster_metadata_and_partial_snapshots(mongo):
    db, people, events, snapshots = mongo
    db.participant_events.insert_one({"participant_id": "P0814", "event_id": "EXISTING", "bank_name": "Keep bank", "transportation": "Government (Official) Vehicle (GOV)"})
    rows = annotate_participant_reviews([_base_participant(travel_doc_number="001234")], people)
    rows[0]["_review"]["accepted_fields"] = []
    # Missing travel fields are deliberately not manufactured by Word import.
    for field in ("transportation", "traveling_from", "returning_to", "travel_doc_type"):
        rows[0].pop(field, None)
    rows[0]["_word_source"] = {"file": "synthetic.csv", "fields": {"service_number": "0099"}}
    result = upload_preview_data({"event": {"eid": "EXISTING"}, "participants": rows}, event_repo=events, participant_repo=people, participant_event_repo=snapshots, existing_event_id="EXISTING", partial_snapshots=True)
    event = db.events.find_one({"eid": "EXISTING"})
    assert event["participants"] == ["POLD", "P0814"]
    assert event["participant_ids"] == ["POLD"]
    assert event["custom_field"] == "retain" and event["training_areas"] == ["narcotics"]
    assert event["title"] == "Retain this title"
    snapshot = db.participant_events.find_one({"participant_id": "P0814", "event_id": "EXISTING"})
    assert snapshot["bank_name"] == "Keep bank"
    assert snapshot["transportation"] == "Government (Official) Vehicle (GOV)"
    assert snapshot["travel_doc_number"] == "001234"
    assert "traveling_from" not in snapshot
    assert snapshot["word_import_sources"][0]["fields"]["service_number"] == "0099"
    assert result["participants"][0].pid == "P0814"
    upload_preview_data({"event": {"eid": "EXISTING"}, "participants": rows}, event_repo=events, participant_repo=people, participant_event_repo=snapshots, existing_event_id="EXISTING", partial_snapshots=True)
    assert db.participants.count_documents({}) == 1
    assert db.participant_events.count_documents({}) == 1
    assert len(db.participant_events.find_one({})["word_import_sources"]) == 1


def test_real_transaction_rolls_back_profile_new_pid_snapshots_and_roster(mongo):
    db, people, events, snapshots = mongo
    rows = annotate_participant_reviews([_base_participant(phone="+385111111"), _base_participant(name="New PERSON", dob="1981-03-26")], people)
    rows[0]["_review"]["accepted_fields"] = ["phone"]
    original = snapshots.upsert_partial
    count = 0
    def fail_after_write(*args, **kwargs):
        nonlocal count
        original(*args, **kwargs)
        count += 1
        if count == 2:
            raise RuntimeError("Synthetic write failure")
    snapshots.upsert_partial = fail_after_write
    with pytest.raises(UploadError, match="Synthetic write failure"):
        upload_preview_data({"event": {"eid": "EXISTING"}, "participants": rows}, event_repo=events, participant_repo=people, participant_event_repo=snapshots, existing_event_id="EXISTING", partial_snapshots=True)
    assert db.participants.count_documents({}) == 1
    assert db.participants.find_one({"pid": "P0814"})["phone"] == "+385123456"
    assert db.counters.count_documents({}) == 0
    assert db.participant_events.count_documents({}) == 0
    assert "participants" not in db.events.find_one({"eid": "EXISTING"})


def test_new_profile_with_unstated_birth_fields_can_be_loaded_and_matched_again(mongo):
    db, people, events, snapshots = mongo
    row = {"name": "New PERSON", "dob": "1981-03-26", "representing_country": "HR", "gender": "Male"}
    result = upload_preview_data({"event": {"eid": "EXISTING"}, "participants": [row]}, event_repo=events, participant_repo=people, participant_event_repo=snapshots, existing_event_id="EXISTING", partial_snapshots=True)
    saved = result["participants"][0]
    reloaded = people.find_by_pid(saved.pid)
    assert reloaded.pob is None and reloaded.birth_country is None
    reviewed = annotate_participant_reviews([row], people)[0]
    assert reviewed["_review"]["pid"] == saved.pid
