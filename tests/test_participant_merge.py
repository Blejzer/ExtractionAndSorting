"""Merge planning, authenticated review, and optional real MongoDB transactions."""

from copy import deepcopy
from datetime import datetime
import os
import re
import uuid

from flask import Flask
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, OperationFailure
import pytest

import routes.participant_merges as routes
import services.participant_merge_service as merges
from routes.participants import participants_bp
from middleware.handlers import register_error_handlers


PIDS = ["P0025", "P0879"]


def example_state():
    return {
        "profiles": [
            {"_id": "profile-old", "pid": "P0025", "name": "Suela BONJAKU", "representing_country": "C001",
             "pob": None, "birth_country": None,
             "gender": "Female", "position": "Specialist in Cyber Crime Sector", "organization": "",
             "grade": 0, "intl_authority": False, "citizenships": ["C001"],
             "created_at": datetime(2016, 1, 1), "_audit": [{"actor": "import", "field": "grade"}]},
            {"_id": "profile-new", "pid": "P0879", "name": "Suela BONJAKU", "representing_country": "C001",
             "pob": None, "birth_country": None,
             "gender": "Female", "position": "Specialist", "organization": "Police",
             "grade": 1, "intl_authority": True, "citizenships": ["C001", "C054"],
             "custom_field": "Preserve legacy fields", "_audit": [{"actor": "editor", "field": "position"}]},
        ],
        "links": [
            {"_id": "link-1", "participant_id": "P0025", "event_id": "PFE17M3", "traveling_from": "Tirana", "bank_name": "Old bank"},
            {"_id": "link-2", "participant_id": "P0879", "event_id": "PFE17M3", "traveling_from": "Tirana", "bank_name": "New bank", "iban": "event-specific-account"},
            *[{"_id": f"link-{i}", "participant_id": "P0025", "event_id": eid}
              for i, eid in enumerate(["PFE16M3", "PFE18M4", "PFE22M9"], 3)],
        ],
        "events": [{"_id": f"event-{eid}", "eid": eid, "title": eid, "participants": roster,
                    "place": "Zagreb", "start_date": datetime(2017, 1, 1), "end_date": datetime(2017, 1, 5)}
                   for eid, roster in [
                       ("PFE17M3", ["P0025", "P0879", "P0100"]),
                       ("PFE16M3", ["P0025"]), ("PFE18M4", ["P0025"]), ("PFE22M9", ["P0025"]),
                   ]],
        "tests": [
            {"_id": "test-1", "pid": "P0025", "eid": "PFE17M3", "type": "pre", "score": 0},
            {"_id": "test-2", "pid": "P0879", "eid": "PFE17M3", "type": "pre", "score": 80},
            {"_id": "test-3", "pid": "P0879", "eid": "PFE17M3", "type": "post", "score": 90},
        ],
    }


def choices_for(service, state, target="P0025"):
    return {c["key"]: 0 for c in service.plan(state, target)["conflicts"]}


def test_suela_has_four_events_and_explicit_profile_snapshot_and_score_conflicts():
    state = example_state()
    original = deepcopy(state)
    service = merges.ParticipantMergeService()
    plan = service.plan(state, "P0025")
    assert len(plan["attendance"]) == 4
    assert next(e for e in plan["attendance"] if e["eid"] == "PFE17M3")["sources"] == PIDS
    assert {(c["scope"], c["field"]) for c in plan["conflicts"]} == {
        ("Profile", "position"), ("Profile", "grade"), ("Profile", "intl_authority"),
        ("Event PFE17M3", "bank_name"), ("Test PFE17M3 (pre)", "score"),
    }
    assert plan["profile"]["organization"] == "Police"
    assert plan["profile"]["custom_field"] == "Preserve legacy fields"
    assert plan["profile"]["citizenships"] == ["C001", "C054"]
    assert state == original


def test_choices_keep_zero_false_and_event_banking_never_spreads_to_other_events():
    service, state = merges.ParticipantMergeService(), example_state()
    choices = choices_for(service, state)
    plan = service.plan(state, "P0025", choices, require_choices=True)
    assert plan["profile"]["grade"] == 0
    assert plan["profile"]["intl_authority"] is False
    assert next(d for d in plan["tests"] if d["type"] == "pre")["score"] == 0
    assert next(d for d in plan["links"] if d["event_id"] == "PFE17M3")["iban"] == "event-specific-account"
    assert all("iban" not in d for d in plan["links"] if d["event_id"] != "PFE17M3")
    assert {d["merge_source_pid"] for d in plan["profile"]["_audit"]} == set(PIDS)


def test_user_can_choose_newer_pid_and_each_source_value():
    service, state = merges.ParticipantMergeService(), example_state()
    choices = {c["key"]: 1 for c in service.plan(state, "P0879")["conflicts"]}
    plan = service.plan(state, "P0879", choices, require_choices=True)
    assert plan["profile"]["_id"] == "profile-new"
    assert plan["profile"]["position"] == "Specialist in Cyber Crime Sector"
    assert all(d["participant_id"] == "P0879" for d in plan["links"])
    assert all(d["pid"] == "P0879" for d in plan["tests"])


@pytest.mark.parametrize("choices", [{}, {"choice_0": -1}, {"choice_0": 100}, {"choice_0": True}, {"choice_0": "0"}, {"evil": 0}])
def test_missing_or_invalid_choices_are_rejected(choices):
    with pytest.raises(merges.MergeError):
        merges.ParticipantMergeService().plan(example_state(), "P0025", choices, require_choices=True)


def test_roster_only_legacy_attendance_is_kept():
    state = example_state()
    state["events"].append({"_id": "legacy-event", "eid": "LEGACY", "participant_ids": ["P0879"]})
    plan = merges.ParticipantMergeService().plan(state, "P0025")
    assert {"event_id": "LEGACY", "participant_id": "P0025"} in plan["links"]


@pytest.fixture
def route_client(monkeypatch):
    state = example_state()
    service = merges.ParticipantMergeService()
    monkeypatch.setattr(service, "snapshot", lambda pids: deepcopy(state))
    monkeypatch.setattr(service, "candidates", lambda search: state["profiles"])
    calls = []
    monkeypatch.setattr(service, "merge", lambda *args: calls.append(args))
    monkeypatch.setattr(routes, "_service", service)
    monkeypatch.setattr(routes, "get_country_lookup", lambda: {"C001": "Albania"})
    app = Flask(__name__, template_folder="../templates")
    app.secret_key = "test-secret"
    app.config["TESTING"] = True
    register_error_handlers(app)
    app.register_blueprint(participants_bp)
    app.register_blueprint(routes.participant_merges_bp)
    for endpoint in ("main.show_home", "auth.login", "auth.logout", "events.show_events", "imports.upload_form"):
        app.add_url_rule("/stub/" + endpoint, endpoint=endpoint, view_func=lambda: "stub")
    client = app.test_client()
    with client.session_transaction() as session:
        session["username"] = "reviewer"
    return client, state, calls


def form_value(response, name):
    return re.search(fr'name="{name}" value="([^"]+)"', response.get_data(as_text=True)).group(1)


def preview(client):
    page = client.get("/participants/merge")
    csrf = form_value(page, "csrf_token")
    response = client.post("/participants/merge/preview", data={"csrf_token": csrf, "pids": PIDS, "target_pid": "P0025"})
    assert response.status_code == 200
    return csrf, form_value(response, "preview_token"), response


def review(client, state):
    csrf, token, _ = preview(client)
    response = client.post("/participants/merge/review", data={
        "csrf_token": csrf, "preview_token": token,
        **choices_for(merges.ParticipantMergeService(), state),
    })
    assert response.status_code == 200
    return csrf, form_value(response, "preview_token"), response


def test_page_and_full_review_flow_never_mutate_until_confirmation(route_client):
    client, state, calls = route_client
    csrf, token, response = review(client, state)
    assert b"Final merged values" in response.data
    assert b"Specialist in Cyber Crime Sector" in response.data
    assert b"Counted once" in response.data
    assert not calls
    response = client.post("/participants/merge/confirm", data={"csrf_token": csrf, "preview_token": token, "same_person": "yes"})
    assert response.status_code == 302
    assert response.location.endswith("/participant/P0025")
    assert calls[0][-1] == "reviewer"
    # Replay requires a new selection/review, even in the same browser session.
    assert client.post("/participants/merge/confirm", data={"csrf_token": csrf, "preview_token": token, "same_person": "yes"}).status_code == 400


@pytest.mark.parametrize("url", ["/participants/merge", "/participants/merge/preview", "/participants/merge/review", "/participants/merge/confirm"])
def test_all_merge_steps_require_login(route_client, url):
    client, _, calls = route_client
    with client.session_transaction() as session:
        session.clear()
    response = client.get(url) if url == "/participants/merge" else client.post(url)
    assert response.status_code == 302
    assert "/stub/auth.login" in response.location
    assert not calls


def test_missing_csrf_and_forged_preview_are_rejected(route_client):
    client, _, calls = route_client
    assert client.post("/participants/merge/preview", data={"pids": PIDS, "target_pid": "P0025"}).status_code == 400
    csrf, token, _ = preview(client)
    assert client.post("/participants/merge/review", data={"csrf_token": csrf, "preview_token": token + "invalid"}).status_code == 400
    assert not calls


def test_cross_session_preview_is_rejected(route_client):
    client, _, calls = route_client
    _, token, _ = preview(client)
    with client.session_transaction() as session:
        session["participant_merge_csrf"] = "another-session-token"
    assert client.post("/participants/merge/review", data={"csrf_token": "another-session-token", "preview_token": token}).status_code == 400
    assert not calls


def test_preview_expiry_and_wrong_stage_are_rejected(route_client, monkeypatch):
    client, _, calls = route_client
    csrf, token, _ = preview(client)
    assert client.post("/participants/merge/confirm", data={"csrf_token": csrf, "preview_token": token, "same_person": "yes"}).status_code == 400
    import itsdangerous.timed
    clock = itsdangerous.timed.time.time
    monkeypatch.setattr(itsdangerous.timed.time, "time", lambda: clock() + 1801)
    assert client.post("/participants/merge/review", data={"csrf_token": csrf, "preview_token": token}).status_code == 400
    assert not calls


def test_changed_data_requires_a_new_review(route_client):
    client, state, calls = route_client
    csrf, token, _ = preview(client)
    state["profiles"][0]["position"] = "Edited after preview"
    assert client.post("/participants/merge/review", data={"csrf_token": csrf, "preview_token": token}).status_code == 400
    assert not calls


def test_confirmation_checkbox_required_and_database_error_does_not_leak_credentials(route_client, monkeypatch):
    client, state, calls = route_client
    csrf, token, _ = review(client, state)
    assert client.post("/participants/merge/confirm", data={"csrf_token": csrf, "preview_token": token}).status_code == 400
    def failure(*args):
        raise OperationFailure("mongodb://private-user:private-password@host")
    monkeypatch.setattr(routes._service, "merge", failure)
    response = client.post("/participants/merge/confirm", data={"csrf_token": csrf, "preview_token": token, "same_person": "yes"})
    assert response.status_code == 503
    assert b"private-password" not in response.data
    assert not calls


@pytest.fixture
def mongo_db():
    """Opt in with the URI of a disposable local replica set; never use application DB credentials."""
    uri = os.getenv("MERGE_TEST_MONGODB_URI")
    if not uri:
        pytest.skip("Set MERGE_TEST_MONGODB_URI for real transaction checks")
    assert uri.startswith(("mongodb://127.0.0.1:", "mongodb://localhost:"))
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    name = "participant_merge_test_" + uuid.uuid4().hex
    database = client[name]
    class Connection:
        def collection(self, name):
            return database[name]
        def start_session(self):
            return client.start_session()
    for collection, key in (("participants", "profiles"), ("participant_events", "links"), ("events", "events"), ("tests", "tests")):
        database[collection].insert_many(example_state()[key])
    database.participants.create_index("pid", unique=True)
    database.participant_events.create_index([("participant_id", 1), ("event_id", 1)], unique=True)
    database.tests.create_index([("eid", 1), ("pid", 1), ("type", 1)], unique=True)
    try:
        yield Connection(), database
    finally:
        client.drop_database(name)
        client.close()


def test_real_transaction_preserves_every_reference_archive_and_indexes(mongo_db):
    connection, db = mongo_db
    service = merges.ParticipantMergeService(connection)
    state = service.snapshot(PIDS)
    merge_id = service.merge(PIDS, "P0025", merges.fingerprint(state), choices_for(service, state), "reviewer")
    assert db.participants.count_documents({}) == 1
    assert db.participant_events.count_documents({"participant_id": "P0025"}) == 4
    assert db.tests.count_documents({"pid": "P0025"}) == 2
    assert db.tests.find_one({"type": "pre"})["score"] == 0
    assert db.events.find_one({"eid": "PFE17M3"})["participants"] == ["P0025", "P0100"]
    archive = db.participant_merges.find_one({"_id": merge_id})
    assert archive["originals"] == state
    assert archive["actor"] == "reviewer"
    assert db.participants.find_one()["custom_field"] == "Preserve legacy fields"
    assert db.participants.find_one()["_audit"][-1]["action"] == "merge"
    from domain.models.participant import Participant
    # The detail page serializes the full model to JSON, including its audit.
    profile_json = Participant.from_mongo(db.participants.find_one()).model_dump(mode="json", by_alias=True)
    assert profile_json["_audit"][-1]["merge_id"] == str(merge_id)
    assert service.resolve_alias("P0879") == "P0025"
    assert db.counters.find_one({"_id": "participant_pid"})["seq"] == 879


@pytest.mark.parametrize("collection,query,change", [
    ("participants", {"pid": "P0025"}, {"name": "Changed"}),
    ("participant_events", {"_id": "link-1"}, {"bank_name": "Changed"}),
    ("tests", {"_id": "test-1"}, {"score": 99}),
    ("events", {"eid": "PFE17M3"}, {"participants": ["P0025", "P0879", "P0777"]}),
])
def test_real_transaction_rejects_stale_profile_attendance_score_or_roster(mongo_db, collection, query, change):
    connection, db = mongo_db
    service = merges.ParticipantMergeService(connection)
    state = service.snapshot(PIDS)
    db[collection].update_one(query, {"$set": change})
    before = service.snapshot(PIDS)
    with pytest.raises(merges.MergeError, match="changed since"):
        service.merge(PIDS, "P0025", merges.fingerprint(state), choices_for(service, state), "reviewer")
    assert service.snapshot(PIDS) == before
    assert db.participant_merges.count_documents({}) == 0


def test_real_transaction_rolls_back_all_changes_after_a_late_failure(mongo_db):
    connection, db = mongo_db
    service = merges.ParticipantMergeService(connection)
    state = service.snapshot(PIDS)
    # A unique alias collision occurs after profile/link/roster/test writes and archival.
    db.participant_aliases.insert_one({"_id": "P0879", "target_pid": "P9999"})
    with pytest.raises(DuplicateKeyError):
        service.merge(PIDS, "P0025", merges.fingerprint(state), choices_for(service, state), "reviewer")
    assert service.snapshot(PIDS) == state
    assert db.participant_merges.count_documents({}) == 0
    assert db.counters.count_documents({}) == 0


def test_real_legacy_rosters_and_three_record_merges(mongo_db):
    connection, db = mongo_db
    db.events.insert_one({"_id": "legacy", "eid": "LEGACY", "participant_ids": ["P0879", "P0900", "P0100"]})
    db.participants.insert_one({"_id": "third", "pid": "P0900", "name": "Suela BONJAKU", "representing_country": "C001", "email": "suela@example.test"})
    service = merges.ParticipantMergeService(connection)
    pids = PIDS + ["P0900"]
    state = service.snapshot(pids)
    service.merge(pids, "P0025", merges.fingerprint(state), choices_for(service, state), "reviewer")
    assert db.events.find_one({"eid": "LEGACY"})["participant_ids"] == ["P0025", "P0100"]
    assert db.participant_events.find_one({"event_id": "LEGACY"})["participant_id"] == "P0025"
    assert db.participants.find_one()["email"] == "suela@example.test"
    assert db.participant_events.count_documents({}) == 5


def test_real_suggestions_and_alias_chains(mongo_db):
    connection, db = mongo_db
    db.participants.insert_many([
        {"pid": "P0900", "name": "  suela   Bonjaku ", "representing_country": "C001"},
        {"pid": "P0901", "name": "Suela BONJAKU", "representing_country": "C054"},
    ])
    service = merges.ParticipantMergeService(connection)
    assert {p["pid"] for p in service.candidates()} == set(PIDS + ["P0900"])
    assert [p["pid"] for p in service.candidates("P0879")] == ["P0879"]
    assert len(service.candidates("Suela BONJAKU")) == 4
    db.participant_aliases.insert_many([{"_id": "P0800", "target_pid": "P0879"}, {"_id": "P0879", "target_pid": "P0025"}])
    assert service.resolve_alias("P0800") == "P0025"


@pytest.mark.parametrize("pids", [["P0025"], ["P0025", "P0025"], ["P0025", "invalid"], ["P0025", "P9999"], [f"P{i:04d}" for i in range(11)]])
def test_real_invalid_or_missing_selection_cannot_change_data(mongo_db, pids):
    connection, db = mongo_db
    with pytest.raises(merges.MergeError):
        merges.ParticipantMergeService(connection).snapshot(pids)
    assert db.participants.count_documents({}) == 2
    assert db.participant_merges.count_documents({}) == 0


def test_old_profile_edit_and_event_detail_urls_redirect_after_merge(route_client, monkeypatch):
    client, _, _ = route_client
    monkeypatch.setattr(merges.ParticipantMergeService, "resolve_alias", lambda self, pid: "P0025")
    for path in ("/participant/P0879", "/participant/P0879/edit", "/participant/P0879/events/PFE17M3/details"):
        response = client.get(path + "?search=Suela")
        assert response.status_code == 302
        assert "/participant/P0025" in response.location
        assert "search=Suela" in response.location


def test_real_new_attendance_and_scores_using_a_retired_id_use_surviving_parent(mongo_db, monkeypatch):
    connection, db = mongo_db
    import repositories.participant_event_repository as links
    import repositories.test_repository as tests
    import repositories.event_repository as events
    from domain.models.test import TrainingTest, AttemptType
    from types import SimpleNamespace
    for module in (links, tests, events):
        monkeypatch.setattr(module, "mongodb", connection)
    service = merges.ParticipantMergeService(connection)
    state = service.snapshot(PIDS)
    service.merge(PIDS, "P0025", merges.fingerprint(state), choices_for(service, state), "reviewer")
    links.ParticipantEventRepository().ensure_link("P0879", "NEW-EVENT")
    assert db.participant_events.find_one({"event_id": "NEW-EVENT"})["participant_id"] == "P0025"
    score = TrainingTest(eid="NEW-EVENT", pid="P0879", type=AttemptType.pre, score=75)
    tests.TrainingTestRepository().save(score)
    assert score.pid == "P0025"
    assert db.tests.find_one({"eid": "NEW-EVENT"})["pid"] == "P0025"
    events.EventRepository().save(SimpleNamespace(to_mongo=lambda: {"eid": "NEW-EVENT", "participants": PIDS}))
    assert db.events.find_one({"eid": "NEW-EVENT"})["participants"] == ["P0025"]
    with pytest.raises(ValueError, match="no longer exists"):
        links.ParticipantEventRepository().ensure_link("P9999", "NEW-EVENT")
    assert db.participant_events.count_documents({"participant_id": "P9999"}) == 0


def test_real_repeat_merge_keeps_redirect_chain_and_original_audit_provenance(mongo_db):
    connection, db = mongo_db
    service = merges.ParticipantMergeService(connection)
    state = service.snapshot(PIDS)
    service.merge(PIDS, "P0025", merges.fingerprint(state), choices_for(service, state), "reviewer")
    db.participants.insert_one({"pid": "P0900", "name": "Suela BONJAKU", "representing_country": "C001"})
    state = service.snapshot(["P0025", "P0900"])
    service.merge(["P0025", "P0900"], "P0900", merges.fingerprint(state), choices_for(service, state, "P0900"), "reviewer")
    assert service.resolve_alias("P0879") == "P0900"
    original = next(entry for entry in db.participants.find_one()["_audit"] if entry.get("actor") == "editor")
    assert original["merge_source_pid"] == "P0879"


def test_retired_participant_ids_cannot_be_recreated(mongo_db, monkeypatch):
    import repositories.participant_repository as participants
    from domain.models.participant import Participant
    connection, db = mongo_db
    monkeypatch.setattr(participants, "mongodb", connection)
    service = merges.ParticipantMergeService(connection)
    state = service.snapshot(PIDS)
    old = Participant.from_mongo(next(doc for doc in state["profiles"] if doc["pid"] == "P0879"))
    service.merge(PIDS, "P0025", merges.fingerprint(state), choices_for(service, state), "reviewer")
    with pytest.raises(ValueError, match="retired by a merge"):
        participants.ParticipantRepository().save(old)
    with pytest.raises(ValueError, match="retired by a merge"):
        participants.ParticipantRepository().bulk_save([old])
    assert db.participants.count_documents({}) == 1


@pytest.mark.parametrize("reference_type", ["attendance", "score", "roster"])
def test_real_concurrent_new_references_force_a_new_merge_review(mongo_db, monkeypatch, reference_type):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from types import SimpleNamespace
    import repositories.participant_event_repository as links
    import repositories.test_repository as tests
    import repositories.event_repository as events
    from domain.models.test import TrainingTest, AttemptType
    connection, db = mongo_db
    for module in (links, tests, events):
        monkeypatch.setattr(module, "mongodb", connection)
    service = merges.ParticipantMergeService(connection)
    original_snapshot = service.snapshot
    state = original_snapshot(PIDS)
    ready, resume = Event(), Event()
    first = True
    def paused_snapshot(pids, *, session=None):
        nonlocal first
        result = original_snapshot(pids, session=session)
        if first:
            first = False
            ready.set()
            assert resume.wait(10)
        return result
    monkeypatch.setattr(service, "snapshot", paused_snapshot)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(service.merge, PIDS, "P0025", merges.fingerprint(state), choices_for(service, state), "reviewer")
        try:
            assert ready.wait(10)
            if reference_type == "attendance":
                links.ParticipantEventRepository().ensure_link("P0879", "CONCURRENT")
            elif reference_type == "score":
                tests.TrainingTestRepository().save(TrainingTest(eid="CONCURRENT", pid="P0879", type=AttemptType.pre, score=75))
            else:
                events.EventRepository().save(SimpleNamespace(to_mongo=lambda: {"eid": "CONCURRENT", "participants": ["P0879"]}))
        finally:
            resume.set()
        with pytest.raises(merges.MergeError, match="changed since"):
            future.result(timeout=10)
    assert db.participants.count_documents({}) == 2
    assert db.participant_merges.count_documents({}) == 0
