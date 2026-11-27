"""Authenticated Excel review and atomic participant-linked storage without Atlas access."""

from copy import deepcopy
from io import BytesIO
import re
from types import SimpleNamespace

from flask import Flask
import pytest
from pymongo.errors import OperationFailure
from domain.models.test import TrainingTest

import repositories.prepost_import_repository as repository
import routes.prepost_tests as routes
from tests.test_prepost_import import workbook_bytes, edited
from tests.test_prepost_layouts import forms_bytes, partial_bytes, raw_summary_bytes
from tests.test_prepost_ai import unfamiliar_bytes, plan as ai_plan
from services.prepost_ai_service import AIImportUnavailable
import services.prepost_ai_service as agent


class Collection:
    def __init__(self, database, name):
        self.database, self.name = database, name

    @property
    def docs(self):
        return self.database.docs[self.name]

    def find(self, query, projection=None, session=None):
        def matches(doc):
            return all(doc.get(k) in v["$in"] if isinstance(v, dict) else doc.get(k) == v for k, v in query.items())
        return [deepcopy(d) for d in self.docs if matches(d)]

    def find_one(self, query, session=None):
        docs = self.find(query)
        return docs[0] if docs else None

    def create_index(self, *args, **kwargs):
        assert self.name == "tests" and kwargs["unique"] is True

    def update_one(self, query, update, upsert=False, session=None):
        assert session is not None
        if self.name == "tests":
            self.database.writes += 1
            if self.database.fail and self.database.writes == 2:
                raise OperationFailure("private database connection failure")
        doc = next((d for d in self.docs if all(d.get(k) == v for k, v in query.items())), None)
        matched = doc is not None
        if not matched and upsert:
            doc = dict(query)
            self.docs.append(doc)
        if doc is not None:
            doc.update(deepcopy(update.get("$set", {})))
            for k in update.get("$unset", {}):
                doc.pop(k, None)
            for k, v in update.get("$inc", {}).items():
                doc[k] = doc.get(k, 0) + v
        return SimpleNamespace(matched_count=int(matched))


class Database:
    def __init__(self):
        self.fail, self.writes = False, 0
        self.docs = dict(events=[dict(eid="E1", title="Example", participants=["P1", "P2"]),
                                 dict(eid="E2", title="Other", participants=["P1", "P2"])],
                         participants=[dict(pid="P1", name="ANA example", representing_country="C1"),
                                       dict(pid="P2", name="Bojan SAMPLE", representing_country="C2")],
                         participant_events=[], tests=[])

    def collection(self, name):
        assert name in self.docs  # no new data store and no evaluation summary writes
        return Collection(self, name)

    def start_session(self):
        database = self
        class Session:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def with_transaction(self, callback, **kwargs):
                original = deepcopy(database.docs)
                try:
                    return callback(self)
                except Exception:
                    database.docs = original
                    raise
        return Session()


@pytest.fixture
def db(monkeypatch):
    database = Database()
    monkeypatch.setattr(repository, "mongodb", database)
    return database


@pytest.fixture
def app(db):
    application = Flask(__name__, template_folder="../templates")
    application.config.update(TESTING=True, SECRET_KEY="test-only")
    application.register_blueprint(routes.prepost_tests_bp)
    for endpoint in ("main.show_home", "participants.show_participants", "events.show_events",
                     "imports.upload_form", "auth.login", "auth.logout"):
        application.add_url_rule("/stub/" + endpoint, endpoint=endpoint, view_func=lambda: "stub")
    return application


def hidden(response, field):
    return re.search(fr'name="{field}" value="([^"]+)"', response.get_data(as_text=True))[1]


def login(client):
    with client.session_transaction() as state:
        state["username"] = "reviewer"
    return hidden(client.get("/events/E1/test-scores"), "csrf_token")


def preview(client, csrf, eid="E1", data=None):
    response = client.post(f"/events/{eid}/test-scores", data=dict(
        action="preview", csrf_token=csrf, workbook=(BytesIO(data or workbook_bytes()), "tests.xlsx")))
    assert response.status_code == 200
    return hidden(response, "preview_token"), response


def save(client, csrf, token, eid="E1", **changes):
    return client.post(f"/events/{eid}/test-scores", data=dict(
        action="save", csrf_token=csrf, preview_token=token,
        participant_0="P1", participant_1="P2", **changes))


def test_upload_review_save_and_csv_link_to_event_and_pid(app, db):
    client = app.test_client()
    csrf = login(client)
    token, page = preview(client, csrf)
    assert b'"P1" selected' in page.data and b"66.67%" in page.data
    assert db.docs["tests"] == []
    response = save(client, csrf, token)
    assert response.status_code == 302
    tests = db.docs["tests"]
    assert {(t["eid"], t["pid"], t["type"]) for t in tests} == {
        ("E1", "P1", "pre"), ("E1", "P1", "post"), ("E1", "P2", "pre"), ("E1", "P2", "post")}
    assert all(t["source_file"] == "tests.xlsx" and t["score_unit"] == "percent" for t in tests)
    assert all(TrainingTest.from_mongo(t).score == t["score"] for t in tests)
    assert all(not {"answers", "points", "correct", "question_count", "answered"} & t.keys() for t in tests)
    assert b"66.67%" in client.get(response.location).data
    exported = client.get("/events/E1/test-scores.csv")
    assert exported.status_code == 200 and b"E1,P1,ANA example,C1,66.666" in exported.data
    assert exported.headers["Cache-Control"] == "no-store"
    assert db.docs["events"][0]["participants"] == ["P1", "P2"]
    # A replay has a stale snapshot and cannot double-import.
    assert save(client, csrf, token).status_code == 409 and len(db.docs["tests"]) == 4


def test_authentication_csrf_signature_event_session_and_expiry(app, db, monkeypatch):
    client = app.test_client()
    assert client.get("/events/E1/test-scores").status_code == 302
    assert client.get("/events/E1/test-scores.csv").status_code == 302
    assert save(client, "", "").status_code == 302
    csrf = login(client)
    assert save(client, "é", "").status_code == 400
    assert save(client, csrf, "").status_code == 400
    token, _ = preview(client, csrf)
    assert save(client, csrf, token + "changed").status_code == 400
    assert save(client, csrf, token, eid="E2").status_code == 400
    other = app.test_client()
    assert save(other, login(other), token).status_code == 400
    import itsdangerous.timed
    clock = itsdangerous.timed.time.time
    monkeypatch.setattr(itsdangerous.timed.time, "time", lambda: clock() + 1801)
    assert save(client, csrf, token).status_code == 400
    assert not db.docs["tests"]


def test_unresolved_requires_explicit_skip_manual_match_and_pid_validation(app, db):
    client = app.test_client()
    csrf = login(client)
    token, _ = preview(client, csrf, data=workbook_bytes(names=("A. Example", "Bojan SAMPLE")))
    for pid in ("", "MISSING", "P2"):
        response = client.post("/events/E1/test-scores", data=dict(action="save", csrf_token=csrf,
            preview_token=token, participant_0=pid, participant_1="P2"))
        assert response.status_code in (400, 409) and not db.docs["tests"]
    response = client.post("/events/E1/test-scores", data=dict(action="save", csrf_token=csrf,
        preview_token=token, participant_0="P1", participant_1="__skip__"))
    assert response.status_code == 302
    assert {t["pid"] for t in db.docs["tests"]} == {"P1"}


def test_changed_roster_and_scores_reject_stale_preview(app, db):
    client = app.test_client()
    csrf = login(client)
    token, _ = preview(client, csrf)
    db.docs["participants"][0]["name"] = "Changed NAME"
    assert save(client, csrf, token).status_code == 409
    db.docs["participants"][0]["name"] = "ANA example"
    db.docs["tests"].append(dict(eid="E1", pid="P1", type="pre", score=50))
    assert save(client, csrf, token).status_code == 409
    token, _ = preview(client, csrf)
    assert save(client, csrf, token).status_code == 409
    assert save(client, csrf, token, replace_scores="1").status_code == 302
    assert db.docs["tests"][0]["score"] == pytest.approx(200 / 3)


def test_transaction_failure_rolls_back_all_scores_and_hides_database_details(app, db):
    client = app.test_client()
    csrf = login(client)
    token, _ = preview(client, csrf)
    db.fail = True
    response = save(client, csrf, token)
    assert response.status_code == 503 and b"No scores were saved" in response.data
    assert b"private database" not in response.data and not db.docs["tests"]
    assert "_merge_revision" not in db.docs["participants"][0]


def test_missing_post_attempt_preserves_existing_post_and_other_event(app, db):
    db.docs["tests"] = [dict(eid="E1", pid="P1", type="post", score=90),
                         dict(eid="E2", pid="P1", type="pre", score=10)]
    client = app.test_client()
    csrf = login(client)
    def blank_post(workbook):
        for row in range(9, 12):
            workbook["PostTest"].cell(row, 2).value = None
    token, _ = preview(client, csrf, data=edited(blank_post))
    assert save(client, csrf, token).status_code == 302
    assert db.docs["tests"][0] == dict(eid="E1", pid="P1", type="post", score=90)
    assert db.docs["tests"][1] == dict(eid="E2", pid="P1", type="pre", score=10)


def test_invalid_files_missing_event_and_html_escape(app, db):
    client = app.test_client()
    csrf = login(client)
    assert client.get("/events/MISSING/test-scores").status_code == 404
    invalid = client.post("/events/E1/test-scores", data=dict(action="preview", csrf_token=csrf,
        workbook=(BytesIO(b"bad"), "test.xlsx")))
    assert invalid.status_code == 400
    token, page = preview(client, csrf, data=workbook_bytes(names=("<script>alert(1)</script>", "Bojan SAMPLE")))
    assert b"<script>alert(1)</script>" not in page.data
    assert b"&lt;script&gt;alert(1)&lt;/script&gt;" in page.data
    assert not db.docs["tests"]


def test_forms_import_saves_only_percentages_and_clears_old_question_details(app, db):
    db.docs["tests"] = [dict(eid="E1", pid="P1", type="pre", score=50, answers=["a"], points=[1],
                             correct=1, question_count=1, answered=1)]
    client = app.test_client()
    csrf = login(client)
    token, page = preview(client, csrf, data=forms_bytes())
    assert b"Only percentage scores will be saved" in page.data
    assert b"answered" not in page.data and not re.search(r"\d+/\d+ correct", page.get_data(as_text=True))
    assert save(client, csrf, token, replace_scores="1").status_code == 302
    assert db.docs["tests"][0]["score"] == 66.67
    assert all(not {"answers", "points", "correct", "question_count", "answered"} & t.keys() for t in db.docs["tests"])


def test_scoring_choice_is_reviewed_and_preserved_in_signed_preview(app, db):
    client = app.test_client()
    csrf = login(client)
    response = client.post("/events/E1/test-scores", data=dict(action="preview", csrf_token=csrf,
        scoring="exact", workbook=(BytesIO(partial_bytes()), "tests.xlsx")))
    assert response.status_code == 200
    assert b"bez parcijale" in response.data and b"33.33%" in response.data
    assert save(client, csrf, hidden(response, "preview_token")).status_code == 302
    assert next(t["score"] for t in db.docs["tests"] if t["type"] == "pre") == pytest.approx(100 / 3)


def test_ai_is_explicit_and_native_layouts_do_not_call_the_provider(app, db, monkeypatch):
    client = app.test_client()
    csrf = login(client)
    def unexpected(*args,**kwargs): pytest.fail("AI must not run for a native layout or without opt-in")
    monkeypatch.setattr(routes,"analyze_prepost_with_ai",unexpected)
    preview(client,csrf)
    response=client.post("/events/E1/test-scores",data=dict(action="preview",csrf_token=csrf,
        workbook=(BytesIO(unfamiliar_bytes()),"unfamiliar.xlsx")))
    assert response.status_code==400 and b"Select Use AI interpretation" in response.data
    assert not db.docs["tests"]


def test_ai_preview_then_manual_links_save_verified_results_and_interpretation(app,db,monkeypatch):
    monkeypatch.setattr(agent,"request_layout",lambda snapshot,scoring:ai_plan())
    client=app.test_client()
    csrf=login(client)
    response=client.post("/events/E1/test-scores",data=dict(action="preview",csrf_token=csrf,allow_ai="1",
        workbook=(BytesIO(unfamiliar_bytes()),"unfamiliar.xlsx")))
    assert response.status_code==200 and b"AI-assisted interpretation" in response.data
    assert b"PreTest!B5" in response.data and b"manual participant selection" in response.data
    assert b"No participant-linked pre/post pairs" in response.data
    assert not db.docs["tests"]
    token=hidden(response,"preview_token")
    missing=client.post("/events/E1/test-scores",data=dict(action="save",csrf_token=csrf,preview_token=token,
        participant_0="",participant_1="",participant_2="P1",participant_3="P2"))
    assert missing.status_code==400 and not db.docs["tests"]
    response=client.post("/events/E1/test-scores",data=dict(action="save",csrf_token=csrf,preview_token=token,
        participant_0="P1",participant_1="P2",participant_2="P1",participant_3="P2"))
    assert response.status_code==302 and len(db.docs["tests"])==4
    saved=client.get(response.location)
    assert b"2 participants have linked pre/post results" in saved.data
    assert b"+28.00 percentage points" in saved.data
    assert all(test["cell"] in ("B5","C5") for test in db.docs["tests"])


def test_ai_unavailable_does_not_write_or_expose_provider_details(app,db,monkeypatch):
    def fail(*args,**kwargs): raise AIImportUnavailable("AI interpretation is unavailable.")
    monkeypatch.setattr(routes,"analyze_prepost_with_ai",fail)
    client=app.test_client()
    csrf=login(client)
    response=client.post("/events/E1/test-scores",data=dict(action="preview",csrf_token=csrf,allow_ai="1",
        workbook=(BytesIO(unfamiliar_bytes()),"unfamiliar.xlsx")))
    assert response.status_code==503 and b"AI interpretation is unavailable" in response.data
    assert not db.docs["tests"]


def test_ai_reference_variants_preview_then_save_canonical_cell_sources(app,db,monkeypatch):
    proposal=ai_plan()
    for record in proposal["records"]:
        record["score_cell"]=f"'{record['sheet']}'!${record['score_cell'][0]}$5"
        record["evidence_cell"]=" a$5 "
    monkeypatch.setattr(agent,"request_layout",lambda snapshot,scoring:proposal)
    client=app.test_client()
    csrf=login(client)
    response=client.post("/events/E1/test-scores",data=dict(action="preview",csrf_token=csrf,allow_ai="1",
        workbook=(BytesIO(unfamiliar_bytes()),"unfamiliar.xlsx")))
    assert response.status_code==200 and b"PreTest!B5" in response.data
    assert not db.docs["tests"]
    token=hidden(response,"preview_token")
    response=client.post("/events/E1/test-scores",data=dict(action="save",csrf_token=csrf,preview_token=token,
        participant_0="P1",participant_1="P2",participant_2="P1",participant_3="P2"))
    assert response.status_code==302
    assert all(t["cell"] in {"B5","C5"} for t in db.docs["tests"])


def test_bad_ai_reference_shows_actionable_error_and_writes_nothing(app,db,monkeypatch):
    proposal=ai_plan()
    proposal["records"][0]["evidence_cell"]="A1:A5"
    monkeypatch.setattr(agent,"request_layout",lambda snapshot,scoring:proposal)
    client=app.test_client()
    csrf=login(client)
    response=client.post("/events/E1/test-scores",data=dict(action="preview",csrf_token=csrf,allow_ai="1",
        workbook=(BytesIO(unfamiliar_bytes()),"unfamiliar.xlsx")))
    assert response.status_code==400 and b"evidence_cell=" in response.data and b"A1:A5" in response.data
    assert b"AI result 1" in response.data and b"No results were imported" in response.data
    assert not db.docs["tests"]


def test_repeated_ai_cell_selections_preview_and_save_each_result_once(app,db,monkeypatch):
    proposal=ai_plan()
    proposal["records"] += [dict(item) for item in proposal["records"]]
    monkeypatch.setattr(agent,"request_layout",lambda snapshot,scoring:proposal)
    client=app.test_client()
    csrf=login(client)
    response=client.post("/events/E1/test-scores",data=dict(action="preview",csrf_token=csrf,allow_ai="1",
        workbook=(BytesIO(unfamiliar_bytes()),"unfamiliar.xlsx")))
    assert response.status_code==200
    assert b"Collapsed 4 repeated AI selections" in response.data
    assert response.data.count(b'name="participant_')==4
    assert not db.docs["tests"]
    token=hidden(response,"preview_token")
    response=client.post("/events/E1/test-scores",data=dict(action="save",csrf_token=csrf,preview_token=token,
        participant_0="P1",participant_1="P2",participant_2="P1",participant_3="P2"))
    assert response.status_code==302 and len(db.docs["tests"])==4
    summary=client.get(response.location)
    assert b"+28.00 percentage points" in summary.data


def test_conflicting_duplicate_cell_is_actionable_and_does_not_save(app,db,monkeypatch):
    proposal=ai_plan()
    proposal["records"].append(dict(proposal["records"][0],name_cell="C1"))
    monkeypatch.setattr(agent,"request_layout",lambda snapshot,scoring:proposal)
    client=app.test_client()
    csrf=login(client)
    response=client.post("/events/E1/test-scores",data=dict(action="preview",csrf_token=csrf,allow_ai="1",
        workbook=(BytesIO(unfamiliar_bytes()),"unfamiliar.xlsx")))
    assert response.status_code==400
    assert b"PreTest!B5" in response.data and b"conflicting" in response.data
    assert b"results 1 and 5" in response.data and not db.docs["tests"]


def test_named_score_percentage_matrix_shows_and_links_both_tests_without_ai(app,db,monkeypatch):
    def never_call_ai(*args,**kwargs):pytest.fail("This result table must be read locally")
    monkeypatch.setattr(routes,"analyze_prepost_with_ai",never_call_ai)
    client=app.test_client()
    csrf=login(client)
    response=client.post("/events/E1/test-scores",data=dict(action="preview",csrf_token=csrf,allow_ai="1",
        workbook=(BytesIO(raw_summary_bytes(caches=True)),"result-matrix.xlsx")))
    assert response.status_code==200
    assert b"PreTest!B6" in response.data and b"PostTest!B6" in response.data
    assert b'"P1" selected' in response.data and b'"P2" selected' in response.data
    assert response.data.count(b'name="participant_')==2
    token=hidden(response,"preview_token")
    assert save(client,csrf,token).status_code==302
    assert len(db.docs["tests"])==4
    assert next(t["score"] for t in db.docs["tests"] if t["pid"]=="P1" and t["type"]=="post")==100
