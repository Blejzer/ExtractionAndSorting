"""Authenticated review and optimistic storage over an in-memory database substitute."""

from copy import deepcopy
from types import SimpleNamespace
import re

from flask import Flask
import pytest
from pymongo.errors import DuplicateKeyError, ServerSelectionTimeoutError

import repositories.event_evaluation_repository as repository_module
import routes.event_evaluations as routes
from repositories.event_evaluation_repository import EventEvaluationRepository, EvaluationConflict
from services.event_evaluation_service import parse_evaluation
from tests.test_event_evaluation_service import REPORT


class Collection:
    def __init__(self):
        self.docs, self.writes = {}, 0
    def find_one(self, query):
        doc = self.docs.get(query["_id"])
        return deepcopy(doc) if doc else None
    def insert_one(self, doc):
        if doc["_id"] in self.docs:
            raise DuplicateKeyError("duplicate event")
        self.docs[doc["_id"]] = deepcopy(doc)
        self.writes += 1
    def update_one(self, query, update):
        doc = self.docs.get(query["_id"])
        if not doc or doc["version"] != query["version"]:
            return SimpleNamespace(matched_count=0)
        doc.update(deepcopy(update["$set"]))
        doc["version"] += update["$inc"]["version"]
        self.writes += 1
        return SimpleNamespace(matched_count=1)


@pytest.fixture
def storage(monkeypatch):
    collection = Collection()
    class Database:
        def collection(self, name):
            assert name == "event_evaluations"
            return collection
    monkeypatch.setattr(repository_module, "mongodb", Database())
    return collection


def test_repository_preserves_source_and_rejects_stale_replacements(storage):
    repo, report = EventEvaluationRepository(), parse_evaluation(REPORT)
    assert repo.save("E1", REPORT, report, 0, "reviewer") == 1
    first = repo.get("E1")
    assert first["raw_text"] == REPORT and first["report"] == report
    assert repo.save("E1", REPORT + "\n", report, 1, "editor") == 2
    latest = repo.get("E1")
    assert latest["created_at"] == first["created_at"] and latest["created_by"] == "reviewer"
    assert latest["updated_by"] == "editor"
    for version in (0, 1):
        with pytest.raises(EvaluationConflict):
            repo.save("E1", "Stale", report, version, "reviewer")
    assert repo.get("E1") == latest and repo.get("E2") is None


@pytest.fixture
def app(storage, monkeypatch):
    application = Flask(__name__, template_folder="../templates")
    application.config.update(TESTING=True, SECRET_KEY="evaluation-test")
    application.register_blueprint(routes.event_evaluations_bp)
    for endpoint in ("main.show_home", "participants.show_participants", "events.show_events",
                     "imports.upload_form", "auth.login", "auth.logout"):
        application.add_url_rule("/stub/" + endpoint, endpoint=endpoint, view_func=lambda: "stub")
    monkeypatch.setattr(routes, "get_event",
                        lambda eid: SimpleNamespace(eid=eid, title="Example training") if eid in ("E1", "E2") else None)
    return application


def login(client):
    with client.session_transaction() as state:
        state["username"] = "reviewer"
    page = client.get("/events/E1/evaluations")
    return re.search(r'name="csrf_token" value="([^"]+)"', page.get_data(as_text=True))[1]


def preview(client, csrf, text=REPORT, eid="E1", version=0):
    response = client.post(f"/events/{eid}/evaluations", data=dict(
        csrf_token=csrf, raw_text=text, expected_version=str(version), action="preview"))
    assert response.status_code == 200
    token = re.search(r'name="preview_token" value="([^"]+)"', response.get_data(as_text=True))[1]
    return token, response


def save(client, csrf, token, text=REPORT, eid="E1", version=0):
    return client.post(f"/events/{eid}/evaluations", data=dict(
        csrf_token=csrf, preview_token=token, raw_text=text,
        expected_version=str(version), action="save"))


def test_preview_is_read_only_then_save_and_export_preserve_report(app, storage):
    client = app.test_client()
    csrf = login(client)
    token, page = preview(client, csrf)
    assert not storage.docs
    assert b"98.73%" in page.data and b"23.87 percentage points" in page.data
    response = save(client, csrf, token)
    assert response.status_code == 302
    assert storage.docs["E1"]["raw_text"] == REPORT and storage.docs["E1"]["event_id"] == "E1"
    page = client.get(response.location)
    assert b"Saved evaluation" in page.data and page.headers["Cache-Control"] == "no-store"
    exported = client.get("/events/E1/evaluations.csv")
    assert exported.status_code == 200 and "attachment" in exported.headers["Content-Disposition"]
    assert b"overall_training" in exported.data and b"reported_knowledge_increase" in exported.data
    assert client.get("/events/E1/evaluations/source.txt").data.decode("utf-8") == REPORT
    assert client.get("/events/E2/evaluations.csv").status_code == 404


@pytest.mark.parametrize("path", ["/events/E1/evaluations", "/events/E1/evaluations.csv",
                                  "/events/E1/evaluations/source.txt"])
def test_authentication_required(app, path):
    assert app.test_client().get(path).status_code == 302


def test_csrf_review_and_event_binding(app, storage):
    client = app.test_client()
    assert client.post("/events/E1/evaluations", data={"action": "save"}).status_code == 302
    csrf = login(client)
    assert save(client, "invalid", "").status_code == 400
    assert save(client, "\u00e9", "").status_code == 400
    assert save(client, csrf, "").status_code == 400
    token, _ = preview(client, csrf)
    assert save(client, csrf, token, text=REPORT + "\n").status_code == 400
    assert save(client, csrf, token, eid="E2").status_code == 400
    assert not storage.docs
    assert client.get("/events/MISSING/evaluations").status_code == 404


def test_cross_session_and_expired_reviews(app, storage, monkeypatch):
    first = app.test_client()
    csrf = login(first)
    token, _ = preview(first, csrf)
    other = app.test_client()
    assert save(other, login(other), token).status_code == 400
    import itsdangerous.timed
    clock = itsdangerous.timed.time.time
    monkeypatch.setattr(itsdangerous.timed.time, "time", lambda: clock() + 1801)
    assert save(first, csrf, token).status_code == 400
    assert not storage.docs


def test_invalid_scores_replays_and_concurrent_edits_do_not_overwrite(app, storage):
    client = app.test_client()
    csrf = login(client)
    bad = client.post("/events/E1/evaluations", data=dict(
        csrf_token=csrf, raw_text="SCALE 1-5\nRate the overall training.\n6",
        expected_version="0", action="preview"))
    assert bad.status_code == 400 and not storage.docs
    token, _ = preview(client, csrf)
    assert save(client, csrf, token).status_code == 302
    assert save(client, csrf, token).status_code == 409 and storage.writes == 1
    changed = REPORT.replace("4.94", "4.93")
    token, _ = preview(client, csrf, changed, version=1)
    storage.docs["E1"]["version"] = 2
    assert save(client, csrf, token, changed, version=1).status_code == 409
    assert storage.docs["E1"]["raw_text"] == REPORT


def test_html_escape_and_safe_database_error(app, storage, monkeypatch):
    client = app.test_client()
    csrf = login(client)
    raw = REPORT + "\n</textarea><script>alert(1)</script>"
    token, page = preview(client, csrf, raw)
    assert b"<script>alert(1)</script>" not in page.data
    assert b"&lt;script&gt;alert(1)&lt;/script&gt;" in page.data
    def fail(*args, **kwargs):
        raise ServerSelectionTimeoutError("mongodb://private-user:private-password@host")
    monkeypatch.setattr(EventEvaluationRepository, "save", fail)
    response = save(client, csrf, token, raw)
    assert response.status_code == 503
    assert b"private-password" not in response.data and b"The evaluation was not saved" in response.data
    assert not storage.docs
