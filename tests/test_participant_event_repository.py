import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import repositories.participant_event_repository as pe_repo_module


def test_participant_event_repository(monkeypatch):
    docs = [
        {"pid": "P1", "eid": "E1"},
        {"pid": "P1", "eid": "E2"},
        {"pid": "P2", "eid": "E1"},
    ]

    class DummyCollection:
        def __init__(self, docs):
            self.docs = docs

        def create_index(self, *args, **kwargs):
            pass

        def update_one(self, doc, *_args, **_kwargs):
            self.docs.append(doc)
            class Res:
                upserted_id = "1"
            return Res()

        def find(self, query, projection=None):
            if "pid" in query:
                return (doc for doc in self.docs if doc["pid"] == query["pid"])
            if "eid" in query:
                return (doc for doc in self.docs if doc["eid"] == query["eid"])
            return iter([])

    class DummyMongo:
        def collection(self, name):  # noqa: ARG002 - name unused
            return DummyCollection(docs)

    monkeypatch.setattr(pe_repo_module, "mongodb", DummyMongo())

    repo = pe_repo_module.ParticipantEventRepository()

    assert repo.find_events("P1") == ["E1", "E2"]
    assert set(repo.find_participants("E1")) == {"P1", "P2"}

    repo.add("P3", "E3")
    assert {"pid": "P3", "eid": "E3"} in docs
