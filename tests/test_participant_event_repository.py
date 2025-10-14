import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import repositories.participant_event_repository as pe_repo_module


def test_participant_event_repository(monkeypatch):
    docs = [
        {"participant_id": "P1", "event_id": "E1"},
        {"participant_id": "P1", "event_id": "E2"},
        {"participant_id": "P2", "event_id": "E1"},
    ]

    class DummyCollection:
        def __init__(self, docs):
            self.docs = docs

        def create_index(self, *args, **kwargs):
            pass

        def update_one(self, _query, update, *_args, **_kwargs):
            self.docs.append(dict(update.get("$set", {})))
            class Res:
                upserted_id = "1"
            return Res()

        def find(self, query, projection=None):
            def matches(doc, clause):
                return all(doc.get(k) == v for k, v in clause.items())

            return (doc for doc in self.docs if matches(doc, query))

    class DummyMongo:
        def collection(self, name):  # noqa: ARG002 - name unused
            return DummyCollection(docs)

    monkeypatch.setattr(pe_repo_module, "mongodb", DummyMongo())

    repo = pe_repo_module.ParticipantEventRepository()

    assert repo.find_events("P1") == ["E1", "E2"]
    assert set(repo.find_participants("E1")) == {"P1", "P2"}

    repo.add("P3", "E3")
    assert {
        "participant_id": "P3",
        "event_id": "E3",
    } in docs
