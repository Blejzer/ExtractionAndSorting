import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import repositories.participant_event_repository as pe_repo_module
from domain.models.event_participant import (
    DocType,
    EventParticipant,
    Transport,
)


def test_participant_event_repository(monkeypatch):
    base_payload = {
        "transportation": Transport.air,
        "traveling_from": "ZAG",
        "returning_to": "ZAG",
        "travel_doc_type": DocType.passport,
    }

    docs = [
        {"participant_id": "P1", "event_id": "E1", **base_payload},
        {"participant_id": "P1", "event_id": "E2", **base_payload},
        {"participant_id": "P2", "event_id": "E1", **base_payload},
    ]

    class DummyCollection:
        def __init__(self, docs):
            self.docs = docs

        def create_index(self, *args, **kwargs):
            pass

        def update_one(self, query, update, *_args, **kwargs):
            doc = next(
                (d for d in self.docs if d["participant_id"] == query["participant_id"] and d["event_id"] == query["event_id"]),
                None,
            )
            payload = dict(update.get("$set", {}))
            insert_payload = dict(update.get("$setOnInsert", {}))
            if doc:
                doc.update(payload)
                class Res:
                    upserted_id = None
                return Res()
            if kwargs.get("upsert"):
                new_doc = {**insert_payload, **payload}
                self.docs.append(new_doc)
                class Res:
                    upserted_id = "1"
                return Res()
            raise AssertionError("upsert expected")

        def find(self, query, projection=None):
            def matches(doc, clause):
                return all(doc.get(k) == v for k, v in clause.items())

            return (doc for doc in self.docs if matches(doc, query))

        def find_one(self, query):
            return next(self.find(query), None)

    class DummyMongo:
        def collection(self, name):  # noqa: ARG002 - name unused
            return DummyCollection(docs)

    monkeypatch.setattr(pe_repo_module, "mongodb", DummyMongo())

    repo = pe_repo_module.ParticipantEventRepository()

    assert repo.find_events("P1") == ["E1", "E2"]
    assert set(repo.find_participants("E1")) == {"P1", "P2"}

    new_entry = EventParticipant(
        participant_id="P3",
        event_id="E3",
        **base_payload,
    )
    repo.upsert(new_entry)

    stored = repo.find("P3", "E3")
    assert stored is not None
    assert stored.participant_id == "P3"
    assert stored.event_id == "E3"

    assert repo.list_for_event("E3")[0].participant_id == "P3"
    assert repo.list_for_participant("P3")[0].event_id == "E3"

    repo.ensure_link("P4", "E4")
    assert any(doc["participant_id"] == "P4" and doc["event_id"] == "E4" for doc in docs)
