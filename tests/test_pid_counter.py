import pytest

import repositories.participant_repository as module


@pytest.mark.parametrize("pids,sequence,expected", [
    ([], None, "P0001"),
    (["P0001", "P0250"], None, "P0251"),
    (["P9999", "P10000", "invalid"], None, "P10001"),
    (["P0100"], 500, "P0501"),
    (["P0100"], 5, "P0101"),
])
def test_counter_reconciles_existing_participants(monkeypatch, pids, sequence, expected):
    session = object()

    class Participants:
        def find(self, query, projection, **kwargs):
            assert kwargs["session"] is session
            return [{"pid": pid} for pid in pids]

    class Counters:
        def __init__(self):
            self.seq = sequence

        def update_one(self, query, update, **kwargs):
            assert kwargs["session"] is session
            assert kwargs["upsert"]
            self.seq = max(self.seq or 0, update["$max"]["seq"])

        def find_one_and_update(self, query, update, **kwargs):
            assert kwargs["session"] is session
            self.seq += update["$inc"]["seq"]
            return {"seq": self.seq}

    counters = Counters()
    participants = Participants()

    class Mongo:
        def collection(self, name):
            return counters if name == "counters" else participants

    monkeypatch.setattr(module, "mongodb", Mongo())
    repo = module.ParticipantRepository()
    assert repo.generate_next_pid(session=session) == expected
    assert repo.generate_next_pid(session=session) == f"P{int(expected[1:]) + 1:04d}"
