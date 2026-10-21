from __future__ import annotations

import datetime as dt
import re

import repositories.participant_repository as participant_repo_module


class DummyCollection:
    def __init__(self, docs):
        self.docs = list(docs)

    def find(self, query):  # pragma: no cover - simple generator
        def matches(doc):
            for key, value in query.items():
                if isinstance(value, dict) and "$regex" in value:
                    flags = re.I if "i" in value.get("$options", "") else 0
                    if not re.search(value["$regex"], doc.get(key) or "", flags):
                        return False
                elif doc.get(key) != value:
                    return False
            return True

        for doc in self.docs:
            if matches(doc):
                yield doc

    def find_one(self, query):
        return next(self.find(query), None)


class DummyMongo:
    def __init__(self, docs):
        self._collection = DummyCollection(docs)

    def collection(self, name):  # noqa: ARG002 - name unused
        return self._collection


def _build_repo(monkeypatch, docs):
    monkeypatch.setattr(participant_repo_module, "mongodb", DummyMongo(docs))
    return participant_repo_module.ParticipantRepository()


def _participant_doc(pid: str, name: str, *, representing_country: str, dob: dt.datetime | None):
    return {
        "pid": pid,
        "name": name,
        "representing_country": representing_country,
        "gender": "Male",
        "grade": 1,
        "dob": dob,
        "pob": "Zagreb",
        "birth_country": representing_country,
    }


def test_find_by_display_name_country_and_dob_normalizes_inputs(monkeypatch):
    docs = [
        _participant_doc(
            "P123",
            "john DOE",
            representing_country="c001",
            dob=dt.datetime(1990, 1, 1),
        )
    ]
    repo = _build_repo(monkeypatch, docs)

    calls: list[str] = []

    def fake_country_lookup(name: str) -> str:
        calls.append(name)
        return "c001"

    monkeypatch.setattr(participant_repo_module, "get_country_cid_by_name", fake_country_lookup)

    participant = repo.find_by_display_name_country_and_dob(
        name_display="  john   doe  ",
        country_name="Croatia, Europe & Eurasia",
        dob_source="1990-01-01",
    )

    assert participant is not None
    assert participant.pid == "P123"
    assert calls == ["Croatia, Europe & Eurasia"]


def test_find_by_display_name_country_and_dob_prefers_explicit_country(monkeypatch):
    docs = [
        _participant_doc(
            "P999",
            "Jane DOE",
            representing_country="c009",
            dob=dt.datetime(1985, 5, 5),
        )
    ]
    repo = _build_repo(monkeypatch, docs)

    def exploding_lookup(_name: str) -> str:  # pragma: no cover - safety
        raise AssertionError("country lookup should not be called when CID is provided")

    monkeypatch.setattr(participant_repo_module, "get_country_cid_by_name", exploding_lookup)

    participant = repo.find_by_display_name_country_and_dob(
        name_display="Jane Doe",
        country_name="Unknown Country",
        representing_country="c009",
    )

    assert participant is not None
    assert participant.pid == "P999"


def test_find_by_display_name_country_and_dob_handles_missing_dob(monkeypatch):
    docs = [
        _participant_doc(
            "P555",
            "SAM DOE",
            representing_country="c010",
            dob=dt.datetime(2000, 2, 2),
        )
    ]
    repo = _build_repo(monkeypatch, docs)

    monkeypatch.setattr(
        participant_repo_module,
        "get_country_cid_by_name",
        lambda _name: "c010",
    )

    participant = repo.find_by_display_name_country_and_dob(
        name_display="SAM doe",
        country_name="Some Country",
        dob_source=None,
    )

    assert participant is not None
    assert participant.pid == "P555"


def test_lookup_matches_case_and_whitespace_when_stored_birth_details_are_missing(monkeypatch):
    doc = _participant_doc("P1234", "  JANE   SAMPLE  ", representing_country="C054", dob=None)
    doc["pob"] = None
    repo = _build_repo(monkeypatch, [doc])
    person = repo.find_by_name_dob_and_representing_country_cid(
        name="Jane SAMPLE", dob=dt.datetime(1980, 2, 10), representing_country="C054",
    )
    assert person is not None and person.pid == "P1234"
    assert person.dob is None and person.pob is None


def test_lookup_does_not_match_another_country_or_conflicting_known_dob(monkeypatch):
    repo = _build_repo(monkeypatch, [
        _participant_doc("P1234", "JANE SAMPLE", representing_country="C054", dob=dt.datetime(1980, 2, 10)),
    ])
    assert repo.find_by_name_dob_and_representing_country_cid(
        name="Jane SAMPLE", dob=dt.datetime(1981, 2, 10), representing_country="C054",
    ) is None
    assert repo.find_by_name_dob_and_representing_country_cid(
        name="Jane SAMPLE", dob=dt.datetime(1980, 2, 10), representing_country="C027",
    ) is None


def test_lookup_treats_name_punctuation_literally_and_requires_the_full_name(monkeypatch):
    repo = _build_repo(monkeypatch, [
        _participant_doc("P1234", "JANE (A.) SAMPLE", representing_country="C054", dob=None),
    ])
    assert repo.find_by_name_dob_and_representing_country_cid(
        name="Jane (A.) SAMPLE", dob=None, representing_country="C054",
    ).pid == "P1234"
    for name in ("Jane A. SAMPLE", "Jane SAMPLE", "Jane .* SAMPLE", "", "   "):
        assert repo.find_by_name_dob_and_representing_country_cid(
            name=name, dob=None, representing_country="C054",
        ) is None
