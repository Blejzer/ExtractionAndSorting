import pytest

from utils import country_resolver


@pytest.mark.parametrize(
    "raw,expected_country,method",
    [
        ("BH", "Bosnia and Herzegovina", "alias"),
        ("hrv", "Croatia", "iso"),
        ("Kosovar", "Kosovo", "alias"),
        ("Makedonija", "North Macedonia", "alias"),
        ("Bosnia Herzegovina", "Bosnia and Herzegovina", "exact"),
    ],
)
def test_resolve_country(raw, expected_country, method):
    result = country_resolver.resolve_country(raw)

    assert result is not None
    assert result["country"] == expected_country
    assert result["region"] == country_resolver.COUNTRIES[expected_country]
    assert result["method"] == method


def test_returns_none_for_unknown_country():
    assert country_resolver.resolve_country("Freedonia") is None


def test_normalises_with_stopwords_and_accents():
    result = country_resolver.resolve_country("Republika Srbija")

    assert result is not None
    assert result["country"] == "Serbia"
    assert result["method"] == "alias"


class FakeCollection:
    def __init__(self):
        self.inserted = []

    def insert_one(self, doc):
        self.inserted.append(doc)


def test_ensure_country_reuses_existing_and_inserts_new_entries():
    collection = FakeCollection()
    country_lookup = {"serbia": "c001"}

    assert country_resolver.ensure_country(collection, country_lookup, "Serbia") == "c001"
    assert collection.inserted == []

    cid_new = country_resolver.ensure_country(collection, country_lookup, "Newland")

    assert cid_new == "c002"
    assert country_lookup["newland"] == "c002"
    assert collection.inserted == [{"cid": "c002", "country": "Newland"}]

    assert country_resolver.ensure_country(collection, country_lookup, None) == "c000"
