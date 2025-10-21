from utils.country_resolver import normalize_citizenships


def test_normalize_citizenships_resolves_localised_names():
    result = normalize_citizenships(["Makedonija"])

    assert result == ["C181"]


def test_normalize_citizenships_handles_short_aliases():
    result = normalize_citizenships(["Kos"])

    assert result == ["C117"]


def test_normalize_citizenships_uses_canonical_country_name():
    result = normalize_citizenships(["North Macedonia"])

    assert result == ["C181"]
