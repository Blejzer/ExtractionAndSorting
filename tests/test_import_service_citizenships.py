import services.import_service as import_service


def test_normalize_citizenships_resolves_localised_names():
    result = import_service._normalize_citizenships(["Makedonija"])

    assert result == ["C181"]


def test_normalize_citizenships_handles_short_aliases():
    result = import_service._normalize_citizenships(["Kos"])

    assert result == ["C117"]


def test_normalize_citizenships_uses_canonical_country_name():
    result = import_service._normalize_citizenships(["North Macedonia"])

    assert result == ["C181"]
