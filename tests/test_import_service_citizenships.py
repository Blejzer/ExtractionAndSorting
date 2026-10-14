from utils.country_resolver import _split_multi_country, normalize_citizenships
import pytest


def test_normalize_citizenships_resolves_localised_names():
    result = normalize_citizenships(["Makedonija"])

    assert result == ["C181"]


def test_normalize_citizenships_handles_short_aliases():
    result = normalize_citizenships(["Kos"])

    assert result == ["C117"]


def test_normalize_citizenships_uses_canonical_country_name():
    result = normalize_citizenships(["North Macedonia"])

    assert result == ["C181"]


@pytest.mark.parametrize("raw", [
    "Kosovo, Europe & Eurasia; Serbia, Europe & Eurasia, World",
    ["Kosovo", "Europe & Eurasia", "Serbia", "Europe & Eurasia", "World"],
])
def test_country_display_regions_do_not_add_citizenships(raw):
    values = _split_multi_country(raw)
    assert values == ["Kosovo", "Serbia"]
    assert normalize_citizenships(values) == ["C117", "C194"]
