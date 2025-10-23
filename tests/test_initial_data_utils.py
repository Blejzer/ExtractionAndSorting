import pytest

from domain.models.participant import Gender
from utils.initial_data import _normalize_gender, _split_location


def test_split_location_with_country_code():
    place, country = _split_location("Zagreb C033")
    assert place == "Zagreb"
    assert country == "C033"


def test_split_location_with_comma_separator():
    place, country = _split_location("Opatija, Croatia")
    assert place == "Opatija"
    assert country == "Croatia"


def test_split_location_without_country():
    place, country = _split_location("Online")
    assert place == "Online"
    assert country is None


@pytest.mark.parametrize(
    "value, expected",
    [
        ("Mr", Gender.male),
        ("m", Gender.male),
        ("Male", Gender.male),
        ("Mrs", Gender.female),
        ("Ms", Gender.female),
        ("female", Gender.female),
    ],
)
def test_normalize_gender_title_tokens(value, expected):
    assert _normalize_gender(value) == expected


@pytest.mark.parametrize("value", ["", None, "Unknown", "X"])
def test_normalize_gender_returns_none_for_unhandled_values(value):
    assert _normalize_gender(value) is None
