import pandas as pd

from utils.initial_data import _split_location, as_dt_utc_midnight


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


def test_as_dt_returns_none_for_missing_values():
    assert as_dt_utc_midnight(None) is None
    assert as_dt_utc_midnight(pd.NA) is None


def test_as_dt_returns_none_for_unparseable_values():
    assert as_dt_utc_midnight("not-a-date") is None


def test_as_dt_returns_aware_datetime():
    result = as_dt_utc_midnight("2024-05-01")
    assert result is not None
    assert result.tzinfo is not None
