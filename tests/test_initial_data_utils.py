from utils.initial_data import _split_location


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
