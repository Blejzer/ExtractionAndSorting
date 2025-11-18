from utils.normalize_phones import normalize_phone


def test_normalize_phone():
    assert normalize_phone("+1 (202) 555-1234") == "+12025551234"
    assert normalize_phone("00387 65 318 453") == "+38765318453"
    assert normalize_phone("123") is None
    assert normalize_phone(None) is None

