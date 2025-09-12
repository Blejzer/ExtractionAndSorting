import services.participant_service as svc


def test_normalize_phone():
    assert svc.normalize_phone("+1 (202) 555-1234") == "+12025551234"
    assert svc.normalize_phone("123") is None
    assert svc.normalize_phone(None) is None

