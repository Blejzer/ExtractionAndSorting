from utils.names import _name_key


def test_vucetaj_variants_same_key():
    key1 = _name_key("VUÇETAJ", "Gani")
    key2 = _name_key("Vuçetaj", "Gani")
    key3 = _name_key("Vucetaj", "Gani")
    assert key1 == key2 == key3


def test_kujaca_variants_same_key():
    key1 = _name_key("Kujača", "Miro")
    key2 = _name_key("Kujaca", "Miro")
    key3 = _name_key("KUJACA", "MIRO")
    assert key1 == key2 == key3
