from utils.names import (
    _name_key,
    _name_key_from_raw,
    _split_name_variants,
    _to_app_display_name,
    normalize_name,
)


def test_name_key_from_raw_handles_commas_and_spaces():
    expected = _name_key("Smith", "John Paul")
    assert _name_key_from_raw("Smith, John Paul") == expected
    assert _name_key_from_raw("John Paul Smith") == expected


def test_split_name_variants_emits_possible_surnames():
    variants = list(_split_name_variants("John Michael Van Der Meer"))
    assert variants == [
        ("john", "michael van der", "meer"),
        ("john", "michael van", "der meer"),
        ("john", "michael", "van der meer"),
    ]


def test_normalize_name_uppercases_last_but_preserves_all_caps():
    assert normalize_name("john smith") == "john SMITH"
    assert normalize_name("SMITH") == "SMITH"


def test_to_app_display_name_handles_last_first_input():
    assert _to_app_display_name("SMITH, John") == "John SMITH"
    assert _to_app_display_name("Jane Doe") == "Jane DOE"
