from services.participant_service import _parse_grade_value, _format_grade
from domain.models.participant import Grade


def test_parse_grade_value_accepts_valid_digits():
    assert _parse_grade_value("0") == Grade.BLACK_LIST.value
    assert _parse_grade_value("1") == Grade.NORMAL.value
    assert _parse_grade_value("2") == Grade.EXCELLENT.value


def test_parse_grade_value_defaults_to_normal_when_blank():
    assert _parse_grade_value(" ") == Grade.NORMAL.value


def test_parse_grade_value_passes_through_none():
    assert _parse_grade_value(None) is None


def test_format_grade_handles_int_and_enum_values():
    assert _format_grade(0) == "Black List"
    assert _format_grade(Grade.NORMAL) == "Normal"
    assert _format_grade(2) == "Excellent"


def test_format_grade_returns_none_for_invalid_input():
    assert _format_grade(99) is None
    assert _format_grade(None) is None
