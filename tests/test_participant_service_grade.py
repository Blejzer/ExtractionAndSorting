from services.participant_service import _format_grade, get_grade_choices
from domain.models.participant import Grade


def test_get_grade_choices_returns_all_grades():
    choices = dict(get_grade_choices())
    assert choices[Grade.BLACK_LIST.value] == "Black List"
    assert choices[Grade.NORMAL.value] == "Normal"
    assert choices[Grade.EXCELLENT.value] == "Excellent"


def test_format_grade_handles_int_and_enum_values():
    assert _format_grade(0) == "Black List"
    assert _format_grade(Grade.NORMAL) == "Normal"
    assert _format_grade(2) == "Excellent"


def test_format_grade_returns_none_for_invalid_input():
    assert _format_grade(99) is None
    assert _format_grade(None) is None
