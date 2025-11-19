import pytest

from utils.excel import _norm_tablename


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Table Alb", "tablealb"),
        ("table-Alb", "tablealb"),
        ("Participants List", "participantslist"),
        ("tableAlb1", "tablealb1"),
        (None, ""),
    ],
)

def test_norm_tablename_normalizes_to_lower_alphanumeric(raw, expected):
    assert _norm_tablename(raw) == expected
