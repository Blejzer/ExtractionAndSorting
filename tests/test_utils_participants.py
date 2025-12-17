import pandas as pd

from domain.models.participant import Gender
from utils.participants import _normalize_gender


def test_normalize_gender_accepts_enum_instance():
    assert _normalize_gender(Gender.male) is Gender.male


def test_normalize_gender_handles_common_strings():
    assert _normalize_gender("M") is Gender.male
    assert _normalize_gender("female") is Gender.female


def test_normalize_gender_handles_titles_and_whitespace():
    assert _normalize_gender(" Mr. ") is Gender.male
    assert _normalize_gender("Mrs") is Gender.female


def test_normalize_gender_returns_none_for_unknown_or_empty():
    assert _normalize_gender("x") is None
    assert _normalize_gender("") is None
    assert _normalize_gender(None) is None
    assert _normalize_gender(float("nan")) is None
    assert _normalize_gender(pd.NA) is None
