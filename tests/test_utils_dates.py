from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from utils.dates import coerce_datetime, date_to_iso, normalize_dob


def test_normalize_dob_from_iso_string():
    assert normalize_dob("1990-01-05") == datetime(1990, 1, 5)


def test_normalize_dob_handles_excel_serial():
    # Excel serial for 2020-01-01 is 43831
    assert normalize_dob(43831) == datetime(2020, 1, 1)


def test_normalize_dob_strips_timezone_and_time():
    tz_dt = datetime(1990, 1, 1, 14, 30, tzinfo=timezone.utc)
    assert normalize_dob(tz_dt) == datetime(1990, 1, 1)


def test_normalize_dob_handles_pandas_timestamp():
    ts = pd.Timestamp("1985-05-05 03:30", tz="Europe/Zagreb")
    assert normalize_dob(ts) == datetime(1985, 5, 5)


def test_normalize_dob_rejects_ghost_date():
    assert normalize_dob("1900-01-01") is None


def test_normalize_dob_invalid_string_returns_none():
    assert normalize_dob("not-a-date") is None


def test_normalize_dob_handles_missing_values():
    assert normalize_dob(None) is None
    assert normalize_dob(pd.NA) is None


def test_normalize_dob_strict_invalid_string_raises():
    with pytest.raises(ValueError):
        normalize_dob("still-not-a-date", strict=True)


def test_coerce_datetime_parses_strings_with_timezone():
    tz = ZoneInfo("Europe/Zagreb")
    dt = coerce_datetime("2024-05-01", tzinfo=tz)
    assert dt and dt.year == 2024 and dt.tzinfo == tz


def test_coerce_datetime_handles_excel_serial_numbers():
    dt = coerce_datetime(43831)
    assert dt == datetime(2020, 1, 1)


def test_date_to_iso_handles_strings_and_excel_serials():
    assert date_to_iso("1999-12-31") == "1999-12-31"
    assert date_to_iso(45000) == "2023-03-15"
