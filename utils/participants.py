"""Participant-related helper functions."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:  # pragma: no cover - circular import avoidance
    from domain.models.participant import Gender


def _normalize_gender(value):
    """Normalize diverse gender labels into the ``Gender`` enum."""
    from domain.models.participant import Gender  # local import avoids circular

    if isinstance(value, Gender):
        return value
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    normalized = text.lower().rstrip(".")
    if normalized in {"m", "male", "man", "mr"}:
        return Gender.male
    if normalized in {"f", "female", "woman", "ms", "mrs"}:
        return Gender.female

    return None
