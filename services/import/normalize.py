"""Normalization utilities for import services."""

import re
from typing import Optional


def normalize_text(value: Optional[str]) -> str:
    """Normalize whitespace and coerce None to an empty string."""
    return re.sub(r"\s+", " ", (value or "").strip())
