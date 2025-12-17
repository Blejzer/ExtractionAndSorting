from __future__ import annotations

import re
from datetime import datetime, UTC


def _filename_year_from_eid(filename: str) -> int:
    """Infer 4-digit year from file name pattern like 'PFE25M2' → 2025."""

    m = re.search(r"PFE(\d{2})M", filename.upper())
    return 2000 + int(m.group(1)) if m else datetime.now(UTC).year
