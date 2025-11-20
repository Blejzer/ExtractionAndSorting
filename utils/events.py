import re
from datetime import UTC, datetime


def _filename_year_from_eid(filename: str) -> int:
    """Infer the event year from an EID prefix (e.g. PFE25 → 2025)."""

    m = re.search(r"(\d{2})", filename)
    return 2000 + int(m.group(1)) if m else datetime.now(UTC).year
