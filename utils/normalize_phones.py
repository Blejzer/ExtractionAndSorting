"""Phone normalization helpers and maintenance script.

The :func:`normalize_phone` helper converts arbitrary input (``None``, strings,
numbers) into a canonical string that:

* always starts with ``+``
* contains only digits thereafter
* retains the country code (E.164 style, 8–15 digits total)

It uses :mod:`phonenumbers` if available for strict validation and falls back to
a simple digit-only heuristic otherwise.
"""

from __future__ import annotations

import re
from typing import Optional

try:  # pragma: no cover - optional dependency
    import phonenumbers  # type: ignore
except Exception:  # pragma: no cover
    phonenumbers = None


DIGITS_RE = re.compile(r"\D")


def normalize_phone(value: object) -> Optional[str]:
    """Return a phone number formatted as ``+`` followed by digits or ``None``."""

    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    if phonenumbers is not None:  # pragma: no branch - best-effort parsing
        try:
            num = phonenumbers.parse(text, None)
            if phonenumbers.is_possible_number(num):
                return phonenumbers.format_number(
                    num, phonenumbers.PhoneNumberFormat.E164
                )
        except Exception:
            pass

    stripped = text
    if stripped.startswith("00"):
        stripped = stripped[2:]
    if stripped.startswith("+"):
        stripped = stripped[1:]

    digits = DIGITS_RE.sub("", stripped)
    if not digits:
        return None

    if 8 <= len(digits) <= 15:
        return f"+{digits}"
    return None


def main() -> None:
    from config.database import mongodb  # pragma: no cover - script utility

    participants = mongodb.collection("participants")
    for doc in participants.find({"phone": {"$exists": True}}):
        raw = doc.get("phone", "")
        normalized = normalize_phone(raw)
        if not normalized:
            print(f"Skipping {doc.get('pid', doc.get('_id'))}: invalid phone '{raw}'")
            continue
        if normalized != raw:
            participants.update_one({"_id": doc["_id"]}, {"$set": {"phone": normalized}})
            print(
                f"Updated {doc.get('pid', doc.get('_id'))}: '{raw}' -> '{normalized}'"
            )


if __name__ == "__main__":  # pragma: no cover - manual utility
    main()
