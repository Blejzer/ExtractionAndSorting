#!/usr/bin/env python3
"""Normalize participant phone numbers.

This script iterates over all documents in the ``participants`` collection and
reformats any ``phone`` field to the canonical ``+`` followed by 11-12 digits
form. Non-digit characters are removed and numbers outside the expected length
range are left unchanged.
"""

from __future__ import annotations

import re
from typing import Optional

from config.database import mongodb_connection


DIGITS_RE = re.compile(r"\D")


def normalize_phone(value: object) -> Optional[str]:
    """Return phone number as ``+`` followed by digits or ``None`` if invalid."""
    digits = DIGITS_RE.sub("", "" if value is None else str(value))
    if 11 <= len(digits) <= 12:
        return f"+{digits}"
    return None


def main() -> None:
    participants = mongodb_connection.participants
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


if __name__ == "__main__":
    main()
