"""One-time script to normalize existing participant phone numbers.

This script uses :func:`services.participant_service.normalize_phone` to ensure
all stored phone numbers follow the canonical ``+`` followed by 11-12 digits
format. Only documents with a ``phone`` field are processed and updated if a
normalized value differs from the original.
"""

from __future__ import annotations

from services.participant_service import normalize_phone
from repositories.participant_repository import ParticipantRepository


def main() -> None:
    repo = ParticipantRepository()
    for doc in repo.collection.find({"phone": {"$exists": True}}):
        raw = doc.get("phone", "")
        normalized = normalize_phone(raw)
        if not normalized:
            print(f"Skipping {doc.get('pid', doc.get('_id'))}: invalid phone '{raw}'")
            continue
        if normalized != raw:
            repo.collection.update_one({"_id": doc["_id"]}, {"$set": {"phone": normalized}})
            print(
                f"Updated {doc.get('pid', doc.get('_id'))}: '{raw}' -> '{normalized}'"
            )


if __name__ == "__main__":  # pragma: no cover - manual execution only
    main()
