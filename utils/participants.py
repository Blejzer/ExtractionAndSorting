# utils/participants.py

"""Participant-related helper functions and caches.

Invariant:
- All name_display inputs MUST already be in 'First Middle LAST' form.
- This module does NOT normalize names, it only compares them.
"""
from __future__ import annotations


from typing import TYPE_CHECKING, Optional

import pandas as pd

from domain.models.participant import Participant
from repositories.participant_repository import ParticipantRepository
from utils.country_resolver import get_country_cid_by_name
from utils.dates import normalize_dob

from config.settings import DEBUG_PRINT

if TYPE_CHECKING:  # pragma: no cover - circular import avoidance
    from domain.models.participant import Gender


class ParticipantLookupCache:
    """Shared cache for participant lookups keyed by country and name."""

    def __init__(self, repo: ParticipantRepository) -> None:
        self._repo = repo
        self._cache: dict[str, dict[str, list[Participant]]] = {}

    def clear(self) -> None:
        """Drop all cached participant data so it can be refreshed."""

        self._cache.clear()

    def refresh(self) -> None:
        """Reset cached lookups so they will be reloaded on demand."""

        self.clear()

    def _load_for_country(self, representing_country: str) -> None:
        if representing_country in self._cache:
            if DEBUG_PRINT:
                print(f"[CACHE] Country {representing_country} already loaded "
                      f"({len(self._cache[representing_country])} names)")
            return

        if DEBUG_PRINT:
            print(f"[CACHE] Loading participants for country={representing_country}")

        try:
            participants = self._repo.find_by_country(representing_country)
        except Exception as exc:
            if DEBUG_PRINT:
                print(f"[CACHE][ERROR] find_by_country failed: {exc}")
            self._cache[representing_country] = {}
            return

        lookup: dict[str, list[Participant]] = {}

        for p in participants:
            name = p.name or ""
            lookup.setdefault(name, []).append(p)

        self._cache[representing_country] = lookup

        if DEBUG_PRINT:
            print(f"[CACHE] Loaded {len(participants)} participants")
            print(f"[CACHE] Unique names: {len(lookup)}")
            print(f"[CACHE] Sample names: {list(lookup.keys())[:5]}")

    def find_by_display_name_country_and_dob(
            self,
            *,
            name_display: str,
            country_name: str,
            dob_source: object | None = None,
            representing_country: Optional[str] = None,
    ) -> Optional[Participant]:

        cid = representing_country or get_country_cid_by_name(country_name)
        if not cid or not name_display:
            if DEBUG_PRINT:
                print("[LOOKUP][SKIP] Missing cid or name_display",
                      cid, repr(name_display))
            return None

        self._load_for_country(cid)

        desired_dob = normalize_dob(dob_source)
        country_cache = self._cache.get(cid, {})

        if DEBUG_PRINT:
            print("[LOOKUP] country:", cid)
            print("[LOOKUP] name_display:", repr(name_display))
            print("[LOOKUP] desired_dob:", desired_dob)
            print("[LOOKUP] cache_size:", len(country_cache))
            print("[LOOKUP] exact_name_match:",
                  name_display in country_cache)

            if name_display not in country_cache:
                print("[LOOKUP] sample cache names:",
                      list(country_cache.keys())[:5])

        candidates = country_cache.get(name_display, [])

        if DEBUG_PRINT:
            print(f"[LOOKUP] candidates found: {len(candidates)}")

        for p in candidates:
            stored_dob = normalize_dob(p.dob)

            if DEBUG_PRINT:
                print("[LOOKUP] checking pid:", p.pid,
                      "stored_dob:", stored_dob)

            if stored_dob:
                if desired_dob and stored_dob == desired_dob:
                    if DEBUG_PRINT:
                        print(f"[MATCH] name+country+dob → pid={p.pid}")
                    return p
                continue

            if DEBUG_PRINT:
                print(f"[MATCH] name+country (DOB missing in DB) → pid={p.pid}")
            return p

        if DEBUG_PRINT:
            print("[LOOKUP] NO MATCH")

        return None


def initialize_cache(repo: ParticipantRepository | None) -> ParticipantLookupCache | None:
    """Create or reset the shared participant cache using ``repo``."""

    global _GLOBAL_PARTICIPANT_CACHE, _GLOBAL_PARTICIPANT_REPO

    _GLOBAL_PARTICIPANT_REPO = repo
    if repo is None:
        _GLOBAL_PARTICIPANT_CACHE = None
        return None

    if (
        _GLOBAL_PARTICIPANT_CACHE is None
        or _GLOBAL_PARTICIPANT_CACHE._repo is not repo  # noqa: SLF001 - internal rebind
    ):
        _GLOBAL_PARTICIPANT_CACHE = ParticipantLookupCache(repo)
    else:
        _GLOBAL_PARTICIPANT_CACHE.refresh()

    return _GLOBAL_PARTICIPANT_CACHE


def lookup(
    *,
    name_display: str,
    country_name: str,
    dob_source: object | None = None,
    representing_country: Optional[str] = None,
) -> Optional[Participant]:
    """Lookup a participant from the shared cache if available."""

    if _GLOBAL_PARTICIPANT_CACHE is None:
        if _GLOBAL_PARTICIPANT_REPO is None:
            return None
        initialize_cache(_GLOBAL_PARTICIPANT_REPO)
        if _GLOBAL_PARTICIPANT_CACHE is None:
            return None

    return _GLOBAL_PARTICIPANT_CACHE.find_by_display_name_country_and_dob(
        name_display=name_display,
        country_name=country_name,
        dob_source=dob_source,
        representing_country=representing_country,
    )


def refresh() -> None:
    """Clear cached participant lookups to reflect latest DB state."""

    if _GLOBAL_PARTICIPANT_CACHE:
        _GLOBAL_PARTICIPANT_CACHE.refresh()


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
