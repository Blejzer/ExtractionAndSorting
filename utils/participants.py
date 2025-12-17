"""Participant-related helper functions and caches."""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Optional

import pandas as pd

from domain.models.participant import Participant
from repositories.participant_repository import ParticipantRepository
from domain.models.event_participant import DocType
from utils.country_resolver import get_country_cid_by_name
from utils.helpers import _as_str_or_empty
from utils.dates import normalize_dob
from utils.names import _to_app_display_name

if TYPE_CHECKING:  # pragma: no cover - circular import avoidance
    from domain.models.participant import Gender


class ParticipantLookupCache:
    """Shared cache for participant lookups keyed by country and name/DOB."""

    def __init__(self, repo: ParticipantRepository) -> None:
        self._repo = repo
        self._cache: dict[str, dict[tuple[str, Optional[datetime]], Participant]] = {}

    def clear(self) -> None:
        """Drop all cached participant data so it can be refreshed."""

        self._cache.clear()

    def refresh(self) -> None:
        """Reset cached lookups so they will be reloaded on demand."""

        self.clear()

    def _load_for_country(self, representing_country: str) -> None:
        if representing_country in self._cache:
            return

        try:
            participants = self._repo.find_by_country(representing_country)
        except Exception:
            self._cache[representing_country] = {}
            return

        lookup: dict[tuple[str, Optional[datetime]], Participant] = {}
        for participant in participants:
            key = (
                _to_app_display_name(participant.name or ""),
                normalize_dob(participant.dob),
            )
            lookup[key] = participant

        self._cache[representing_country] = lookup

    def find_by_display_name_country_and_dob(
        self,
        *,
        name_display: str,
        country_name: str,
        dob_source: object | None = None,
        representing_country: Optional[str] = None,
    ) -> Optional[Participant]:
        cid = representing_country or get_country_cid_by_name(country_name)
        if not cid:
            return None

        self._load_for_country(cid)
        normalized_key = (_to_app_display_name(name_display), normalize_dob(dob_source))
        cached = self._cache.get(cid, {}).get(normalized_key)
        if cached:
            return cached

        try:
            participant = self._repo.find_by_display_name_country_and_dob(
                name_display=name_display,
                country_name=country_name,
                dob_source=dob_source,
                representing_country=cid,
            )
        except Exception:
            participant = None

        if participant:
            self._cache.setdefault(cid, {})[normalized_key] = participant

        return participant


_GLOBAL_PARTICIPANT_CACHE: ParticipantLookupCache | None = None
_GLOBAL_PARTICIPANT_REPO: ParticipantRepository | None = None


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


def _coerce_grade_value(value: object) -> int:
    """
    Accept only integers 0, 1, 2 (Normal=1 default).
    Any invalid or out-of-range value → 1.
    """

    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 1
    try:
        iv = int(float(value))
        return iv if iv in (0, 1, 2) else 1
    except Exception:
        s = _as_str_or_empty(value)
        if s.lower() == "normal":
            return 1
        return 1


def _normalize_doc_type_label(value: object) -> str:
    """Return 'Passport' only if value == 'Passport'; everything else 'ID Card'."""

    if value == "Passport":
        return str(DocType.passport.value)
    return str(DocType.id_card.value)
