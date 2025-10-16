"""Country name normalisation and resolution utilities.

This module provides a ``resolve_country`` helper that takes an arbitrary
country string and attempts to convert it into a canonical country name and
its associated region.  The goal is to gracefully handle noisy inputs such as
localised names (e.g. "Hrvatska"), abbreviations ("BH"), or adjectival forms
("Kosovar").

The implementation intentionally keeps the lookup logic self-contained so that
it can be reused from tests or future ETL style scripts without pulling in a
database connection.  The :data:`COUNTRIES` mapping acts as the source of
truth.  ``resolve_country`` progressively tries multiple matching strategies:

* Alias/shortcut dictionary lookups (covers local language variants and common
  abbreviations).
* ISO alpha-2/alpha-3 code lookups.
* Exact matches after normalisation.
* Prefix-based shortcuts for adjectival forms ("Kosovar" -> "Kosovo").
* Finally a fuzzy match fallback using :func:`difflib.get_close_matches`.

Whenever a match is found the helper returns a dictionary describing the
resolved country, the region, the method used and a heuristic score.  If no
match is found ``None`` is returned.
"""

from __future__ import annotations

import re
import unicodedata
from difflib import get_close_matches
from typing import Dict, Optional

# 1) Source of truth (country -> region).  This is intentionally focused on the
# countries that appear in the project data, but it can be safely extended when
# new countries show up.
COUNTRIES: Dict[str, str] = {
    "Afghanistan": "South & Central Asia",
    "Albania": "Europe & Eurasia",
    "Algeria": "Middle East & North Africa",
    "American Samoa": "East Asia & the Pacific",
    "Andorra": "Europe & Eurasia",
    "Angola": "Sub-Saharan Africa",
    "Argentina": "Western Hemisphere",
    "Armenia": "Europe & Eurasia",
    "Australia": "East Asia & the Pacific",
    "Austria": "Europe & Eurasia",
    "Azerbaijan": "Europe & Eurasia",
    "Belarus": "Europe & Eurasia",
    "Belgium": "Europe & Eurasia",
    "Bosnia and Herzegovina": "Europe & Eurasia",
    "Bulgaria": "Europe & Eurasia",
    "Canada": "Western Hemisphere",
    "China": "East Asia & the Pacific",
    "Croatia": "Europe & Eurasia",
    "Cyprus": "Europe & Eurasia",
    "Czech Republic": "Europe & Eurasia",
    "Denmark": "Europe & Eurasia",
    "Estonia": "Europe & Eurasia",
    "Finland": "Europe & Eurasia",
    "France": "Europe & Eurasia",
    "Georgia": "Europe & Eurasia",
    "Germany": "Europe & Eurasia",
    "Greece": "Europe & Eurasia",
    "Hungary": "Europe & Eurasia",
    "Iceland": "Europe & Eurasia",
    "India": "South & Central Asia",
    "Ireland": "Europe & Eurasia",
    "Italy": "Europe & Eurasia",
    "Japan": "East Asia & the Pacific",
    "Kosovo": "Europe & Eurasia",
    "Latvia": "Europe & Eurasia",
    "Lithuania": "Europe & Eurasia",
    "Luxembourg": "Europe & Eurasia",
    "Malta": "Europe & Eurasia",
    "Mexico": "Western Hemisphere",
    "Montenegro": "Europe & Eurasia",
    "Netherlands": "Europe & Eurasia",
    "North Macedonia": "Europe & Eurasia",
    "Norway": "Europe & Eurasia",
    "Poland": "Europe & Eurasia",
    "Portugal": "Europe & Eurasia",
    "Romania": "Europe & Eurasia",
    "Serbia": "Europe & Eurasia",
    "Slovakia": "Europe & Eurasia",
    "Slovenia": "Europe & Eurasia",
    "Spain": "Europe & Eurasia",
    "Sweden": "Europe & Eurasia",
    "Switzerland": "Europe & Eurasia",
    "Turkey": "Europe & Eurasia",
    "Ukraine": "Europe & Eurasia",
    "United Kingdom": "Europe & Eurasia",
    "United States": "Western Hemisphere",
}

# 2) Common aliases (expand as new values appear in the incoming data).
ALIASES: Dict[str, str] = {
    # Bosnia and Herzegovina
    "bih": "Bosnia and Herzegovina",
    "bh": "Bosnia and Herzegovina",
    "bosnia i hercegovina": "Bosnia and Herzegovina",
    "bosna i hercegovina": "Bosnia and Herzegovina",
    "bosna": "Bosnia and Herzegovina",
    "bosnia": "Bosnia and Herzegovina",
    "bosnian": "Bosnia and Herzegovina",
    "b i h": "Bosnia and Herzegovina",
    # Croatia
    "rh": "Croatia",  # Republika Hrvatska
    "hr": "Croatia",
    "republika hrvatska": "Croatia",
    "hrvatska": "Croatia",
    "cro": "Croatia",
    "croatian": "Croatia",
    "h r": "Croatia",
    # Serbia
    "srbija": "Serbia",
    "r srbija": "Serbia",
    "republika srbija": "Serbia",
    "rs": "Serbia",
    "srb": "Serbia",
    "ser": "Serbia",
    "serbia": "Serbia",
    "serbian": "Serbia",
    # Montenegro
    "cg": "Montenegro",
    "mne": "Montenegro",
    "crna gora": "Montenegro",
    "montenegro": "Montenegro",
    "montenegrin": "Montenegro",
    "mon": "Montenegro",
    # North Macedonia
    "mk": "North Macedonia",
    "mkd": "North Macedonia",
    "nm": "North Macedonia",
    "n mk": "North Macedonia",
    "north macedonia": "North Macedonia",
    "macedonia": "North Macedonia",
    "mac": "North Macedonia",
    "mak": "North Macedonia",
    "makedonija": "North Macedonia",
    "macedonian": "North Macedonia",
    # Slovenia
    "slo": "Slovenia",
    "svn": "Slovenia",
    "slovenija": "Slovenia",
    "slovene": "Slovenia",
    "slovenian": "Slovenia",
    # Albania
    "alb": "Albania",
    "shqiperi": "Albania",
    "shqiperia": "Albania",
    "shqipëria": "Albania",
    "albania": "Albania",
    "albanian": "Albania",
    # Kosovo (names often vary in datasets)
    "kos": "Kosovo",
    "kosovo": "Kosovo",
    "kosovar": "Kosovo",
    "rks": "Kosovo",
    "kosovo*": "Kosovo",
    # Austria
    "aut": "Austria",
    "oe": "Austria",
    "austria": "Austria",
    "austrian": "Austria",
    # Italy
    "ita": "Italy",
    "it": "Italy",
    "italy": "Italy",
    "italian": "Italy",
    # Germany
    "ger": "Germany",
    "de": "Germany",
    "deu": "Germany",
    "germany": "Germany",
    "german": "Germany",
    # United States
    "usa": "United States",
    "u s a": "United States",
    "us": "United States",
    "united states": "United States",
    "american": "United States",
    # United Kingdom
    "uk": "United Kingdom",
    "gb": "United Kingdom",
    "gbr": "United Kingdom",
    "great britain": "United Kingdom",
    "britain": "United Kingdom",
    "british": "United Kingdom",
}

# 3) ISO code map (alpha-2 and alpha-3).
ISO_ALPHA2_3: Dict[str, str] = {
    "ba": "Bosnia and Herzegovina",
    "bih": "Bosnia and Herzegovina",
    "hr": "Croatia",
    "hrv": "Croatia",
    "rs": "Serbia",
    "srb": "Serbia",
    "me": "Montenegro",
    "mne": "Montenegro",
    "mk": "North Macedonia",
    "mkd": "North Macedonia",
    "si": "Slovenia",
    "svn": "Slovenia",
    "al": "Albania",
    "alb": "Albania",
    "xk": "Kosovo",
    "aut": "Austria",
    "at": "Austria",
    "de": "Germany",
    "deu": "Germany",
    "it": "Italy",
    "ita": "Italy",
    "us": "United States",
    "usa": "United States",
    "gb": "United Kingdom",
    "gbr": "United Kingdom",
}

# Additional prefix based shortcuts for adjectival or longer forms that are not
# easily handled through the alias table.  Keys are the first three characters
# of a token once normalised.
PREFIX_SHORTCUTS: Dict[str, str] = {
    "alb": "Albania",
    "aus": "Austria",
    "bos": "Bosnia and Herzegovina",
    "cro": "Croatia",
    "ger": "Germany",
    "hun": "Hungary",
    "ita": "Italy",
    "kos": "Kosovo",
    "mac": "North Macedonia",
    "mak": "North Macedonia",
    "mon": "Montenegro",
    "pol": "Poland",
    "ser": "Serbia",
    "slo": "Slovenia",
    "spa": "Spain",
    "swe": "Sweden",
    "ukr": "Ukraine",
    "uni": "United Kingdom",
    "usa": "United States",
}

_STOPWORDS = {"republika", "republik", "republic", "and", "i", "of", "the", "r"}


def _strip_accents(value: str) -> str:
    """Remove diacritics from the input string."""

    return "".join(
        char for char in unicodedata.normalize("NFD", value)
        if unicodedata.category(char) != "Mn"
    )


def _normalize(value: str) -> str:
    """Normalise the input value for matching.

    The function lowercases the string, removes accents, strips punctuation,
    collapses whitespace, and finally removes stopwords such as "republika".
    """

    value = value.strip().lower()
    value = _strip_accents(value)
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    tokens = [token for token in value.split() if token not in _STOPWORDS]
    return " ".join(tokens)


_NAMES_NORMALISED: Dict[str, str] = { _normalize(name): name for name in COUNTRIES }


def resolve_country(raw: str) -> Optional[Dict[str, object]]:
    """Resolve the provided raw value to a canonical country entry.

    Args:
        raw: Arbitrary user provided string representing a country or
            citizenship.

    Returns:
        ``None`` when no mapping could be determined or a dictionary with the
        resolved country information when a match is found.
    """

    if not raw or not raw.strip():
        return None

    q_norm = _normalize(raw)

    # 1) Direct alias hit
    alias_match = ALIASES.get(q_norm)
    if alias_match:
        return {
            "country": alias_match,
            "region": COUNTRIES.get(alias_match),
            "method": "alias",
            "score": 1.0,
        }

    # 2) ISO codes.  These are checked after removing any whitespace so that
    # values such as "B. I. H." resolve correctly.
    iso_key = q_norm.replace(" ", "")
    iso_match = ISO_ALPHA2_3.get(iso_key)
    if iso_match:
        return {
            "country": iso_match,
            "region": COUNTRIES.get(iso_match),
            "method": "iso",
            "score": 0.98,
        }

    # 3) Exact country name after normalisation
    exact_match = _NAMES_NORMALISED.get(q_norm)
    if exact_match:
        return {
            "country": exact_match,
            "region": COUNTRIES.get(exact_match),
            "method": "exact",
            "score": 0.97,
        }

    # 4) Prefix shortcuts (useful for adjectival forms such as "Kosovar")
    tokens = q_norm.split()
    if tokens:
        prefix = tokens[0][:3]
        prefix_match = PREFIX_SHORTCUTS.get(prefix)
        if prefix_match:
            return {
                "country": prefix_match,
                "region": COUNTRIES.get(prefix_match),
                "method": "prefix",
                "score": 0.9,
            }

    # 5) Fuzzy match fallback
    candidates = list(COUNTRIES.keys())
    best = get_close_matches(raw, candidates, n=1, cutoff=0.75)
    if not best:
        best_norm = get_close_matches(q_norm, list(_NAMES_NORMALISED.keys()), n=1, cutoff=0.75)
        if not best_norm:
            return None
        resolved_name = _NAMES_NORMALISED[best_norm[0]]
    else:
        resolved_name = best[0]

    return {
        "country": resolved_name,
        "region": COUNTRIES.get(resolved_name),
        "method": "fuzzy",
        "score": 0.85,
    }


__all__ = [
    "COUNTRIES",
    "ALIASES",
    "ISO_ALPHA2_3",
    "PREFIX_SHORTCUTS",
    "resolve_country",
]

