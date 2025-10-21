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
from typing import Callable, Dict, Iterable, Optional

from utils.translation import translate

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

COUNTRY_NAME_TO_CID: Dict[str, str] = {
    "Albania": "C003",
    "Bosnia and Herzegovina": "C027",
    "Croatia": "C033",
    "Kosovo": "C117",
    "Montenegro": "C146",
    "North Macedonia": "C181",
    "Serbia": "C194",
}

COUNTRY_TABLE_MAP: Dict[str, str] = {
    "tableAlb": "Albania, Europe & Eurasia",
    "tableBih": "Bosnia and Herzegovina, Europe & Eurasia",
    "tableCro": "Croatia, Europe & Eurasia",
    "tableKos": "Kosovo, Europe & Eurasia",
    "tableMne": "Montenegro, Europe & Eurasia",
    "tableNmk": "North Macedonia, Europe & Eurasia",
    "tableSer": "Serbia, Europe & Eurasia",
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


_INVALID_CITIZENSHIP_TOKENS = {
    "",
    "no",
    "none",
    "i dont have",
    "i don't have",
    "dont have",
    "n/a",
    "na",
    "none declared",
}


def _citizenship_key(value: str) -> str:
    """Return a normalised key for matching citizenship labels."""

    return re.sub(r"[^a-z]", "", value.lower())


CITIZENSHIP_SYNONYMS: Dict[str, list[str]] = {
    _citizenship_key("Bosnia and Herzegovina"): ["C027"],
    _citizenship_key("Bosnia Herzegovina"): ["C027"],
    _citizenship_key("Bosna i Hercegovina"): ["C027"],
    _citizenship_key("Bosnia i Hercegovina"): ["C027"],
    _citizenship_key("Bosnian"): ["C027"],
    _citizenship_key("Republic of Serbia"): ["C194"],
    _citizenship_key("R Serbia"): ["C194"],
    _citizenship_key("Serbia"): ["C194"],
    _citizenship_key("Serbian"): ["C194"],
    _citizenship_key("Kosovo"): ["C117"],
    _citizenship_key("Kosovar"): ["C117"],
    _citizenship_key("Republic of Kosovo"): ["C117"],
    _citizenship_key("Montenegro"): ["C146"],
    _citizenship_key("Montnegro"): ["C146"],
    _citizenship_key("Montenegrin"): ["C146"],
    _citizenship_key("North Macedonia"): ["C181"],
    _citizenship_key("Macedonia"): ["C181"],
    _citizenship_key("Makedonija"): ["C181"],
    _citizenship_key("Macedonian"): ["C181"],
    _citizenship_key("Albania"): ["C003"],
    _citizenship_key("Shqiperi"): ["C003"],
    _citizenship_key("Shqipëria"): ["C003"],
    _citizenship_key("Albanian"): ["C003"],
    _citizenship_key("Croatia"): ["C033"],
    _citizenship_key("Republika Hrvatska"): ["C033"],
    _citizenship_key("Croatian"): ["C033"],
}


def normalize_citizenships(
    values: Iterable[str],
    lookup: Optional[Callable[[str], Optional[str]]] = None,
) -> list[str]:
    """Normalise citizenship labels into CID codes."""

    tokens: list[str] = []
    for value in values:
        for part in re.split(r"[;,]", value or ""):
            token = _normalize_whitespace(part)
            if token:
                tokens.append(token)

    resolved: list[str] = []
    for token in tokens:
        if not re.search(r"[a-zA-Z]", token):
            continue

        lowered = token.lower()
        if lowered in _INVALID_CITIZENSHIP_TOKENS:
            continue

        key = _citizenship_key(lowered)
        cids: Optional[list[str]] = CITIZENSHIP_SYNONYMS.get(key)
        translated: Optional[str] = None

        if not cids:
            match = resolve_country(token) or resolve_country(lowered)
            if not match:
                translated = translate(token, "en") if token else ""
                translated_norm = _normalize_whitespace(translated)
                if translated_norm and translated_norm.lower() != lowered:
                    match = resolve_country(translated_norm) or resolve_country(translated)

            if match:
                canonical = match["country"]
                cid = _lookup_cid(canonical, lookup)
                if cid:
                    cids = [cid]
                else:
                    canonical_key = _citizenship_key(canonical)
                    cids = CITIZENSHIP_SYNONYMS.get(canonical_key)

        if not cids:
            if translated is None:
                translated = translate(token, "en") if token else ""
            lookup_value = translated or token
            cid = _lookup_cid(lookup_value, lookup)
            if cid:
                cids = [cid]

        if not cids:
            continue

        for cid in cids:
            if cid and cid not in resolved:
                resolved.append(cid)

    return resolved


_CID_PATTERN = re.compile(r"C\d{3}")


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip())


def _lookup_cid(candidate: str, lookup: Optional[Callable[[str], Optional[str]]] = None) -> Optional[str]:
    if not candidate:
        return None

    normalised = _normalize_whitespace(candidate)
    if not normalised:
        return None

    for key in (normalised, normalised.title()):
        cid = COUNTRY_NAME_TO_CID.get(key)
        if cid:
            return cid
        if lookup:
            lookup_value = lookup(key)
            if lookup_value:
                return lookup_value

    return None


def resolve_birth_country_cid(
    raw: str,
    representing_cid: str,
    representing_label: str,
    lookup: Optional[Callable[[str], Optional[str]]] = None,
) -> str:
    """Resolve ``raw`` to a CID, falling back to representing country data."""

    value = _normalize_whitespace(raw or "")
    if not value:
        return representing_cid or ""

    candidates: list[str] = []

    resolved = resolve_country(value)
    if resolved:
        canonical = resolved.get("country") or ""
        if canonical:
            candidates.append(canonical)

    candidates.append(value)

    if "," in value:
        primary = _normalize_whitespace(value.split(",", 1)[0])
        if primary and primary not in candidates:
            primary_resolved = resolve_country(primary)
            if primary_resolved:
                canonical_primary = primary_resolved.get("country") or ""
                if canonical_primary and canonical_primary not in candidates:
                    candidates.insert(0, canonical_primary)
            candidates.append(primary)

    for candidate in candidates:
        cid = _lookup_cid(candidate, lookup)
        if cid:
            return cid

    if representing_cid and _CID_PATTERN.fullmatch(representing_cid.strip()):
        return representing_cid.strip()

    for fallback in (representing_cid, representing_label):
        fallback_norm = _normalize_whitespace(fallback or "")
        if not fallback_norm:
            continue
        if _CID_PATTERN.fullmatch(fallback_norm):
            return fallback_norm
        cid = _lookup_cid(fallback_norm, lookup)
        if cid:
            return cid
        if "," in fallback_norm:
            primary = _normalize_whitespace(fallback_norm.split(",", 1)[0])
            if primary:
                primary_resolved = resolve_country(primary)
                if primary_resolved:
                    canonical_primary = primary_resolved.get("country") or ""
                    cid = _lookup_cid(canonical_primary, lookup)
                    if cid:
                        return cid
                cid = _lookup_cid(primary, lookup)
                if cid:
                    return cid

    return representing_cid or representing_label or value


__all__ = [
    "COUNTRIES",
    "ALIASES",
    "ISO_ALPHA2_3",
    "PREFIX_SHORTCUTS",
    "COUNTRY_TABLE_MAP",
    "COUNTRY_NAME_TO_CID",
    "CITIZENSHIP_SYNONYMS",
    "normalize_citizenships",
    "resolve_country",
    "resolve_birth_country_cid",
]

