"""Shared reporting vocabulary; no database or application dependencies."""

import re
import unicodedata


COUNTRIES = {
    "AL": "Albania",
    "BA": "BiH",
    "HR": "Croatia",
    "XK": "Kosovo",
    "ME": "Montenegro",
    "MK": "North Macedonia",
    "RS": "Serbia",
}

TRAINING_AREAS = {
    "cybercrime": "Cybercrime",
    "crypto": "Cryptocurrency",
    "dark_web": "Dark web",
    "financial": "Financial crime / money laundering",
    "narcotics": "Narcotics",
    "trafficking": "Human trafficking / migrant smuggling",
    "firearms": "Firearms trafficking",
    "corruption": "Corruption",
    "environmental": "Environmental crime",
    "organized_crime": "Organized crime / cross-cutting investigations",
}


def normalize_text(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").casefold())
    return " ".join("".join(c for c in text if not unicodedata.combining(c)).split())


_COUNTRY_ALIASES = {
    "AL": ("albania", "albanija", "alb", "shqiperia"),
    "BA": ("bih", "bosnia and herzegovina", "bosnia & herzegovina", "bosna i hercegovina", "bosnia", "bos", "bih"),
    "HR": ("croatia", "hrvatska", "hrv", "cro"),
    "XK": ("kosovo", "kosova", "kos", "rks", "xkx"),
    "ME": ("montenegro", "crna gora", "mne"),
    "MK": ("north macedonia", "macedonia", "severna makedonija", "sjeverna makedonija", "mkd", "mkd (north macedonia)"),
    "RS": ("serbia", "srbija", "srb"),
}
_COUNTRY_LOOKUP = {
    normalize_text(alias): code
    for code, aliases in _COUNTRY_ALIASES.items()
    for alias in (code, *aliases)
}


def country_code(value: object, country_names: dict[str, str] | None = None) -> str | None:
    """Resolve a regional country name, ISO code, or stored country CID."""
    key = normalize_text(value)
    name = (country_names or {}).get(key, key)
    return _COUNTRY_LOOKUP.get(normalize_text(name))


_AREA_PATTERNS = {
    "cybercrime": r"\b(?:cyber[ -]?(?:crime|criminal|security)|digital (?:evidence|forensics?)|computer crime|online investigations?)\b",
    "crypto": r"\b(?:crypto(?:currency|currencies|assets?)?|virtual (?:currency|currencies|assets?)|bitcoin|blockchain)\b",
    "dark_web": r"\b(?:dark[ -]?(?:web|net)|hidden services?|onion services?)\b",
    "financial": r"\b(?:financial (?:crime|investigations?)|money[ -]?laundering|asset (?:recovery|tracing)|illicit financ\w*|aml|fraud|follow(?:ing)? the money)\b",
    "narcotics": r"\b(?:narcotics?|drugs?|cocaine|heroin|methamphetamine|synthetic opioids?|fentanyl)\b",
    "trafficking": r"\b(?:human trafficking|trafficking in (?:human beings|persons)|migrant smuggling|smuggling of migrants|thb)\b",
    "firearms": r"\b(?:firearms?|arms trafficking|weapons? trafficking)\b",
    "corruption": r"\b(?:corruption|bribery|anti[ -]?corruption)\b",
    "environmental": r"\b(?:environmental crime|wildlife trafficking|waste trafficking|illegal logging)\b",
    "organized_crime": r"\b(?:organi[sz]ed crime|transnational crime|criminal networks?|joint investigations?|special investigative techniques)\b",
}


def training_areas(event: dict) -> tuple[list[str], str]:
    """Explicit tags override conservative title suggestions, including empty tags."""
    explicit = event.get("training_areas")
    if explicit is not None:
        return [area for area in TRAINING_AREAS if area in explicit], "Reviewed"
    title = normalize_text(event.get("title"))
    matches = [area for area, pattern in _AREA_PATTERNS.items() if re.search(pattern, title)]
    # Generic organized-crime wording is a fallback, not an extra umbrella count.
    if len(matches) > 1 and "organized_crime" in matches:
        matches.remove("organized_crime")
    return matches, "Suggested from title" if matches else "Unclassified"
