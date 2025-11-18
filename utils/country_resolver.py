# utils/country_resolver.py
import re
import unicodedata
from typing import Optional, Dict, List, TypedDict
from config.database import mongodb


class _CountryCacheEntry(TypedDict):
    """Internal representation of a cached country document."""

    cid: str
    country: str
    _lower: str
    _normalized: str


COUNTRY_CACHE: Optional[List[_CountryCacheEntry]] = None
RESOLVE_CACHE: Dict[str, Optional[Dict[str, str]]] = {}


def get_country_cache() -> List[_CountryCacheEntry]:
    """Return the cached list of countries, loading it on first access."""

    global COUNTRY_CACHE
    if COUNTRY_CACHE is not None:
        return COUNTRY_CACHE

    collection = mongodb.collection("countries")
    docs_iter = []
    if hasattr(collection, "find"):
        find_callable = collection.find
        # Try increasingly simpler signatures to support both pymongo and tests.
        for args in (({}, {"cid": 1, "country": 1}), ({},), tuple()):
            try:
                docs_iter = find_callable(*args)
                break
            except TypeError:
                continue

    cache: List[_CountryCacheEntry] = []
    for doc in docs_iter:
        cid = doc.get("cid")
        country_name = doc.get("country")
        if not cid or not country_name:
            continue
        name_str = str(country_name)
        cache.append(
            _CountryCacheEntry(
                cid=cid,
                country=name_str,
                _lower=name_str.lower(),
                _normalized=_normalize_ascii(name_str),
            )
        )

    COUNTRY_CACHE = cache
    return COUNTRY_CACHE


# === Excel Country Table Map (used by import_service_v2) ===
# These link table names like "tableAlb" to the full display label used in the UI.
COUNTRY_TABLE_MAP = {
    "tableAlb": "Albania, Europe & Eurasia",
    "tableBih": "Bosnia and Herzegovina, Europe & Eurasia",
    "tableCro": "Croatia, Europe & Eurasia",
    "tableKos": "Kosovo, Europe & Eurasia",
    "tableMne": "Montenegro, Europe & Eurasia",
    "tableNmk": "North Macedonia, Europe & Eurasia",
    "tableSer": "Serbia, Europe & Eurasia",
}

# ==============================================================================
# Normalization Helper
# ==============================================================================

def _normalize_ascii(text: str) -> str:
    """Lowercase, remove accents, and trim whitespace."""
    if not text:
        return ""
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.lower().strip()


# ==============================================================================
# Skip / Noise Values
# ==============================================================================

_SKIP_VALUES = {
    "", "n/a", "none", "no", "i don't have", "i do not have", "/", "-", "—", "–", "0"
}


# ==============================================================================
# Flexible Resolver
# ==============================================================================

def resolve_country_flexible(raw_value: str) -> Optional[Dict[str, str]]:
    """
    Resolve a country reference (citizenship, birth_country, representing_country)
    into {'cid': 'Cxxx', 'country': '<value from Mongo>'}.

    Handles:
    - Partial words and prefixes (e.g. 'Kosovar' -> Kosovo)
    - Local names (Hrvatska -> Croatia, Srbija -> Serbia, Makedonija -> North Macedonia)
    - Multi-word variants (R. Serbia, BiH, Sjeverna Makedonija)
    - Reads cid and country directly from MongoDB, never hardcoded
    """

    text = str(raw_value or "")
    if text in RESOLVE_CACHE:
        return RESOLVE_CACHE[text]
    if not text:
        RESOLVE_CACHE[text] = None
        return None

    s = _normalize_ascii(text)
    if s in _SKIP_VALUES:
        RESOLVE_CACHE[text] = None
        return None

    countries = get_country_cache()

    # --- Aliases and Prefix Rules (normalized lowercase forms) ---
    alias_rules = [
        # Albania / Albanian
        (r"^(alb|albanian)\b", "Albania"),

        # Bosnia and Herzegovina / BiH / Bosnian / (Herzegovina)
        (r"^(bih)\b", "Bosnia and Herzegovina"),
        (r"^(bosn|bosna|bosnian)\b", "Bosnia and Herzegovina"),
        (r"herzeg", "Bosnia and Herzegovina"),

        # Croatia / Hrvatska / RH / Croatian
        (r"^(cro|hrv|hrvat|hrvatska|rh)\b", "Croatia"),
        (r"\bcroatian\b", "Croatia"),

        # Kosovo / Kosovar
        (r"^kos", "Kosovo"),
        (r"\bkosovar\b", "Kosovo"),

        # Montenegro / Montenegrin / Crna Gora / Monte…
        (r"^(monte|monten|crna\s*gora)\b", "Montenegro"),
        (r"\bmontenegrin\b", "Montenegro"),

        # North Macedonia / Macedonian / Makedonija / MKD / Sjeverna/Severna
        (r"^(maced|maked|mkd|sjeverna|severna|north\s+maced)\b", "North Macedonia"),
        (r"\bmacedonian\b", "North Macedonia"),

        # Serbia / Srbija / R Serbia / R. Serbia / Republika Srbija / Serbian
        (r"^(serb|srb)\b", "Serbia"),
        (r"^r\s+serbia\b", "Serbia"),
        (r"\brepublika\s*srbija\b", "Serbia"),
        (r"\bserbian\b", "Serbia"),
    ]

    result: Optional[Dict[str, str]] = None

    # --- 1. Try alias/prefix recognition first ---
    for pattern, canonical in alias_rules:
        if re.search(pattern, s):
            doc = _find_country_by_prefix(countries, canonical)
            if doc:
                result = _format_country_result(doc)
                break

    # --- 2. Try direct cache lookups (handles exact English names or partial matches) ---
    if result is None:
        doc = _find_country_by_prefix(countries, text)
        if doc:
            result = _format_country_result(doc)

    if result is None:
        doc = _find_country_by_contains(countries, text)
        if doc:
            result = _format_country_result(doc)

    # --- 3. Try normalized substring search (last fallback) ---
    if result is None and s:
        for doc in countries:
            name_norm = doc.get("_normalized", "")
            if name_norm.startswith(s) or s in name_norm:
                result = _format_country_result(doc)
                break

    RESOLVE_CACHE[text] = result
    return result


# ==============================================================================
# Find country cid by name
# ==============================================================================
def get_country_cid_by_name(name: str) -> Optional[str]:
    if not name:
        return None
    doc = _find_country_by_prefix(get_country_cache(), name)
    return doc["cid"] if doc else None


def _split_multi_country(value) -> list[str]:
    """
    Split values like 'BiH i RH', 'Bosnia and Herzegovina, R. Serbia',
    'Makedonija / Srbija' into individual tokens.
    Accepts str or list; returns a flat list of strings.
    """
    if isinstance(value, list):
        seq = value
    else:
        seq = [value]

    out: list[str] = []
    for item in seq:
        s = str(item or "")
        if not s.strip():
            continue
        # normalize a couple of common patterns before splitting
        s = re.sub(r"\bR\.\s*", "R ", s, flags=re.IGNORECASE)  # 'R. Serbia' → 'R Serbia'
        s = s.replace("&", " and ")
        # split on commas, semicolons, slashes, EN 'and', HR 'i'
        parts = re.split(r"[;,/]|(?:\band\b)|(?:\bi\b)", s, flags=re.IGNORECASE)
        out.extend(p.strip() for p in parts if p and p.strip())
    return out


def _find_country_by_prefix(
    countries: List[_CountryCacheEntry],
    candidate: str,
) -> Optional[_CountryCacheEntry]:
    text = str(candidate or "").lower().strip()
    if not text:
        return None
    for doc in countries:
        if doc.get("_lower", "").startswith(text):
            return doc
    return None


def _find_country_by_contains(
    countries: List[_CountryCacheEntry],
    candidate: str,
) -> Optional[_CountryCacheEntry]:
    text = str(candidate or "").lower().strip()
    if not text:
        return None
    for doc in countries:
        if text in doc.get("_lower", ""):
            return doc
    return None


def _format_country_result(doc: _CountryCacheEntry) -> Dict[str, str]:
    return {"cid": doc["cid"], "country": doc["country"]}
