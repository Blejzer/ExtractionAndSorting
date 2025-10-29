# utils/country_resolver.py
import re
import unicodedata
from typing import Optional, Dict
from config.database import mongodb


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

    if not raw_value:
        return None

    s = _normalize_ascii(raw_value)
    if s in _SKIP_VALUES:
        return None

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

    # --- 1. Try alias/prefix recognition first ---
    for pattern, canonical in alias_rules:
        if re.search(pattern, s):
            doc = mongodb.collection("countries").find_one({
                "country": {"$regex": rf"^{re.escape(canonical)}", "$options": "i"}
            })
            if doc and doc.get("cid"):
                return {"cid": doc["cid"], "country": doc["country"]}

    # --- 2. Try direct DB lookups (handles exact English names or partial matches) ---
    # Example: "Albania", "Croatia, Europe & Eurasia"
    coll = mongodb.collection("countries")

    # Starts with
    doc = coll.find_one({
        "country": {"$regex": rf"^{re.escape(raw_value)}", "$options": "i"}
    })
    if doc and doc.get("cid"):
        return {"cid": doc["cid"], "country": doc["country"]}

    # Contains
    doc = coll.find_one({
        "country": {"$regex": re.escape(raw_value), "$options": "i"}
    })
    if doc and doc.get("cid"):
        return {"cid": doc["cid"], "country": doc["country"]}

    # --- 3. Try normalized substring search (last fallback) ---
    for doc in coll.find({}):
        name_norm = _normalize_ascii(doc.get("country", ""))
        if name_norm.startswith(s) or s in name_norm:
            return {"cid": doc["cid"], "country": doc["country"]}

    return None


# ==============================================================================
# Find country cid by name
# ==============================================================================
def get_country_cid_by_name(name: str) -> Optional[str]:
    if not name:
        return None
    doc = mongodb.collection("countries").find_one({"country": {"$regex": rf"^{re.escape(name)}", "$options": "i"}})
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
