"""Lookup builders for import service."""

from __future__ import annotations

import re
from typing import Dict, Optional

import pandas as pd

from domain.models.event_participant import DocType
from services.import.normalize import normalize_text
from utils.names import _name_key, _name_key_from_raw, _to_app_display_name
from utils.normalize_phones import normalize_phone
from utils.participants import _normalize_gender
from utils.translation import translate

DOC_TYPE_CACHE: dict[str, str] = {}
_DOC_TYPE_SEEN: set[str] = set()


def collect_doc_type(value: object) -> str:
    """Collect raw travel document values without normalizing yet."""
    if not value:
        return ""

    raw = str(value).strip()
    if raw:
        _DOC_TYPE_SEEN.add(raw)
    return raw


def finalize_doc_type_cache() -> None:
    """Normalize all collected document types exactly once."""
    for raw in _DOC_TYPE_SEEN:
        key = re.sub(r"[^a-z0-9]+", " ", raw.lower()).strip()

        # Passport detection
        if "pass" in key:
            normalized = str(DocType.passport.value)
        else:
            normalized = str(DocType.id_card.value)

        DOC_TYPE_CACHE[raw] = normalized


def build_lookup_participantslista(df_positions: pd.DataFrame) -> Dict[str, Dict[str, str]]:
    """
    Build lookup from the 'ParticipantsLista' sheet.

    Key:
        'LAST|First Middle'
    Value:
        {
            "position": ...,
            "phone": ...,
            "email": ...
        }
    """
    name_col = next((c for c in df_positions.columns if "name (" in c.lower()), None)
    pos_col = next((c for c in df_positions.columns if "position" in c.lower()), None)
    phone_col = next((c for c in df_positions.columns if "phone" in c.lower()), None)
    email_col = next((c for c in df_positions.columns if "email" in c.lower()), None)

    look: Dict[str, Dict[str, str]] = {}
    if not name_col:
        return look

    for _, row in df_positions.iterrows():
        raw = normalize_text(str(row.get(name_col, "")))
        key = _name_key_from_raw(raw)
        if not key:
            continue
        phone_value = normalize_phone(row.get(phone_col, "")) if phone_col else None
        look[key] = {
            "position": normalize_text(str(row.get(pos_col, ""))) if pos_col else "",
            "phone": phone_value or "",
            "email": normalize_text(str(row.get(email_col, ""))) if email_col else "",
        }
    return look


def build_lookup_main_online(df_online: pd.DataFrame) -> Dict[str, Dict[str, object]]:
    """
    Build lookup from the 'MAIN ONLINE → ParticipantsList' table.

    Key:
        'LAST|First Middle'  (plus fallback 'LAST|First')
    Value:
        normalized field dictionary with translated and enriched values.
    """
    cols = {c.lower().strip(): c for c in df_online.columns}

    def col(label: str) -> Optional[str]:
        return cols.get(label.lower())

    look: Dict[str, Dict[str, object]] = {}
    for _, row in df_online.iterrows():
        first = normalize_text(str(row.get(col("Name")) or ""))
        middle = normalize_text(str(row.get(col("Middle name")) or ""))
        last = normalize_text(str(row.get(col("Last name")) or ""))

        if not first and not last:
            continue

        first_middle = " ".join(part for part in [first, middle] if part).strip()
        key = _name_key(last, first_middle)
        keys = [key]
        if middle and first:
            keys.append(_name_key(last, first))  # Fallback

        # --- Gender normalization ---
        gender_col = col("Gender")
        gender_raw = (str(row.get(gender_col, "")) if gender_col else "").strip()
        normalized_gender = _normalize_gender(gender_raw)
        gender = normalized_gender.value if normalized_gender else gender_raw

        # --- Birth country translation ---
        birth_country_raw = re.sub(
            r",\s*world$",
            "",
            normalize_text(str(row.get(col("Country of Birth"), ""))),
            flags=re.IGNORECASE,
        )

        # --- Travel document type ---
        travel_doc_type_col = col("Traveling document type")

        travel_doc_type_raw = collect_doc_type(
            row.get(travel_doc_type_col, "") if travel_doc_type_col else ""
        )
        # --- Transport and banking fields ---
        transportation_col = col("Transportation")
        transport_other_col = col("Transportation (Other)")
        iban_type_col = col("IBAN Type")

        transportation_value = str(row.get(transportation_col, "")) if transportation_col else ""
        transport_other_value = str(row.get(transport_other_col, "")) if transport_other_col else ""
        iban_type_value = str(row.get(iban_type_col, "")) if iban_type_col else ""

        # --- Compose normalized entry ---
        phone_col = col("Phone number")
        phone_raw = row.get(phone_col, "") if phone_col else ""
        phone_list_value = normalize_phone(phone_raw) or ""

        entry = {
            "name": _to_app_display_name(" ".join([first, middle, last]).strip()),
            "gender": gender,
            "dob": row.get(col("Date of Birth (DOB)")),
            "pob": normalize_text(str(row.get(col("Place Of Birth (POB)"), ""))),
            "birth_country": birth_country_raw,
            "citizenships": [
                normalize_text(x)
                for x in re.split(r"[;,]", str(row.get(col("Citizenship(s)"), "")))
                if normalize_text(x)
            ],
            "email_list": normalize_text(str(row.get(col("Email address"), ""))),
            "phone_list": phone_list_value,
            "travel_doc_type": travel_doc_type_raw,
            "travel_doc_number": normalize_text(str(row.get(col("Traveling document number"), ""))),
            "travel_doc_issue": row.get(col("Traveling document issuance date")),
            "travel_doc_expiry": row.get(col("Traveling document expiration date")),
            "travel_doc_issued_by": translate(
                normalize_text(str(row.get(col("Traveling document issued by"), ""))), "en"
            ),
            "transportation_declared": transportation_value.strip(),
            "transport_other": transport_other_value.strip(),
            "traveling_from_declared": normalize_text(str(row.get(col("Traveling from"), ""))),
            "returning_to": normalize_text(str(row.get(col("Returning to"), ""))),
            "diet_restrictions": normalize_text(str(row.get(col("Diet restrictions"), ""))),
            "organization": translate(normalize_text(str(row.get(col("Organization"), ""))), "en"),
            "unit": translate(normalize_text(str(row.get(col("Unit"), ""))), "en"),
            "rank": translate(normalize_text(str(row.get(col("Rank"), ""))), "en"),
            "intl_authority": normalize_text(str(row.get(col("Authority"), ""))),
            "bio_short": translate(
                normalize_text(str(row.get(col("Short professional biography"), ""))), "en"
            ),
            "bank_name": normalize_text(str(row.get(col("Bank name"), ""))),
            "iban": normalize_text(str(row.get(col("IBAN"), ""))),
            "iban_type": iban_type_value.strip(),
            "swift": normalize_text(str(row.get(col("SWIFT"), ""))),
        }

        for nk in keys:
            if not nk:
                continue
            if nk not in look:
                look[nk] = entry

    return look
