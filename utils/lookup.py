import re
from typing import Dict, Optional

import pandas as pd

from utils.helpers import _normalize
from utils.names import _name_key, _name_key_from_raw, _to_app_display_name
from utils.normalize_phones import normalize_phone
from utils.normalization import normalize_gender, normalize_doc_type_label
from utils.translation import translate


def _build_lookup_participantslista(df_positions: pd.DataFrame) -> Dict[str, Dict[str, str]]:
    """Build lookup from the ``ParticipantsLista`` sheet."""

    name_col = next((c for c in df_positions.columns if "name (" in c.lower()), None)
    pos_col = next((c for c in df_positions.columns if "position" in c.lower()), None)
    phone_col = next((c for c in df_positions.columns if "phone" in c.lower()), None)
    email_col = next((c for c in df_positions.columns if "email" in c.lower()), None)

    look: Dict[str, Dict[str, str]] = {}
    if not name_col:
        return look

    for _, row in df_positions.iterrows():
        raw = _normalize(str(row.get(name_col, "")))
        key = _name_key_from_raw(raw)
        if not key:
            continue
        phone_value = normalize_phone(row.get(phone_col, "")) if phone_col else None
        look[key] = {
            "position": _normalize(str(row.get(pos_col, ""))) if pos_col else "",
            "phone": phone_value or "",
            "email": _normalize(str(row.get(email_col, ""))) if email_col else "",
        }
    return look


def _build_lookup_main_online(df_online: pd.DataFrame) -> Dict[str, Dict[str, object]]:
    """Build lookup from the ``MAIN ONLINE → ParticipantsList`` table."""

    cols = {c.lower().strip(): c for c in df_online.columns}

    def col(label: str) -> Optional[str]:
        return cols.get(label.lower())

    look: Dict[str, Dict[str, object]] = {}
    for _, row in df_online.iterrows():
        first = _normalize(str(row.get(col("Name")) or ""))
        middle = _normalize(str(row.get(col("Middle name")) or ""))
        last = _normalize(str(row.get(col("Last name")) or ""))

        if not first and not last:
            continue

        first_middle = " ".join(part for part in [first, middle] if part).strip()
        key = _name_key(last, first_middle)
        keys = [key]
        if middle and first:
            keys.append(_name_key(last, first))  # Fallback

        gender_col = col("Gender")
        gender_raw = (str(row.get(gender_col, "")) if gender_col else "").strip()
        normalized_gender = normalize_gender(gender_raw)
        gender = normalized_gender.value if normalized_gender else gender_raw

        birth_country_raw = re.sub(
            r",\s*world$", "", _normalize(str(row.get(col("Country of Birth"), ""))), flags=re.IGNORECASE
        )

        travel_doc_type_col = col("Traveling document type")
        travel_doc_type_raw = (
            str(row.get(travel_doc_type_col, "")) if travel_doc_type_col else ""
        ).strip()
        travel_doc_type_value = normalize_doc_type_label(travel_doc_type_raw)

        transportation_col = col("Transportation")
        transport_other_col = col("Transportation (Other)")
        iban_type_col = col("IBAN Type")

        transportation_value = str(row.get(transportation_col, "")) if transportation_col else ""
        transport_other_value = str(row.get(transport_other_col, "")) if transport_other_col else ""
        iban_type_value = str(row.get(iban_type_col, "")) if iban_type_col else ""

        phone_col = col("Phone number")
        phone_raw = row.get(phone_col, "") if phone_col else ""
        phone_list_value = normalize_phone(phone_raw) or ""

        entry = {
            "name": _to_app_display_name(" ".join([first, middle, last]).strip()),
            "gender": gender,
            "dob": row.get(col("Date of Birth (DOB)")),
            "pob": _normalize(str(row.get(col("Place Of Birth (POB)"), ""))),
            "birth_country": birth_country_raw,
            "citizenships": [
                _normalize(x)
                for x in re.split(r"[;,]", str(row.get(col("Citizenship(s)"), "")))
                if _normalize(x)
            ],
            "email_list": _normalize(str(row.get(col("Email address"), ""))),
            "phone_list": phone_list_value,
            "travel_doc_type": travel_doc_type_value,
            "travel_doc_number": _normalize(str(row.get(col("Traveling document number"), ""))),
            "travel_doc_issue": row.get(col("Traveling document issuance date")),
            "travel_doc_expiry": row.get(col("Traveling document expiration date")),
            "travel_doc_issued_by": translate(
                _normalize(str(row.get(col("Traveling document issued by"), ""))), "en"
            ),
            "transportation_declared": transportation_value.strip(),
            "transport_other": transport_other_value.strip(),
            "traveling_from_declared": _normalize(str(row.get(col("Traveling from"), ""))),
            "returning_to": _normalize(str(row.get(col("Returning to"), ""))),
            "diet_restrictions": _normalize(str(row.get(col("Diet restrictions"), ""))),
            "organization": translate(_normalize(str(row.get(col("Organization"), ""))), "en"),
            "unit": translate(_normalize(str(row.get(col("Unit"), ""))), "en"),
            "rank": translate(_normalize(str(row.get(col("Rank"), ""))), "en"),
            "intl_authority": _normalize(str(row.get(col("Authority"), ""))),
            "bio_short": translate(_normalize(str(row.get(col("Short professional biography"), ""))), "en"),
            "bank_name": _normalize(str(row.get(col("Bank name"), ""))),
            "iban": _normalize(str(row.get(col("IBAN"), ""))),
            "iban_type": iban_type_value.strip(),
            "swift": _normalize(str(row.get(col("SWIFT"), ""))),
        }

        for nk in keys:
            if not nk:
                continue
            if nk not in look:
                look[nk] = entry

    return look
