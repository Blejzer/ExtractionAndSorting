from __future__ import annotations

import json
import os
from typing import Any

MODEL = os.getenv("OPENAI_EXTRACTION_MODEL", "gpt-4.1")
MAX_LENGTH = 12000

_PARTICIPANT_FIELDS = [
    "name",
    "representing_country",
    "gender",
    "dob",
    "pob",
    "birth_country",
    "citizenships",
    "position",
    "organization",
    "unit",
    "rank",
    "intl_authority",
    "phone",
    "email",
    "diet_restrictions",
    "bio_short",
    "transportation",
    "transport_other",
    "traveling_from",
    "returning_to",
    "travel_doc_type",
    "travel_doc_number",
    "travel_doc_issue_date",
    "travel_doc_expiry_date",
    "travel_doc_issued_by",
    "bank_name",
    "iban",
    "iban_type",
    "swift",
]


def _build_prompt(document_text: str) -> str:
    fields = "\n".join(_PARTICIPANT_FIELDS)
    return (
        "You are a document data extraction engine.\n"
        "Extract participants from the provided document and return ONLY valid JSON.\n\n"
        "Return a JSON object with key 'participants' and array values.\n"
        "Each participant must contain these fields:\n"
        f"{fields}\n\n"
        "Rules:\n"
        "- If missing, use empty string for scalar fields and [] for citizenships\n"
        "- Date format must be YYYY-MM-DD\n"
        "- Detect multiple participants and all supported languages\n"
        "- Do not include markdown code fences\n\n"
        f"DOCUMENT:\n{document_text[:MAX_LENGTH]}"
    )


def _resolve_api_key(explicit_key: str | None = None) -> str | None:
    if explicit_key:
        return explicit_key

    return (
        os.getenv("OPENAIAPI")
        or os.getenv("OPENAI_API_KEY")
        or os.getenv("extractionProjectAPI")
        or os.getenv("EXTRACTION_PROJECT_API")
    )


def _get_client(api_key: str | None = None):
    from openai import OpenAI

    return OpenAI(api_key=_resolve_api_key(api_key))


def _extract_json_payload(raw_text: str) -> dict[str, Any] | list[Any]:
    text = (raw_text or "").strip()
    if not text:
        return {"participants": []}

    if text.startswith("```"):
        text = text.strip("`")
        text = text.replace("json\n", "", 1).strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start_obj = text.find("{")
        end_obj = text.rfind("}")
        if start_obj != -1 and end_obj != -1 and end_obj > start_obj:
            return json.loads(text[start_obj : end_obj + 1])

        start_arr = text.find("[")
        end_arr = text.rfind("]")
        if start_arr != -1 and end_arr != -1 and end_arr > start_arr:
            return json.loads(text[start_arr : end_arr + 1])

        raise


def extract_participants(text: str, *, client=None, model: str = MODEL) -> list[dict[str, Any]]:
    """Extract participants from raw text using OpenAI responses API."""

    if not (text or "").strip():
        return []

    resolved_client = client or _get_client()
    response = resolved_client.responses.create(
        model=model,
        input=[{"role": "user", "content": _build_prompt(text)}],
        temperature=0,
    )
    payload = _extract_json_payload(response.output_text)

    participants: list[dict[str, Any]]
    if isinstance(payload, dict):
        raw_participants = payload.get("participants") or []
        participants = [entry for entry in raw_participants if isinstance(entry, dict)]
    elif isinstance(payload, list):
        participants = [entry for entry in payload if isinstance(entry, dict)]
    else:
        participants = []

    normalized: list[dict[str, Any]] = []
    for participant in participants:
        row = {field: (participant.get(field, []) if field == "citizenships" else participant.get(field, "")) for field in _PARTICIPANT_FIELDS}
        if not isinstance(row.get("citizenships"), list):
            row["citizenships"] = [str(row.get("citizenships", ""))] if row.get("citizenships") else []
        normalized.append(row)

    return normalized
