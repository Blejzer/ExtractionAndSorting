from __future__ import annotations

import os
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any

from repositories.participant_event_repository import ParticipantEventRepository
from repositories.participant_repository import ParticipantRepository
from utils.country_resolver import resolve_country_flexible
from utils.dates import date_to_iso
from utils.docx_parser import extract_docx_text, parse_docx as parse_docx_legacy
from utils.normalize_phones import normalize_phone
from utils.openai_extractor import (
    _resolve_api_key,
    extract_participants as extract_participants_openai,
)
from utils.names import _to_app_display_name


@dataclass
class ComparisonResult:
    extracted: dict[str, Any]
    existing: dict[str, Any] | None
    score: float


class DocxImportService:
    """Extract and persist participants from uploaded DOCX files."""

    def __init__(self) -> None:
        self.participant_repo = ParticipantRepository()
        self.participant_event_repo = ParticipantEventRepository()

    def extract_participants(self, files: list[str], eid: str) -> dict[str, Any]:
        participants: list[dict[str, Any]] = []
        warnings: list[str] = []
        for path in files:
            extracted_rows, engine, warning = self.parse_docx(path)
            if warning:
                warnings.append(f"{os.path.basename(path)}: {warning}")

            for extracted in extracted_rows:
                normalized = self.normalize_fields(extracted)
                participant_json = self.convert_to_participant_json(normalized)
                participant_json["_source_file"] = os.path.basename(path)
                participant_json["_eid"] = eid
                participant_json["_extraction_engine"] = engine
                participants.append(participant_json)

        return {"participants": participants, "eid": eid, "warnings": warnings}

    def parse_docx(self, file_path: str) -> tuple[list[dict[str, Any]], str, str | None]:
        text = extract_docx_text(file_path)

        if not text:
            return parse_docx_legacy(file_path), "legacy", "Document text was empty; used legacy parser."

        api_key = _resolve_api_key()
        if not api_key:
            return parse_docx_legacy(file_path), "legacy", (
                "OpenAI API key is not configured (OPENAI_API_KEY/extractionProjectAPI); "
                "used legacy parser."
            )

        try:
            extracted = extract_participants_openai(text)
        except Exception as exc:
            return parse_docx_legacy(file_path), "legacy", (
                f"OpenAI extraction failed ({exc}); used legacy parser."
            )

        if extracted:
            return extracted, "openai", None

        return parse_docx_legacy(file_path), "legacy", (
            "OpenAI extraction returned no participants; used legacy parser."
        )

    def normalize_fields(self, data: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(data)
        country_value = str(normalized.get("representing_country", ""))
        if country_value:
            resolved = resolve_country_flexible(country_value)
            normalized["representing_country"] = resolved.get("cid", "") if resolved else ""

        birth_country = str(normalized.get("birth_country", ""))
        if birth_country:
            resolved = resolve_country_flexible(birth_country)
            normalized["birth_country"] = resolved.get("cid", "") if resolved else ""

        if not normalized.get("birth_country") and normalized.get("representing_country"):
            normalized["birth_country"] = normalized.get("representing_country", "")

        citizenships = normalized.get("citizenships") or []
        if isinstance(citizenships, str):
            citizenships = [citizenships]
        if not citizenships and normalized.get("representing_country"):
            citizenships = [normalized["representing_country"]]
        normalized["citizenships"] = [
            resolve_country_flexible(value).get("cid")
            for value in citizenships
            if value and resolve_country_flexible(value)
        ] or ([normalized["representing_country"]] if normalized.get("representing_country") else [])

        if normalized.get("name"):
            normalized["name"] = _to_app_display_name(str(normalized["name"]))

        if normalized.get("dob"):
            normalized["dob"] = date_to_iso(normalized.get("dob")) or str(normalized.get("dob", ""))

        phone_value = normalized.get("phone")
        if phone_value:
            normalized["phone"] = normalize_phone(str(phone_value)) or str(phone_value)

        return normalized

    def convert_to_participant_json(self, data: dict[str, Any]) -> dict[str, Any]:
        defaults = {
            "pid": "",
            "name": "",
            "representing_country": "",
            "transportation": "",
            "transport_other": "",
            "traveling_from": "",
            "returning_to": "",
            "grade": 1,
            "position": "",
            "phone": "",
            "email": "",
            "gender": "",
            "dob": "",
            "pob": "",
            "birth_country": "",
            "citizenships": [],
            "travel_doc_type": "",
            "travel_doc_number": "",
            "travel_doc_issue_date": "",
            "travel_doc_expiry_date": "",
            "travel_doc_issued_by": "",
            "diet_restrictions": "",
            "organization": "",
            "unit": "",
            "rank": "",
            "intl_authority": "",
            "bio_short": "",
            "bank_name": "",
            "iban": "",
            "iban_type": "",
            "swift": "",
        }
        payload = dict(defaults)
        payload.update({k: v for k, v in data.items() if k in defaults})
        return payload

    def compare_with_db(self, participant: dict[str, Any]) -> ComparisonResult:
        # strict candidate set by country + dob, then fuzzy on normalized name
        country = participant.get("representing_country")
        desired_dob = participant.get("dob")
        extracted_name = _to_app_display_name(participant.get("name", ""))
        candidates = self.participant_repo.find_by_country(country) if country else []

        best = None
        best_score = 0.0
        for candidate in candidates:
            if desired_dob and candidate.dob:
                candidate_dob = candidate.dob.date().isoformat()
                if candidate_dob != desired_dob:
                    continue
            score = SequenceMatcher(None, extracted_name.lower(), candidate.name.lower()).ratio()
            if score > best_score:
                best_score = score
                best = candidate

        existing_payload = best.to_mongo() if best and best_score >= 0.78 else None
        return ComparisonResult(extracted=participant, existing=existing_payload, score=best_score)

    def save_participant(self, participant: dict[str, Any], eid: str) -> str:
        comparison = self.compare_with_db(participant)
        if comparison.existing:
            pid = comparison.existing["pid"]
            update_payload = {
                key: value
                for key, value in participant.items()
                if key in comparison.existing and value not in (None, "")
            }
            update_payload.pop("pid", None)
            self.participant_repo.update(pid, update_payload)
        else:
            pid = self.participant_repo.generate_next_pid()
            model_payload = dict(participant)
            model_payload.update({"pid": pid, "grade": 1, "gender": participant.get("gender") or "Male"})
            from domain.models.participant import Participant

            participant_model = Participant.model_validate(model_payload, context={"allow_missing_dob": True})
            self.participant_repo.save(participant_model)

        self.participant_event_repo.ensure_link(participant_id=pid, event_id=eid)
        return pid
