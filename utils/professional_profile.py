"""Evidence-based professional role and leadership labels for reporting."""

from dataclasses import dataclass
import re

from domain.reporting import country_code, normalize_text


ROLE_PATTERNS = {
    "Prosecutor": r"\b(?:prosecutor|prossecutor|tuzilac|tuzitelj|tuziteljica|tuziteljka)\b",
    "Judge": r"\b(?:judge|sudija|sudac|sutkinja)\b",
    "Police officer": r"\b(?:police officer|policeman|policewoman|policijski sluzbenik|policijska sluzbenica)\b",
    "Lawyer": r"\b(?:lawyer|attorney|advokat|odvjetnik|odvjetnica)\b",
    "Customs officer": r"\b(?:customs officer|carinski sluzbenik)\b",
}


@dataclass(frozen=True)
class ProfessionalProfile:
    role: str = "Unknown"
    seniority: str = "Unknown"
    role_method: str = "Not found"
    role_evidence: str = ""
    seniority_method: str = "Not found"
    seniority_evidence: str = ""


def _personal_role(text: str) -> list[str]:
    # Employment in a prosecutor's office does not make every staff member a
    # prosecutor. Remove institution names before detecting a personal title.
    text = re.sub(r"\bprosecutor(?:['’]s|s['’]?)?\s+office\b", "", text)
    if re.search(r"\b(?:assistant|secretary|advisor|adviser) to\b|\b(?:not|never)\b", text):
        return []
    return [role for role, pattern in ROLE_PATTERNS.items() if re.search(pattern, text)]


def _leadership(text: str) -> str | None:
    text = re.sub(r"\bprosecutor(?:['’]s|s['’]?)?\s+office\b", "", text)
    if re.search(r"\b(?:former|previous|retired|ex[ -])|\b(?:assistant|secretary|advisor|adviser) to\b|\b(?:not|never)\b", text):
        return None
    if re.search(r"\b(?:department|unit|division|section)\s+(?:head|chief)|\b(?:head|chief)\s+of\s+(?:the\s+)?(?:\w+\s+){0,5}(?:department|unit|division|section)\b", text):
        return "Department / unit head"
    if re.search(r"\b(?:chief (?:public |cantonal |state |district )?prosecutor|glavni tuzilac|glavni tuzitelj|chief of police|director|commissioner|chief judge|president of (?:the )?court)\b", text):
        return "Institution leadership"
    if re.search(r"\b(?:senior|deputy chief|deputy director|deputy prosecutor|visi tuzilac|visi tuzitelj)\b", text):
        return "Senior role"
    return None


def infer_professional_profile(profile: dict) -> ProfessionalProfile:
    role, seniority = "Unknown", "Unknown"
    role_method = seniority_method = "Not found"
    role_evidence = seniority_evidence = ""
    for key in ("position", "rank"):
        original = str(profile.get(key) or "").strip()
        text = normalize_text(original)
        if not re.search(r"\b(?:former|retired|previous|ex[ -])", text):
            roles = _personal_role(text)
            if role == "Unknown" and len(roles) == 1:
                role, role_method, role_evidence = roles[0], f"Stored {key}", original
        level = _leadership(text)
        if seniority == "Unknown" and level:
            seniority, seniority_method, seniority_evidence = level, f"Stored {key}", original

    bio = str(profile.get("bio_short") or "")
    for sentence in re.split(r"(?<=[.!?;])\s+", bio):
        text = normalize_text(sentence)
        # Present-tense assertions take priority over earlier career mentions.
        current = re.search(r"\b(?:i am|she is|he is|i have been|she has been|he has been|currently|since\b.{0,80}\b(?:named|appointed))\s+(.+)", text)
        if not current or re.search(r"\b(?:not|never|retired|former|no longer)\b", text):
            continue
        roles = _personal_role(current.group(1))
        if role == "Unknown" and len(roles) == 1:
            role, role_method, role_evidence = roles[0], "Biography assertion", sentence.strip()
        level = _leadership(current.group(1))
        if seniority == "Unknown" and level:
            seniority, seniority_method, seniority_evidence = level, "Biography assertion", sentence.strip()
    return ProfessionalProfile(role, seniority, role_method, role_evidence, seniority_method, seniority_evidence)


def resolve_professional_country(profile: dict, country_names: dict, snapshot: dict | None = None) -> dict:
    """Prefer stored affiliation; infer jurisdiction only from explicit evidence."""
    for source, values in (("Attendance country", snapshot or {}), ("Profile country", profile)):
        value = values.get("representing_country")
        code = country_code(value, country_names)
        if code:
            return dict(code=code, method=source, evidence=str(value))
    for key in ("position", "organization"):
        original = str(profile.get(key) or "").strip()
        text = normalize_text(original)
        # Explicit jurisdiction in an institutional field, never a surname,
        # citizenship, event location, or an incidental biography location.
        matches = {country_code(match.group(0)) for match in re.finditer(
            r"\b(?:albania|bosnia (?:and|&) herzegovina|bosna i hercegovina|bih|croatia|hrvatska|kosovo|montenegro|crna gora|north macedonia|serbia|srbija)\b", text)}
        matches.discard(None)
        if len(matches) == 1:
            return dict(code=matches.pop(), method="Inferred from institution", evidence=original)
        if not matches and re.search(r"\btuzla canton\b", text) and re.search(r"\bprosecutor(?:['’]s)?\b", text):
            return dict(code="BA", method="Inferred from institution", evidence=original)
        if not matches and re.search(r"\bsremska mitrovica\b", text) and re.search(r"\bprosecutor(?:['’]s)?\b", text):
            return dict(code="RS", method="Inferred from institution", evidence=original)
    return dict(code=None, method="Unresolved country reference", evidence=str(profile.get("representing_country") or ""))
