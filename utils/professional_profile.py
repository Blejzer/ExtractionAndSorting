"""Evidence-based professional role and leadership labels for reporting."""

from dataclasses import dataclass
import re

from domain.reporting import normalize_text


ROLE_PATTERNS = {
    "Prosecutor": r"\b(?:prosecutor|prossecutor|tuzilac|tuzitelj|tuziteljica|tuziteljka)\b",
    "Judge": r"\b(?:judge|sudija|sudac|sutkinja)\b",
    "Police officer": r"\b(?:police officer|policeman|policewoman|policijski sluzbenik|policijska sluzbenica)\b",
    "Lawyer": r"\b(?:lawyer|attorney|advokat|odvjetnik|odvjetnica)\b",
    "Customs officer": r"\b(?:customs officer|carinski sluzbenik)\b",
}


# Regional employer aliases, including police agencies whose names omit "police".
# Acronyms apply to employer/title fields and explicit current employment only.
ORGANIZATION_PATTERNS = {
    "Police": (
        r"\b(?:police|policing|policij\w*|polici(?:a|se)?|policor\w*|полиц\w*|law enforcement|"
        r"asp(?!\.net\b)|mup|mvr|fmup|sipa|fup|ukp|sbpok|муп|мвр|"
        r"ministry of (?:the )?(?:interior|internal affairs)|"
        r"ministarstvo (?:unutrasnjih|unutarnjih) poslova|"
        r"министарство (?:унутрашњих|унутарњих) послова|министерство за внатрешни работи|"
        r"state investigation and protection agency|drzavna agencija za istrage i zastitu)\b"
    ),
    "Prosecutor": (
        r"\b(?:prosecutor|prosecution|prossecutor|tuzilac|tuzilastv\w*|tuzitelj\w*|"
        r"prokuror\w*|тужила\w*|тужите\w*|обвинител\w*|javn\w* obvinitel\w*)\b"
    ),
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


def organization_group(profile: dict) -> str | None:
    """Group employer sectors without exposing agency names or relying on rank."""
    for field in ("organization", "position"):
        text = normalize_text(profile.get(field))
        matches = [group for group, pattern in ORGANIZATION_PATTERNS.items() if re.search(pattern, text)]
        if matches:
            return matches[0] if len(matches) == 1 else None
    # Present employment in a biography can fill missing employer/title fields.
    # Rank alone is too inconsistent to establish the organization category.
    role = infer_professional_profile({"position": profile.get("position"), "bio_short": profile.get("bio_short")}).role
    if role in ("Police officer", "Prosecutor"):
        return {"Police officer": "Police", "Prosecutor": "Prosecutor"}[role]
    groups = set()
    for sentence in re.split(r"(?<=[.!?;])\s+|\n+", str(profile.get("bio_short") or "")):
        text = normalize_text(sentence)
        if re.search(r"\b(?:former|retired|previous|was|were|worked|not|never|no longer|"
                     r"bio|bila|biv\w*|ranije|prethodno|nekad\w*|penzion\w*|nije|nisam)\b", text):
            continue
        if re.search(r"\b(?:zaposlen\w*|radi|radim|trenutno|obavlja\w*|do danas|"
                     r"i work|i am employed|he works|she works|(?:he|she) is employed|"
                     r"currently (?:working|employed)|(?:have|has) been (?:working|employed))\b", text):
            groups.update(group for group, pattern in ORGANIZATION_PATTERNS.items() if re.search(pattern, text))
    return next(iter(groups)) if len(groups) == 1 else None
