"""Professional service evidence, keeping bounds distinct from exact years."""

from dataclasses import dataclass
from datetime import date
import re

from domain.reporting import normalize_text
from utils.police_experience import extract_police_experience
from utils.professional_profile import ROLE_PATTERNS


@dataclass(frozen=True)
class ProfessionalExperience:
    years: int | None = None
    method: str = "Not found"
    evidence: str = ""
    joining_year: int | None = None
    scope: str = "Unknown"
    min_years: int | None = None
    max_years: int | None = None
    qualifier: str | None = None
    display_years: str = "Unknown"
    career_start_year: int | None = None
    role_start_year: int | None = None
    role_years: int | None = None


_ROLES = {
    "Police": r"(?:the )?(?:police(?: (?:officer|force|service))?|law enforcement(?: officer)?|policing|policiji|policije)",
    "Prosecution": r"(?:(?:cantonal|public|state|district|municipal|chief|senior|deputy)\s+)*(?:prosecutor|prosecution|tuzilac|tuzitelj|tuzilastvu|tuziteljstvu)",
    "Judiciary": r"(?:judge|judiciary|judicial service|sudija|sudac|sutkinja)",
    "Legal practice": r"(?:lawyer|attorney|legal practice|advokat|odvjetnik)",
    "Customs": r"(?:customs(?: officer| service)?|carinski sluzbenik)",
}
_ROLE_SCOPE = {"Police officer": "Police", "Prosecutor": "Prosecution", "Judge": "Judiciary", "Lawyer": "Legal practice", "Customs officer": "Customs"}
_NUMBER = r"(?P<qualifier>over|more than|at least|about|approximately|nearly|almost|around)?\s*(?P<years>\d{1,3})(?P<plus>\+)?(?:\s*[-–]\s*(?P<upper>\d{1,3}))?\s+years?"


def _career_timeline(original: str, reference: date, role: str) -> ProfessionalExperience | None:
    # Dated employment bullets, including the application's flattened Word
    # timelines: "2012. - 2021. - Deputy Public Prosecutor ...". Education years
    # without an employment title do not qualify as a career start.
    matches = list(re.finditer(
        r"(?P<ongoing>\bfrom\s+)?(?<!\d)(?P<start>(?:19|20)\d{2})\s*\.?\s*[-–:]\s*"
        r"(?:(?P<end>(?:19|20)\d{2})\s*\.?\s*[-–:]\s*)?", original, re.IGNORECASE))
    entries = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(original)
        description = original[match.end():end].strip(" -\n")
        description = re.split(r"\b(?:graduated|passed the bar|education|qualifications)\b", description, flags=re.IGNORECASE)[0].strip(" -\n")
        text = normalize_text(description)
        personal = re.sub(r"\bprosecutor(?:['’]s|s['’]?)?\s+office\b", "", text)
        roles = [label for label, pattern in ROLE_PATTERNS.items() if re.search(pattern, personal)]
        scopes = {_ROLE_SCOPE[label] for label in roles}
        support = bool(re.search(r"\bexpert assistant\b", text) and re.search(r"\b(?:prosecution|prosecutor(?:['’]s)?)\s+office\b", text))
        if support:
            scopes.add("Prosecution")
        if len(scopes) != 1:
            continue
        start = int(match.group("start"))
        finish = int(match.group("end")) if match.group("end") else None
        if start > reference.year:
            continue
        entries.append(dict(start=start, end=finish, ongoing=bool(match.group("ongoing")),
                            scope=scopes.pop(), support=support, roles=roles,
                            evidence=original[match.start():match.end()] + description))
    if not entries:
        return None
    scopes = {entry["scope"] for entry in entries}
    evidence = " | ".join(entry["evidence"] for entry in entries)
    if len(scopes) != 1 or any(entry["end"] is None and not entry["ongoing"] for entry in entries):
        return ProfessionalExperience(method="Needs review", scope="Career timeline", evidence=evidence)
    entries.sort(key=lambda entry: entry["start"])
    scope = entries[0]["scope"]
    previous_end = None
    for index, entry in enumerate(entries):
        finish = min(entry["end"], reference.year) if entry["end"] is not None else reference.year
        if (finish < entry["start"] or (previous_end is not None and entry["start"] > previous_end + 1)
                or (entry["end"] is None and index != len(entries) - 1)):
            return ProfessionalExperience(method="Needs review", scope=scope, evidence=evidence)
        previous_end = max(previous_end or finish, finish)
    first = entries[0]["start"]
    years = previous_end - first
    if not 0 <= years <= 80 or re.search(r"\b(?:retired|career break|interrupted)\b", normalize_text(original)):
        return ProfessionalExperience(method="Needs review", scope=scope, evidence=evidence)
    personal_starts = [entry["start"] for entry in entries if role in entry["roles"]]
    personal_first = min(personal_starts) if personal_starts else None
    # Support service counts in the professional career, but does not backdate
    # appointment as a prosecutor or judge to an assistant's start year.
    return ProfessionalExperience(years=years, method="Career timeline (estimate)", evidence=evidence,
                                  scope=scope + (" (including support roles)" if any(entry["support"] for entry in entries) else ""),
                                  joining_year=first, career_start_year=first, role_start_year=personal_first,
                                  role_years=previous_end - personal_first if personal_first is not None else None,
                                  display_years=f"{years} (estimate)")


def extract_professional_experience(bio: object, *, reference_date: date, role: str = "Unknown") -> ProfessionalExperience:
    original = str(bio or "").strip()
    candidates = []
    for sentence in re.split(r"(?<=[.!?;])\s+", original):
        text = normalize_text(sentence)
        for scope, profession in _ROLES.items():
            patterns = (
                rf"\b{profession}\s+for\s+{_NUMBER}\b",
                rf"\b{_NUMBER}\s+(?:(?:of )?(?:total |professional )?(?:experience|service|work)\s+)?(?:in|with|at|as)\s+(?:an?\s+)?{profession}\b",
                rf"\b{_NUMBER}\s+(?:of )?(?:total )?{profession}\s+(?:experience|service)\b",
            )
            for pattern in patterns:
                for match in re.finditer(pattern, text):
                    years = int(match.group("years"))
                    upper = int(match.group("upper")) if match.group("upper") else None
                    qualifier = match.group("qualifier")
                    if match.group("plus"):
                        qualifier = qualifier or "at least"
                    before = text[:match.start()]
                    after = text[match.end():]
                    uncertain = bool(re.search(r"\b(?:not|never|no longer|retired|left|interrupted|career break)|\b(?:haven't|hasn't|isn't|wasn't)\b", text))
                    uncertain |= bool(re.search(r"\d[.,]\s*$", before))
                    uncertain |= bool(re.match(r"\s+(?:(?:as|in|at|within)\s+)?(?:an?\s+)?(?:(?:cybercrime|narcotics|criminal|special)\s+)?(?:unit|department|division|investigations|investigator|detective|trainer|instructor|academy)\b", after))
                    uncertain |= bool(re.search(r"\b(?:investigator|detective|trainer|instructor|chief|commissioner|unit|department)\b.{0,30}\bfor\s*$", before))
                    uncertain |= not (0 <= years <= 80) or (upper is not None and not years <= upper <= 80)
                    candidates.append((scope, years, upper, qualifier, uncertain, sentence.strip()))

    if candidates:
        scopes = {item[0] for item in candidates}
        preferred = _ROLE_SCOPE.get(role)
        if preferred in scopes:
            candidates = [item for item in candidates if item[0] == preferred]
            scopes = {preferred}
        evidence = " | ".join(dict.fromkeys(item[5] for item in candidates))
        # Durations in different professions may overlap: never add them.
        distinct = {item[:4] for item in candidates}
        scope = next(iter(scopes)) if len(scopes) == 1 else "Multiple professions"
        if len(distinct) != 1 or any(item[4] for item in candidates):
            return ProfessionalExperience(method="Needs review", scope=scope, evidence=evidence)
        _, years, upper, qualifier, _, _ = candidates[0]
        if upper is not None:
            return ProfessionalExperience(method="Stated range", scope=scope, evidence=evidence,
                                          min_years=years, max_years=upper, qualifier="range", display_years=f"{years}–{upper}")
        if qualifier in ("over", "more than", "at least"):
            return ProfessionalExperience(method="Stated lower bound", scope=scope, evidence=evidence,
                                          min_years=years, qualifier="more_than" if qualifier in ("over", "more than") else "at_least",
                                          display_years=f"{years}+")
        if qualifier:
            return ProfessionalExperience(method="Approximate duration", scope=scope, evidence=evidence,
                                          qualifier=qualifier, display_years=f"{qualifier} {years}")
        return ProfessionalExperience(years=years, method="Stated duration", scope=scope, evidence=evidence,
                                      display_years=str(years))

    timeline = _career_timeline(original, reference_date, role)
    if timeline:
        return timeline
    # Preserve the existing police joining-year and BCS total-service coverage.
    police = extract_police_experience(original, reference_date=reference_date)
    return ProfessionalExperience(years=police.years, method=police.method, evidence=police.evidence,
                                  joining_year=police.joining_year, scope="Police" if police.method != "Not found" else "Unknown",
                                  display_years=str(police.years) if police.years is not None else "Unknown")
