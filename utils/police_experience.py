"""Conservative extraction of police-service evidence from professional bios.

Durations are reported as stated, never advanced using an import/update timestamp.
Joining-year estimates measure time since joining, not verified continuous service.
"""

from dataclasses import dataclass
from datetime import date
import re

from domain.reporting import normalize_text


@dataclass(frozen=True)
class PoliceExperience:
    years: int | None = None
    method: str = "Not found"
    evidence: str = ""
    joining_year: int | None = None


_POLICE = r"(?:the )?(?:police(?: (?:force|service))?|law enforcement|policing|policiji|policije)"
_DURATIONS = (
    rf"\b(?P<years>\d{{1,2}})\s+years?\s+(?:(?:of |total )?(?:professional )?(?:experience|service|work)\s+)?(?:in|with|at)\s+{_POLICE}\b",
    rf"\b(?P<years>\d{{1,2}})\s+years?\s+(?:of )?(?:total )?{_POLICE}\s+(?:experience|service)\b",
    rf"\b(?:served|worked)\s+(?:in|with|for)\s+{_POLICE}\s+for\s+(?P<years>\d{{1,2}})\s+years?\b",
    r"\b(?P<years>\d{1,2})\s+years?\s+(?:(?:of )?(?:experience|service|work)\s+)?as\s+(?:an?\s+)?(?:police|law enforcement) officer\b",
    r"\b(?:police|law enforcement) officer\s+for\s+(?P<years>\d{1,2})\s+years?\b",
    r"\b(?P<years>\d{1,2})\s+godin[ae]\s+(?:rada|(?:radnog )?iskustva|sluzbe|staza)\s+u\s+policiji\b",
)
_JOINING = (
    rf"\b(?:joined|entered)\s+{_POLICE}\s+(?:in\s+)?(?P<year>(?:19|20)\d{{2}})\b",
    rf"\b(?:working|worked|serving|served)\s+(?:in|with|for)\s+{_POLICE}\s+since\s+(?P<year>(?:19|20)\d{{2}})\b",
    rf"\b{_POLICE}\s+(?:officer\s+)?since\s+(?P<year>(?:19|20)\d{{2}})\b",
    r"\bu policiji\s+od\s+(?P<year>(?:19|20)\d{2})\b",
    r"\b(?:started|began)\s+(?:(?:his|her|my|their|a)\s+)?police career\s+in\s+(?P<year>(?:19|20)\d{2})\b",
)


def extract_police_experience(bio: object, *, reference_date: date) -> PoliceExperience:
    original = str(bio or "").strip()
    if not original:
        return PoliceExperience()
    candidates: list[tuple[str, int, str]] = []
    uncertain = False
    for sentence in re.split(r"(?<=[.!?;])\s+|\n+", original):
        text = normalize_text(sentence)
        for method, patterns, group in (
            ("Stated duration", _DURATIONS, "years"),
            ("Years since joining (estimate)", _JOINING, "year"),
        ):
            for pattern in patterns:
                for match in re.finditer(pattern, text):
                    value = int(match.group(group))
                    candidates.append((method, value, sentence))
                    prefix = text[max(0, match.start() - 40):match.start()]
                    suffix = text[match.end():match.end() + 55]
                    # An approximate/ranged duration or service in a specific unit
                    # must not become an exact total-police-service figure.
                    uncertain |= bool(re.search(r"(?:over|more than|at least|about|approximately|nearly|almost|around|\d+\s*[-–]\s*)\s*$", prefix))
                    uncertain |= bool(re.search(r"\d+\.\s*$|\b(?:not|never)\b", prefix))
                    uncertain |= bool(re.search(r"^\s+(?:(?:as|in|at|within)\s+)?(?:an?\s+)?(?:(?:cybercrime|narcotics|criminal|special)\s+)?(?:unit|department|division|academy|investigator|investigations|detective|trainer|instructor|intelligence|administration|operations)\b", suffix))
                    uncertain |= bool(re.search(r"\b(?:investigator|detective|trainer|instructor|chief|commissioner|unit|department)\b.{0,30}\bfor\s*$", prefix))
    if not candidates:
        return PoliceExperience()
    if re.search(r"\b(?:retired|retirement|left the police|career break|interrupted|penzionisan|umirovljen)\b", normalize_text(original)):
        uncertain = True
    durations = {value for method, value, _ in candidates if method == "Stated duration"}
    joining = {value for method, value, _ in candidates if method != "Stated duration"}
    evidence = " | ".join(dict.fromkeys(item[2] for item in candidates))
    if uncertain or len(durations) > 1 or len(joining) > 1:
        return PoliceExperience(method="Needs review", evidence=evidence)
    if durations:
        years = next(iter(durations))
        if not 0 <= years <= 80:
            return PoliceExperience(method="Needs review", evidence=evidence)
        return PoliceExperience(years=years, method="Stated duration", evidence=evidence)
    joined = next(iter(joining))
    years = reference_date.year - joined
    if not 0 <= years <= 80:
        return PoliceExperience(method="Needs review", evidence=evidence, joining_year=joined)
    return PoliceExperience(years=years, method="Years since joining (estimate)", evidence=evidence, joining_year=joined)
