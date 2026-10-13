"""Explicit current employment statements; never infer a represented country."""

import re

from domain.reporting import normalize_text


_CURRENT = re.compile(
    r"\b(?:i\s+(?:am|have been|work|serve)|(?:he|she)\s+(?:is|has been|works|serves))\s+"
    r"(?:(?:currently|now)\s+)?(?:(?:working|serving|employed)\s+)?(?:as\s+)?(?:(?:an?|and|the)\s+)?"
    r"|^(?:currently|since\s+(?:19|20)\d{2}|from\s+(?:19|20)\d{2})[,\s]+"
    r"(?:(?:i|he|she)\s+)?(?:(?:am|is|have been|has been|work|works|working|serving|employed)\s+)?"
    r"(?:as\s+)?(?:(?:an?|and|the)\s+)?",
    re.I,
)
_ROLE = re.compile(
    r"^(?P<value>(?:(?:senior|chief|deputy|assistant|criminal|police|public|cantonal|state|"
    r"financial|cybercrime|narcotics|lead|principal|special|technical)\s+){0,5}"
    r"(?:investigator|detective|inspector|officer|prosecutor|judge|analyst|specialist|"
    r"advisor|adviser|lawyer|attorney|director|commissioner|manager|head|chief))\b",
    re.I,
)
_UNIT = re.compile(
    r"\b(?:in|at|within|of|with)\s+(?:the\s+)?"
    r"(?P<value>(?:[\w'’/-]+\s+){0,7}(?:unit|department|division|section|sector|bureau))\b",
    re.I,
)
_EMPLOYER = re.compile(
    r"\b(?P<value>Ministry\s+of\s+(?:the\s+)?(?:Interior(?:\s+Affairs)?|Internal\s+Affairs)"
    r"|(?:State\s+|National\s+)?Police\s+(?:Directorate|Service|Force|Department)"
    r"|(?:(?:Higher|Basic|Cantonal|State|District|Public)\s+){0,4}"
    r"(?:Prosecutor['’]s|Prosecution)\s+Office)\b",
    re.I,
)


def employment_from_bio(bio):
    """Return unique supported values and fields requiring review."""
    candidates = {field: {} for field in ("position", "unit", "organization")}
    for sentence in re.split(r"(?<=[.!?;])\s+", str(bio or "").strip()):
        text = normalize_text(sentence)
        if re.search(r"\b(?:not|never|no longer|former|previously|retired|was|were|worked)\b", text):
            continue
        assertion = _CURRENT.search(sentence.strip())
        if not assertion:
            continue
        employment = sentence.strip()[assertion.end():]
        role = _ROLE.match(employment)
        unit = _UNIT.search(employment)
        employer = _EMPLOYER.search(employment)
        # Without a title or an explicit work/appointment verb, institution
        # mentions (e.g. "I am attending training at ...") are not employment.
        work = bool(re.search(r"\b(?:work|works|working|serve|serves|serving|employed)\b", text))
        if not role and not work:
            continue
        for field, match in (("position", role), ("unit", unit), ("organization", employer)):
            if match:
                value = " ".join(match["value"].split())
                candidates[field][normalize_text(value)] = {"value": value, "evidence": sentence.strip()}
    values = {field: next(iter(options.values())) for field, options in candidates.items() if len(options) == 1}
    ambiguous = [field for field, options in candidates.items() if len(options) > 1]
    return values, ambiguous
