"""Explicit current employment statements; never infer a represented country."""

import re

from domain.reporting import normalize_text


_CURRENT = re.compile(
    r"\b(?:i\s+(?:am|have been|work|serve)|(?:he|she)\s+(?:is|has been|works|serves))\s+"
    r"(?:(?:currently|now)\s+)?(?:(?:working|serving|employed)\s+)?(?:as\s+)?(?:(?:an?|and|the)\s+)?"
    r"|^(?:currently|since\s+(?:19|20)\d{2}|from\s+(?:19|20)\d{2})[,\s]+"
    r"(?:(?:i|he|she)\s+)?(?:(?:am|is|have been|has been|work|works|working|serving|employed)\s+)?"
    r"(?:as\s+)?(?:(?:an?|and|the)\s+)?"
    r"|^\s*(?:professional\s+experience:\s*)?employed\s+(?=(?:with|at|in|by)\s+)",
    re.I | re.M,
)
_ROLE = re.compile(
    r"^(?P<value>(?:(?:senior|chief|deputy|assistant|criminal|police|public|cantonal|state|"
    r"financial|cybercrime|narcotics|lead|principal|special|technical)\s+){0,5}"
    r"(?:investigator|detective|inspector|officer|prosecutor|judge|analyst|specialist|"
    r"advisor|adviser|lawyer|attorney|director|commissioner|manager|head|chief))\b",
    re.I,
)
_UNIT = re.compile(
    r"(?:\b(?:in|at|within|of|with)\s+|[-–]\s*)(?:the\s+)?"
    r"(?P<value>(?:unit|department|division|section|sector)\s+(?:[IVXLC]+|\d+)"
    r"|(?:(?!(?:in|at|within|with|the|as|of)\b)[\w'’/-]+\s+){0,7}"
    r"(?:unit|department|division|section|sector|bureau))\b",
    re.I,
)
_EMPLOYER = re.compile(
    r"\b(?P<value>Ministry\s+of\s+(?:the\s+)?(?:Interior(?:\s+Affairs)?|Internal\s+Affairs)"
    r"|(?:State\s+|National\s+)?Police\s+(?:Directorate|Service|Force|Department)"
    r"|(?:(?:Higher|Basic|Cantonal|State|District|Public)\s+){0,4}"
    r"(?:Prosecutor(?:['’]s|s['’]?)?|Prosecution)\s+Office"
    r"(?:\s+of\s+(?:[\w'’/-]+\s+){0,5}[\w'’/-]+)?)\b",
    re.I,
)


def employment_from_bio(bio):
    """Return unique supported values and fields requiring review."""
    candidates = {field: {} for field in ("position", "unit", "organization")}
    for sentence in re.split(
        r"(?<=[.!?;])\s+|\n(?=\s*(?:(?:19|20)\d{2}\s*[-–:]|professional\s+experience:|education:))",
        str(bio or "").strip(), flags=re.I,
    ):
        sentence = sentence.strip()
        text = normalize_text(sentence)
        active = bool(re.search(r"\b(?:until today|to date|currently|at present)\b", text))
        if re.search(r"\b(?:not|never|no longer|former|previously|retired)\b", text):
            continue
        if re.search(r"\b(?:was|were|worked)\b", text) and not active:
            continue
        assertion = _CURRENT.search(sentence.strip())
        timeline = re.match(r"^\s*(?:19|20)\d{2}\s*[-–]\s*(?:currently|present|now|today)\b\s*[-–:]?\s*",
                            sentence, re.I)
        appointment = re.search(r"\b(?:elected|appointed|named|promoted)\s+(?:as\s+)?(?:an?\s+)?",
                                sentence, re.I) if active else None
        # A dated current title or continuing appointment is stronger than
        # a subordinate "I have been working ..." clause later in the sentence.
        assertion = timeline or appointment or assertion
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
