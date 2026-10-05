"""Read-only candidate matching; never insert, update, or merge a participant."""

from collections import defaultdict
from difflib import SequenceMatcher

from domain.reporting import country_code, normalize_text
from services.word_extraction_service import name_key, parse_date, word_country


def _email(value):
    text = str(value or "").strip().casefold()
    return text if "@" in text else ""


class WordMatchContext:
    def __init__(self, participants, countries):
        self.countries = {}
        self.by_country = defaultdict(set)
        for doc in countries:
            cid, label = str(doc.get("cid") or "").strip(), str(doc.get("country") or "").strip()
            if not cid or not label:
                continue
            self.countries[cid] = label
            for value in (cid, label, doc.get("iso", "")):
                if value:
                    self.by_country[normalize_text(value)].add(cid)
                    code = country_code(value)
                    if code:
                        self.by_country[code].add(cid)
        self.participants = []
        for doc in participants:
            if not doc.get("pid") or not doc.get("name"):
                continue
            self.participants.append({
                "pid": str(doc["pid"]), "name": str(doc["name"]),
                "country": str(doc.get("representing_country") or ""), "dob": parse_date(doc.get("dob")),
                "email": _email(doc.get("email")), "name_key": name_key(doc["name"]),
                "position": str(doc.get("position") or ""), "organization": str(doc.get("organization") or ""),
            })
        self.by_name, self.by_email = defaultdict(list), defaultdict(list)
        for participant in self.participants:
            self.by_name[participant["name_key"]].append(participant)
            if participant["email"]:
                self.by_email[participant["email"]].append(participant)

    def resolve_country(self, label):
        options = self.by_country.get(normalize_text(label), set())
        if not options:
            code = word_country(label)
            options = self.by_country.get(code, set()) if code else set()
        return next(iter(options)) if len(options) == 1 else ""

    def check(self, record):
        fields = record["fields"]
        record["match_notes"] = []
        cid = fields["representing_country"] or self.resolve_country(fields["country_label"])
        if cid and cid not in self.countries:
            record.update(matches=[], match_status="Review required")
            record["match_notes"].append("Choose a representing-country CID from the country catalog.")
            return
        fields["representing_country"] = cid
        if not cid:
            record["match_notes"].append("Select the representing country; no unique country CID was resolved.")
        key, dob, email = name_key(fields["name"]), parse_date(fields["dob"]), _email(fields["email"])
        if not key:
            record.update(matches=[], match_status="Review required")
            record["match_notes"].append("Enter a participant name before checking.")
            return
        candidates = {p["pid"]: p for p in self.by_name.get(key, [])}
        candidates.update({p["pid"]: p for p in self.by_email.get(email, [])} if email else {})
        # Similar spellings are candidates only, never an automatic identity decision.
        for participant in self.participants:
            if cid and participant["country"] != cid:
                continue
            similar = SequenceMatcher(None, key, participant["name_key"]).ratio() >= .88
            if similar and (not dob or not participant["dob"] or dob == participant["dob"]):
                candidates[participant["pid"]] = participant
        matches = []
        for participant in candidates.values():
            reasons = []
            same_name = key == participant["name_key"]
            same_country = bool(cid) and cid == participant["country"]
            same_dob = bool(dob) and dob == participant["dob"]
            reasons.append("Name matches (ignoring accents/order)" if same_name else "Name differs; inspect the profile")
            if same_country:
                reasons.append("Country CID matches")
            elif cid and participant["country"]:
                reasons.append("Country differs")
            else:
                reasons.append("Country is missing")
            if same_dob:
                reasons.append("DOB matches")
            elif dob and participant["dob"]:
                reasons.append("DOB differs")
            else:
                reasons.append("DOB is missing")
            if email and email == participant["email"]:
                reasons.append("Email matches")
            exact = same_name and same_country and same_dob
            matches.append({"pid": participant["pid"], "name": participant["name"],
                            "country": self.countries.get(participant["country"], participant["country"]),
                            "dob": participant["dob"], "position": participant["position"],
                            "organization": participant["organization"], "reasons": reasons, "exact": exact})
        matches.sort(key=lambda p: (not p["exact"], "DOB differs" in p["reasons"], p["pid"]))
        record["matches"] = matches[:20]
        if len(matches) > 20:
            record["match_notes"].append(f"Showing 20 of {len(matches)} candidates. Add DOB/country to narrow the search.")
        exact_matches = sum(m["exact"] for m in matches)
        if exact_matches == 1:
            record["match_status"] = "Exact match"
        elif exact_matches > 1:
            record["match_status"] = "Multiple exact matches"
        elif matches:
            record["match_status"] = "Possible match"
        elif not cid:
            record["match_status"] = "Review required"
        else:
            record["match_status"] = "No candidate found"
        if not dob:
            record["match_notes"].append("DOB is missing; name/country candidates still need review.")


def load_match_context():
    from config.database import mongodb
    participants = list(mongodb.collection("participants").find({}, {
        "_id": 0, "pid": 1, "name": 1, "representing_country": 1, "dob": 1,
        "email": 1, "position": 1, "organization": 1,
    }))
    countries = list(mongodb.collection("countries").find({}, {"_id": 0, "cid": 1, "country": 1, "iso": 1}))
    return WordMatchContext(participants, countries)
