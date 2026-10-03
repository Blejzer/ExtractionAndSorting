"""Read-only statistics over actual attendee records.

The calculation accepts plain documents so it can be verified without Atlas.
The adapter reads only reporting fields, in four queries, through the app's DB.
"""

from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import date, datetime
from statistics import median
from typing import Iterable

from domain.reporting import COUNTRIES, TRAINING_AREAS, country_code, normalize_text, training_areas
from utils.police_experience import extract_police_experience


# Approximation supplied by the programme owner in October 2026: the current
# seven-country policy has applied for about five years. Keep this historical
# assumption fixed rather than moving it forward each reporting year.
DEFAULT_POLICY_CHANGE = date(2021, 1, 1)


def _identifier(value: object, *fields: str) -> str:
    if isinstance(value, dict):
        value = next((value[key] for key in (*fields, "_id") if value.get(key)), "")
    return str(value).strip() if value is not None else ""


def _document_index(documents: list[dict], *fields: str) -> tuple[dict, dict]:
    """Resolve both programme IDs and Mongo IDs without double-counting people."""
    indexed, aliases = {}, {}
    for doc in documents:
        key = _identifier(doc, *fields)
        if key:
            indexed[key] = doc
            for field in (*fields, "_id"):
                if doc.get(field) is not None:
                    aliases[_identifier(doc[field])] = key
    return indexed, aliases


def _roster_ids(event: dict, linked: set[str], aliases: dict) -> set[str]:
    ids = set(linked)
    for field in ("participants", "participant_ids"):
        for value in event.get(field) or []:
            pid = _identifier(value, "pid", "participant_id")
            if pid:
                ids.add(aliases.get(pid, pid))
    return ids


def reporting_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).date()
        except ValueError:
            return None
    return None


def _distribution(counts: Counter, denominator: int, order: tuple[str, ...] | None = None) -> list[dict]:
    items = sorted(counts.items(), key=lambda item: order.index(item[0]) if order else (-item[1], item[0]))
    return [
        {"label": label, "count": count, "percent": round(100 * count / denominator, 1) if denominator else 0}
        for label, count in items
    ]


def _age_band(birth: date | None, reference: date) -> str:
    if not birth:
        return "Unknown"
    age = reference.year - birth.year - ((reference.month, reference.day) < (birth.month, birth.day))
    if not 18 <= age <= 100:
        return "Unknown"
    if age < 30:
        return "18–29"
    if age < 40:
        return "30–39"
    if age < 50:
        return "40–49"
    if age < 60:
        return "50–59"
    return "60+"


def _experience_band(years: int) -> str:
    if years < 5:
        return "0–4 years"
    if years < 10:
        return "5–9 years"
    if years < 20:
        return "10–19 years"
    if years < 30:
        return "20–29 years"
    return "30+ years"


def _invitation_policy(event: dict, when: date | None, policy_change: date | None) -> tuple[list[str] | None, int | None]:
    countries = event.get("invited_countries")
    quota = event.get("expected_per_country")
    if countries is not None:
        if not isinstance(countries, list) or any(code not in COUNTRIES for code in countries):
            return None, None
        return list(dict.fromkeys(countries)), quota if type(quota) is int and 1 <= quota <= 100 else None
    if policy_change and when:
        if when < policy_change:
            return [code for code in COUNTRIES if code != "HR"], 4
        return list(COUNTRIES), 3
    return None, None


def build_statistics(
    events: Iterable[dict], participants: Iterable[dict], links: Iterable[dict], countries: Iterable[dict],
    *, year: int | None = None, area: str | None = None, policy_change: date | None = None,
    as_of: date | None = None,
) -> dict:
    """Calculate automatically using programme rules and existing attendee data."""
    as_of = as_of or date.today()
    events, participants, links, countries = map(list, (events, participants, links, countries))
    event_index, event_aliases = _document_index(events, "eid", "event_id")
    profiles, profile_aliases = _document_index(participants, "pid", "participant_id")
    country_names = {
        normalize_text(doc[key]): country_code(doc.get("country")) or doc.get("iso") or doc.get("country", "")
        for doc in countries for key in ("cid", "_id") if doc.get(key) is not None
    }
    snapshots = {}
    linked = defaultdict(set)
    unmatched_links = 0
    for doc in links:
        eid = _identifier(doc.get("event_id") or doc.get("eid"), "eid", "event_id")
        pid = _identifier(doc.get("participant_id") or doc.get("pid"), "pid", "participant_id")
        eid, pid = event_aliases.get(eid, eid), profile_aliases.get(pid, pid)
        if eid and pid:
            if eid not in event_index:
                unmatched_links += 1
                continue
            key = (eid, pid)
            linked[key[0]].add(key[1])
            snapshots[key] = doc
        else:
            unmatched_links += 1

    automatic_policy = policy_change is None
    if automatic_policy:
        policy_change = DEFAULT_POLICY_CHANGE
        # Actual Croatian attendance is evidence that Croatia was included by
        # that event, but later first attendance does not postpone the stated
        # five-year policy (Croatia could have had intervening no-shows).
        for eid, event in event_index.items():
            if event.get("invited_countries") is not None:
                # An event-specific exception does not change the programme policy.
                continue
            when = reporting_date(event.get("start_date") or event.get("dateFrom"))
            if when and when < policy_change:
                for pid in _roster_ids(event, linked[eid], profile_aliases):
                    affiliation = snapshots.get((eid, pid), {}).get("representing_country") or profiles.get(pid, {}).get("representing_country")
                    if country_code(affiliation, country_names) == "HR":
                        policy_change = when
                        break

    selected = []
    years = set()
    future_events = 0
    future_attendance_events = 0
    for eid, event in event_index.items():
        when = reporting_date(event.get("start_date") or event.get("dateFrom"))
        if when and when > as_of:
            if not _roster_ids(event, linked[eid], profile_aliases):
                future_events += 1
                continue
        if when:
            years.add(when.year)
        tags, tag_source = training_areas(event)
        if year is not None and (when is None or when.year != year):
            continue
        if area and (area == "unclassified" and tags or area != "unclassified" and area not in tags):
            continue
        selected.append((event, eid, when, tags, tag_source))
        if when and when > as_of:
            future_attendance_events += 1
    selected.sort(key=lambda item: (item[2] or date.min, item[1]), reverse=True)

    country_rows = {
        code: dict(code=code, country=label, attendances=0, unique_people=0, attended_events=0, invited_events=0,
                   no_show_events=0, partial_events=0, met_events=0, extra_events=0,
                   unassessed_events=0, assessed_events=0, allocated_events=0,
                   shortfall=0, additional=0, expected_places=0, filled_places=0)
        for code, label in COUNTRIES.items()
    }
    country_people = defaultdict(set)
    area_events = Counter()
    area_attendances = Counter()
    area_people = defaultdict(set)
    area_country_attendances = defaultdict(Counter)
    area_country_people = defaultdict(set)
    report_events = []
    attendee_ids = set()
    latest_dates = {}
    unknown_profiles = set()
    unresolved_attendances = 0
    total_attendances = 0
    unconfigured_events = 0
    gender_by_country = defaultdict(Counter)

    for event, eid, when, tags, tag_source in selected:
        ids = _roster_ids(event, linked[eid], profile_aliases)
        attendee_ids.update(ids)
        total_attendances += len(ids)
        counts = Counter()
        event_country_people = defaultdict(set)
        unknown_country = 0
        for pid in ids:
            profile = profiles.get(pid, {})
            if not profile:
                unknown_profiles.add(pid)
            affiliation = snapshots.get((eid, pid), {}).get("representing_country") or profile.get("representing_country")
            code = country_code(affiliation, country_names)
            if code:
                counts[code] += 1
                country_rows[code]["attendances"] += 1
                country_people[code].add(pid)
                event_country_people[code].add(pid)
            else:
                unknown_country += 1
            if when and when <= as_of and (pid not in latest_dates or when > latest_dates[pid]):
                latest_dates[pid] = when
        unresolved_attendances += unknown_country
        invited, quota = _invitation_policy(event, when, policy_change)
        if invited is None or quota is None:
            unconfigured_events += 1
        cells = []
        for code in COUNTRIES:
            count = counts[code]
            row = country_rows[code]
            if count:
                row["attended_events"] += 1
            if invited is None:
                status = "Policy not set"
            elif code not in invited:
                status = "Not invited"
            else:
                row["invited_events"] += 1
                if not ids:
                    status = "No attendance uploaded"
                elif unknown_country:
                    status = "Roster unresolved"
                elif count == 0:
                    status = "No-show"
                    row["no_show_events"] += 1
                elif quota is None:
                    status = "Attended — allocation not set"
                elif count < quota:
                    status = "Attended below allocation"
                    row["partial_events"] += 1
                elif count == quota:
                    status = "Met allocation"
                    row["met_events"] += 1
                else:
                    status = "Above allocation"
                    row["extra_events"] += 1
                if ids and not unknown_country and quota is not None:
                    row["expected_places"] += quota
                    row["filled_places"] += min(count, quota)
                    row["shortfall"] += max(quota - count, 0)
                    row["additional"] += max(count - quota, 0)
            if status in ("Policy not set", "Roster unresolved", "Attended — allocation not set", "No attendance uploaded"):
                row["unassessed_events"] += 1
            if invited is not None and code in invited and ids and not unknown_country:
                row["assessed_events"] += 1
                if quota is not None:
                    row["allocated_events"] += 1
            cells.append(dict(code=code, count=count, status=status, expected=quota if invited and code in invited else None))
        for tag in tags or ["unclassified"]:
            area_events[tag] += 1
            area_attendances[tag] += len(ids)
            area_people[tag].update(ids)
            for code, people in event_country_people.items():
                area_country_attendances[tag][code] += len(people)
                area_country_people[(tag, code)].update(people)
        report_events.append(dict(eid=eid, title=event.get("title") or eid, date=when.isoformat() if when else "Undated",
                                  areas=[TRAINING_AREAS[tag] for tag in tags], area_source=tag_source,
                                  attendee_count=len(ids), unresolved=unknown_country, countries=cells))

    for code, row in country_rows.items():
        row["unique_people"] = len(country_people[code])
        row["places_filled_percent"] = round(100 * row["filled_places"] / row["expected_places"], 1) if row["expected_places"] else None

    distributions = {key: Counter() for key in ("gender", "age", "organization", "rank", "position")}
    experiences = []
    experience_bands = Counter()
    experience_methods = Counter()
    for pid in sorted(attendee_ids):
        profile = profiles.get(pid, {})
        reference = latest_dates.get(pid)
        gender = {"male": "Male", "female": "Female"}.get(normalize_text(profile.get("gender")), "Unknown")
        distributions["gender"][gender] += 1
        code = country_code(profile.get("representing_country"), country_names)
        gender_by_country[code or "Unknown"][gender] += 1
        distributions["age"][_age_band(reporting_date(profile.get("dob")), reference) if reference else "Unknown"] += 1
        for key in ("organization", "rank", "position"):
            value = " ".join(str(profile.get(key) or "").split())
            distributions[key][value if value and value not in ("/", "-", "—") else "Unknown"] += 1
        extraction = extract_police_experience(profile.get("bio_short"), reference_date=reference or as_of)
        if reference is None and extraction.joining_year is not None:
            # Undated events provide no defensible date for a historical estimate.
            extraction = type(extraction)(method="Needs review", evidence=extraction.evidence, joining_year=extraction.joining_year)
        experience_methods[extraction.method] += 1
        if extraction.years is not None:
            experience_bands[_experience_band(extraction.years)] += 1
        experiences.append(dict(pid=pid, name=profile.get("name") or pid, country=COUNTRIES.get(code, "Unknown"),
                                reference_date=reference.isoformat() if reference else None, **asdict(extraction)))
    extracted = [item["years"] for item in experiences if item["years"] is not None]
    unique = len(attendee_ids)
    return dict(
        invitation_policy=dict(mode="automatic" if automatic_policy else "configured",
                               change_date=policy_change.isoformat(),
                               estimated=automatic_policy),
        data_coverage=dict(stored_events=len(events), stored_profiles=len(participants), stored_links=len(links),
                           unmatched_links=unmatched_links, invalid_events=len(events) - len(event_index),
                           future_events=future_events, future_attendance_events=future_attendance_events),
        summary=dict(events=len(selected), unique_people=unique, attendances=total_attendances,
                     missing_profiles=len(unknown_profiles), unresolved_attendances=unresolved_attendances,
                     unconfigured_events=unconfigured_events, undated_events=sum(item[2] is None for item in selected),
                     empty_rosters=sum(event["attendee_count"] == 0 for event in report_events)),
        years=sorted(years, reverse=True), countries=list(country_rows.values()), events=report_events,
        areas=[dict(key=key, label=label, events=area_events[key], attendances=area_attendances[key], unique_people=len(area_people[key]),
                    countries=[dict(code=code, attendances=area_country_attendances[key][code], unique_people=len(area_country_people[(key, code)]))
                               for code in COUNTRIES])
               for key, label in {**TRAINING_AREAS, "unclassified": "Unclassified"}.items()],
        diversity={key: _distribution(counts, unique, ("18–29", "30–39", "40–49", "50–59", "60+", "Unknown") if key == "age" else None)
                   for key, counts in distributions.items()},
        gender_by_country=[dict(country=COUNTRIES.get(code, "Unknown"), total=sum(counts.values()),
                               male=counts["Male"], female=counts["Female"], unknown=counts["Unknown"])
                           for code, counts in gender_by_country.items()],
        experience=dict(known=len(extracted), total=unique, coverage_percent=round(100 * len(extracted) / unique, 1) if unique else 0,
                        median_years=median(extracted) if extracted else None, methods=_distribution(experience_methods, unique),
                        bands=_distribution(experience_bands, len(extracted), ("0–4 years", "5–9 years", "10–19 years", "20–29 years", "30+ years")),
                        records=experiences),
    )


def fetch_statistics(**filters) -> dict:
    from config.database import mongodb

    events = list(mongodb.collection("events").find({}, {
        "_id": 1, "eid": 1, "event_id": 1, "title": 1, "start_date": 1, "dateFrom": 1,
        "participants": 1, "participant_ids": 1, "training_areas": 1,
        "invited_countries": 1, "expected_per_country": 1,
    }))
    links = list(mongodb.collection("participant_events").find({}, {
        "_id": 0, "event_id": 1, "participant_id": 1, "eid": 1, "pid": 1, "representing_country": 1,
    }))
    participants = list(mongodb.collection("participants").find({}, {
        "_id": 1, "pid": 1, "participant_id": 1, "name": 1, "representing_country": 1, "gender": 1,
        "dob": 1, "organization": 1, "rank": 1, "position": 1, "bio_short": 1,
    }))
    countries = list(mongodb.collection("countries").find({}, {"_id": 1, "cid": 1, "country": 1, "iso": 1}))
    return build_statistics(events, participants, links, countries, **filters)
