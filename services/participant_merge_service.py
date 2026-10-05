"""Explicit, reviewed participant merges over raw documents, including legacy data."""

from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import re
import unicodedata

from bson import json_util
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from config.database import mongodb
from utils.participants import refresh as refresh_participant_cache


class MergeError(ValueError):
    """A merge cannot proceed without a new review."""


def _encoded(value):
    return json_util.dumps(value, sort_keys=True, json_options=json_util.CANONICAL_JSON_OPTIONS)


def fingerprint(state):
    return hashlib.sha256(_encoded(state).encode()).hexdigest()


def _blank(value):
    return value is None or value == "" or value == [] or value == {} or (
        isinstance(value, str) and not value.strip()
    )


def _name_key(name):
    text = unicodedata.normalize("NFKD", str(name or "")).casefold()
    return " ".join("".join(c for c in text if not unicodedata.combining(c)).split())


class ParticipantMergeService:
    def __init__(self, database=None):
        self.db = database if database is not None else mongodb

    def candidates(self, search=""):
        """Suggest identical normalized names within a country; never merge automatically."""
        projection = {key: 1 for key in (
            "pid", "name", "representing_country", "position", "organization", "dob", "gender"
        )}
        records = [d for d in self.db.collection("participants").find({}, projection) if d.get("pid")]
        records.sort(key=lambda d: (str(d.get("name", "")).casefold(), d["pid"]))
        if search:
            needle = _name_key(search)
            return [d for d in records if needle in _name_key(d.get("name")) or needle in d["pid"].casefold()]
        groups = defaultdict(list)
        for doc in records:
            if _name_key(doc.get("name")):
                groups[(_name_key(doc.get("name")), doc.get("representing_country"))].append(doc)
        return [doc for group in groups.values() if len(group) > 1 for doc in group]

    def resolve_alias(self, pid):
        seen = set()
        while pid not in seen:
            seen.add(pid)
            alias = self.db.collection("participant_aliases").find_one({"_id": pid})
            if not alias:
                return pid
            pid = alias["target_pid"]
        raise MergeError("The participant redirect history needs repair.")

    def snapshot(self, pids, *, session=None):
        if not isinstance(pids, list) or not 2 <= len(pids) <= 10 or len(set(pids)) != len(pids):
            raise MergeError("Select between two and ten different participant records.")
        if any(not isinstance(pid, str) or not re.fullmatch(r"P\d+", pid) for pid in pids):
            raise MergeError("Select valid participant IDs.")
        pids = sorted(pids)
        def read(name, query):
            return sorted(
                list(self.db.collection(name).find(query, session=session)), key=_encoded
            )
        profiles = read("participants", {"pid": {"$in": pids}})
        if len(profiles) != len(pids) or {d.get("pid") for d in profiles} != set(pids):
            raise MergeError("A selected participant no longer exists. Select the records again.")
        links = read("participant_events", {"participant_id": {"$in": pids}})
        tests = read("tests", {"pid": {"$in": pids}})
        if any(not d.get("event_id") for d in links) or any(not d.get("eid") or not d.get("type") for d in tests):
            raise MergeError("An attendance or test record has no event/type. Repair it before merging.")
        eids = sorted({d["event_id"] for d in links} | {d["eid"] for d in tests})
        events = read("events", {"$or": [
            {"eid": {"$in": eids}}, {"participants": {"$in": pids}},
            {"participant_ids": {"$in": pids}},
        ]})
        if any(not d.get("eid") for d in events):
            raise MergeError("An event has no event ID. Repair it before merging.")
        return {"profiles": profiles, "links": links, "tests": tests, "events": events}

    def plan(self, state, target_pid, choices=None, *, require_choices=False):
        profiles = state["profiles"]
        if target_pid not in {d["pid"] for d in profiles}:
            raise MergeError("Choose one of the selected records to keep.")
        choices = choices or {}
        conflicts, rows = [], []

        def combine(documents, identity_field, excluded, scope, union_fields=()):
            documents = sorted(documents, key=lambda d: (d.get(identity_field) != target_pid, _encoded(d)))
            result = deepcopy(documents[0])
            for field in sorted(set().union(*(d.keys() for d in documents)) - set(excluded)):
                options = []
                for doc in documents:
                    value = doc.get(field)
                    if _blank(value):
                        continue
                    same = next((o for o in options if _encoded(o["value"]) == _encoded(value)), None)
                    if same is not None:
                        same["sources"].append(doc[identity_field])
                    else:
                        options.append({"value": deepcopy(value), "sources": [doc[identity_field]]})
                if not options:
                    continue
                value = options[0]["value"]
                key = None
                if field in union_fields and all(isinstance(o["value"], list) for o in options):
                    value = []
                    for option in options:
                        for item in option["value"]:
                            if item not in value:
                                value.append(item)
                elif len(options) > 1:
                    key = f"choice_{len(conflicts)}"
                    conflicts.append({"key": key, "scope": scope, "field": field, "options": options})
                    if key in choices:
                        selected = choices[key]
                        if type(selected) is not int or not 0 <= selected < len(options):
                            raise MergeError("A conflict choice is invalid. Review the merge again.")
                        value = options[selected]["value"]
                    elif require_choices:
                        raise MergeError("Choose a value for every conflicting field.")
                result[field] = deepcopy(value)
                rows.append({"scope": scope, "field": field, "value": value, "conflict": key})
            return result

        target = next(d for d in profiles if d["pid"] == target_pid)
        profile = combine(profiles, "pid", (
            "_id", "pid", "created_at", "updated_at", "_audit", "audit", "_merge_revision"
        ), "Profile", ("citizenships",))
        profile["pid"] = target_pid
        profile["_id"] = target["_id"]
        audit = []
        for doc in profiles:
            for entry in doc.get("_audit", doc.get("audit", [])) or []:
                original_entry = deepcopy(entry)
                original_entry.setdefault("merge_source_pid", doc["pid"])
                audit.append(original_entry)
        profile.pop("audit", None)
        profile["_audit"] = audit
        grouped_links, grouped_tests = defaultdict(list), defaultdict(list)
        for doc in state["links"]:
            grouped_links[doc["event_id"]].append(doc)
        for event in state["events"]:
            for field in ("participants", "participant_ids"):
                roster = event.get(field, [])
                if not isinstance(roster, list):
                    raise MergeError("An event participant roster is invalid. Repair it before merging.")
                if set(roster) & {d["pid"] for d in profiles}:
                    grouped_links.setdefault(event["eid"], [])
        links = []
        for eid, documents in sorted(grouped_links.items()):
            link = combine(documents, "participant_id", ("_id", "participant_id", "event_id"),
                           f"Event {eid}") if documents else {}
            link.update(event_id=eid, participant_id=target_pid)
            links.append(link)
        for doc in state["tests"]:
            grouped_tests[(doc["eid"], doc["type"])].append(doc)
        tests = []
        for (eid, attempt), documents in sorted(grouped_tests.items()):
            test = combine(documents, "pid", ("_id", "pid", "eid", "type"),
                           f"Test {eid} ({attempt})")
            test["pid"] = target_pid
            tests.append(test)
        if set(choices) - {c["key"] for c in conflicts}:
            raise MergeError("Unexpected conflict choices. Review the merge again.")
        event_lookup = {d["eid"]: d for d in state["events"]}
        attendance = []
        for link in links:
            eid = link["event_id"]
            sources = sorted({d["participant_id"] for d in state["links"] if d["event_id"] == eid}
                             | {p for field in ("participants", "participant_ids")
                                for p in event_lookup.get(eid, {}).get(field, [])
                                if p in {d["pid"] for d in profiles}})
            attendance.append({"eid": eid, "title": event_lookup.get(eid, {}).get("title", ""),
                               "sources": sources})
        return {"profile": profile, "links": links, "tests": tests, "conflicts": conflicts,
                "rows": rows, "attendance": attendance, "target_pid": target_pid,
                "profiles": sorted(profiles, key=lambda d: d["pid"])}

    def merge(self, pids, target_pid, expected_fingerprint, choices, actor):
        """Archive originals and change all references in a single majority transaction."""
        def transaction(session):
            state = self.snapshot(pids, session=session)
            if fingerprint(state) != expected_fingerprint:
                raise MergeError("The records changed since your preview. Review them again before merging.")
            plan = self.plan(state, target_pid, choices, require_choices=True)
            now = datetime.now(timezone.utc)
            source_pids = sorted(set(pids) - {target_pid})
            # Writing each parent also conflicts with imports/profile edits in other transactions.
            for doc in state["profiles"]:
                result = self.db.collection("participants").update_one(
                    {"_id": doc["_id"], "pid": doc["pid"]}, {"$inc": {"_merge_revision": 1}}, session=session
                )
                if result.matched_count != 1:
                    raise MergeError("A participant changed during the merge. Review again.")
            archive = {"target_pid": target_pid, "source_pids": source_pids, "actor": actor,
                       "merged_at": now, "originals": state, "choices": choices}
            archive_id = self.db.collection("participant_merges").insert_one(archive, session=session).inserted_id
            profile = plan["profile"]
            profile["updated_at"] = now
            profile["_merge_revision"] = next(d for d in state["profiles"] if d["pid"] == target_pid).get("_merge_revision", 0) + 1
            profile["_audit"].append({"ts": now, "actor": actor, "action": "merge",
                                      "source_pids": source_pids, "merge_id": str(archive_id)})
            self.db.collection("participants").replace_one({"_id": profile["_id"]}, profile, session=session)
            # Remove originals before insertion to respect the compound unique indexes.
            self.db.collection("participant_events").delete_many({"participant_id": {"$in": pids}}, session=session)
            if plan["links"]:
                self.db.collection("participant_events").insert_many(plan["links"], session=session)
            self.db.collection("tests").delete_many({"pid": {"$in": pids}}, session=session)
            if plan["tests"]:
                self.db.collection("tests").insert_many(plan["tests"], session=session)
            for event in state["events"]:
                updates = {}
                for field in ("participants", "participant_ids"):
                    if field in event and set(event[field]) & set(pids):
                        roster = [target_pid if pid in pids else pid for pid in event[field]]
                        updates[field] = list(dict.fromkeys(roster))
                if updates:
                    self.db.collection("events").update_one({"_id": event["_id"]}, {"$set": updates}, session=session)
            for pid in source_pids:
                self.db.collection("participant_aliases").insert_one(
                    {"_id": pid, "target_pid": target_pid, "merge_id": archive_id}, session=session
                )
            # Imports predating counters must never allocate a retired PID again.
            highest = max(int(pid[1:]) for pid in pids)
            self.db.collection("counters").update_one(
                {"_id": "participant_pid"}, {"$max": {"seq": highest}}, upsert=True, session=session
            )
            self.db.collection("participants").delete_many({"pid": {"$in": source_pids}}, session=session)
            return archive_id

        # No nontransactional fallback: standalone MongoDB must not perform a partial merge.
        with self.db.start_session() as session:
            archive_id = session.with_transaction(
                transaction, read_concern=ReadConcern("snapshot"), write_concern=WriteConcern("majority")
            )
        refresh_participant_cache()
        return archive_id
