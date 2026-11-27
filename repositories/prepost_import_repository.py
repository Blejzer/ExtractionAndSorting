"""Atomic reviewed imports into the existing participant-linked tests collection."""

from datetime import datetime, timezone
import hashlib
import math

from bson.json_util import dumps
from pymongo import ASCENDING
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from config.database import mongodb
from services.prepost_import_service import TestImportError


def fingerprint(value):
    return hashlib.sha256(dumps(value, sort_keys=True).encode()).hexdigest()


class PrePostImportRepository:
    def __init__(self):
        self.db = mongodb

    def snapshot(self, eid, *, session=None):
        event = self.db.collection("events").find_one({"eid": eid}, session=session)
        if not event:
            return None
        links = self.db.collection("participant_events").find({"event_id": eid}, session=session)
        pids = sorted({link["participant_id"] for link in links if link.get("participant_id")}
                      | set(event.get("participants", [])) | set(event.get("participant_ids", [])))
        people = self.db.collection("participants").find(
            {"pid": {"$in": pids}}, {"pid": 1, "name": 1, "representing_country": 1}, session=session)
        roster = sorted([dict(pid=p["pid"], name=p.get("name") or p["pid"],
                              country=p.get("representing_country") or "") for p in people], key=lambda p: p["pid"])
        tests = list(self.db.collection("tests").find({"eid": eid}, session=session))
        tests.sort(key=lambda t: (t["pid"], t["type"], str(t.get("_id", ""))))
        keys = [(test["pid"], test["type"]) for test in tests]
        if len(keys) != len(set(keys)):
            raise TestImportError("This event has duplicate stored test attempts. Resolve them before importing.")
        return dict(eid=eid, title=event.get("title", eid), roster=roster, tests=tests)

    def save(self, eid, rows, selections, expected_fingerprint, source, actor, allow_replace=False):
        # The same keys are used by TrainingTestRepository, statistics and participant merging.
        self.db.collection("tests").create_index(
            [("eid", ASCENDING), ("pid", ASCENDING), ("type", ASCENDING)], unique=True)

        def transaction(session):
            state = self.snapshot(eid, session=session)
            if not state or fingerprint(state) != expected_fingerprint:
                raise TestImportError("The event roster or saved scores changed. Upload and review the workbook again.")
            plan = import_plan(state, rows, selections, allow_replace)
            now = datetime.now(timezone.utc)
            # Reuse the existing concurrency revision, excluded from merge field conflicts.
            # Conflicts with a merge/deletion so an import cannot recreate a retired PID.
            for pid in sorted({test["pid"] for test in plan}):
                result = self.db.collection("participants").update_one(
                    {"pid": pid}, {"$inc": {"_merge_revision": 1}}, session=session)
                if result.matched_count != 1:
                    raise TestImportError("A selected participant changed. Upload and review again.")
            for test in plan:
                self.db.collection("tests").update_one(
                    {"eid": eid, "pid": test["pid"], "type": test["type"]},
                    {"$set": {**test, "source_file": source["filename"], "source_sha256": source["sha256"],
                              "updated_at": now, "updated_by": actor},
                     "$unset": {field: "" for field in ("answers", "points", "correct", "question_count", "answered")}},
                    upsert=True, session=session)
            return len(plan)

        # Atlas supports transactions. Never fall back to a partial, nontransactional import.
        with self.db.start_session() as session:
            return session.with_transaction(transaction, read_concern=ReadConcern("snapshot"),
                                            write_concern=WriteConcern("majority"))


def import_plan(state, rows, selections, allow_replace=False):
    pids = {p["pid"] for p in state["roster"]}
    existing = {(t["pid"], t["type"]): t for t in state["tests"]}
    seen, plan = set(), []
    if len(rows) != len(selections):
        raise TestImportError("Review each imported participant before saving.")
    for row, pid in zip(rows, selections):
        if not pid:
            continue
        if pid not in pids:
            raise TestImportError("Choose participants from this event's roster only.")
        for attempt in ("pre", "post"):
            score = row.get(attempt)
            if score is None:
                continue
            if (pid, attempt) in seen:
                raise TestImportError("Two workbook entries select the same participant and test attempt. Correct the selections or skip one.")
            seen.add((pid, attempt))
            old = existing.get((pid, attempt))
            if old and abs(old["score"] - score["score"]) > 0.000001 and not allow_replace:
                raise TestImportError("Some saved scores differ. Review them and tick Replace differing scores to continue.")
            if type(score.get("score")) not in (int, float) or not math.isfinite(score["score"]) or not 0 <= score["score"] <= 100:
                raise TestImportError("Scores must be percentages between 0 and 100.")
            percentage = {k: score[k] for k in ("score", "sheet", "column", "row", "method") if k in score}
            plan.append(dict(eid=state["eid"], pid=pid, type=attempt,
                             **percentage, source_name=row["name"], score_unit="percent"))
    if not plan:
        raise TestImportError("Select at least one participant with a test score to save.")
    return plan
