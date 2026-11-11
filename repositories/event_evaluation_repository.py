"""One current evaluation summary per event, using the configured application database."""

from datetime import datetime, timezone

from pymongo.errors import DuplicateKeyError

from config.database import mongodb


class EvaluationConflict(ValueError):
    pass


class EventEvaluationRepository:
    def __init__(self):
        self.collection = mongodb.collection("event_evaluations")

    def get(self, event_id):
        return self.collection.find_one({"_id": event_id})

    def save(self, event_id, raw_text, report, expected_version, actor):
        if type(expected_version) is not int or expected_version < 0:
            raise ValueError("Invalid evaluation version.")
        if report.get("errors"):
            raise ValueError("Correct the evaluation errors before saving.")
        now = datetime.now(timezone.utc)
        fields = dict(event_id=event_id, raw_text=raw_text, report=report, updated_at=now, updated_by=actor)
        if expected_version == 0:
            try:
                self.collection.insert_one(dict(
                    _id=event_id, **fields, version=1, created_at=now, created_by=actor))
            except DuplicateKeyError as exc:
                raise EvaluationConflict("An evaluation was saved while you were reviewing. Reload before replacing it.") from exc
            return 1
        result = self.collection.update_one(
            {"_id": event_id, "version": expected_version}, {"$set": fields, "$inc": {"version": 1}})
        if result.matched_count != 1:
            raise EvaluationConflict("The saved evaluation changed while you were reviewing. Reload before replacing it.")
        return expected_version + 1
