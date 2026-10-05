"""Coordinate attendance/score/roster writes with participant merges."""

from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern


def write_with_participants(database, pids, operation, *, session=None):
    """Touch active parents and write references in the same transaction.

    A merge touches those same parents. Concurrent writes then either cause a
    new merge review or retry against the surviving PID, including new links
    that would otherwise be invisible to a transaction's snapshot.
    """
    def write(active_session):
        canonical = []
        for original in pids:
            pid, seen = original, set()
            while pid not in seen:
                seen.add(pid)
                result = database.collection("participants").update_one(
                    {"pid": pid}, {"$inc": {"_merge_revision": 1}}, session=active_session
                )
                if result.matched_count:
                    canonical.append(pid)
                    break
                alias = database.collection("participant_aliases").find_one({"_id": pid}, session=active_session)
                if not alias:
                    raise ValueError(f"Participant {original} no longer exists. Review the current participant ID.")
                pid = alias["target_pid"]
            else:
                raise ValueError("The participant redirect history needs repair.")
        return operation(canonical, active_session)

    if session is not None:
        return write(session)
    with database.start_session() as own_session:
        return own_session.with_transaction(
            write, read_concern=ReadConcern("snapshot"), write_concern=WriteConcern("majority")
        )
