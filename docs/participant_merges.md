# Merge duplicate participants

Open **Participants → Merge duplicates**. Suggested duplicates share a normalized name and the stored representing-country code. These suggestions require human review; no records are merged automatically.

1. Select two to ten records for the same person and choose the participant ID to keep. Search by name or ID to find other records; selections persist across searches in the same browser tab.
2. Preview the combined attendance. Choose a stored value for every conflicting profile, per-event, or test field. Missing fields are filled, and citizenship lists are combined. Zero grades/scores and `false` flags remain meaningful values.
3. Review the final values, confirm that the records belong to one person, and select **Merge participants**.

A shared event counts once. Travel/banking fields remain attached to their original event. Pre/post scores are combined per event and attempt, with explicit choices for differing scores. Legacy attendance stored only in event rosters is also retained. The merge reads raw records so partial legacy snapshots and extra fields are preserved.

## Storage and recovery

The surviving participant keeps its PID and MongoDB `_id`. Other profiles are removed from active `participants`, their attendance/test references and both current/legacy event rosters are moved to the surviving PID, and their IDs are retired. Participant profile/edit/event-detail GET URLs redirect to the survivor. New attendance, score, and roster writes through the application also resolve retired IDs.

Each `participant_merges` record stores the actor, timestamp, target/source PIDs, chosen conflict options, and full original participant, attendance, test, and affected event documents. `participant_aliases` stores retired-ID redirects; participant `_audit` contains the merge reference and original audit entries with source PID provenance. Recovery is a database maintenance operation using the archive, with review of any edits made after the merge; there is no automatic Undo action.

Merges require a MongoDB replica set or sharded cluster with transaction support, as does the existing upload workflow. The database user needs read/write access to `participant_merges`, `participant_aliases`, and the existing collections. Archives contain the same personal information as the original records and should use the same database access controls.

Every merge is a single snapshot/majority transaction. Preview tokens expire after 30 minutes and are bound to the authenticated session and a CSRF token. Changes to selected profiles, attendance, tests, or affected events invalidate a preview. Reference writers touch the participant parent within their transaction so a concurrent new attendance/score/roster cannot be stranded by a merge. Stale imports with no accepted profile changes still check/write the parent and abort if it was removed during review. The PID counter is advanced past retired IDs so imports do not allocate them again.

## Verification

Ordinary planner and route checks run with:

```sh
python -m pytest -q tests/test_participant_merge.py tests/test_upload_service.py tests/test_participant_event_repository.py
```

Real transaction, unique-index, rollback, redirect, and concurrent-writer checks are opt-in. Supply a disposable local replica set; these tests create/drop only randomly named test databases and never read application connection variables:

```sh
MERGE_TEST_MONGODB_URI='mongodb://127.0.0.1:27028/?directConnection=true&replicaSet=rs0' \
  python -m pytest -q tests/test_participant_merge.py
```
