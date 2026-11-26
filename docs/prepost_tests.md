# Participant pre/post tests

Open an event and select **Pre/post tests**. Upload the `.xlsx` workbook, review the
participant links and select **Save scores**. The event roster must already exist.
Each unrecognized name needs an explicit roster selection or **Skip entry**. A
unique full name matches regardless of capitalization or extra whitespace;
accent-only variants are suggestions for manual confirmation. `MF` columns are
excluded. No participants or attendance links are created by this import.

The PFE workbook format has `PreTest` and/or `PostTest` sheets, participant names
across columns and consecutive `Pitanje 1…N` / `Question 1…N` rows below them.
The importer reads 0/1 question points, or the limited
`IF(answer_cell=key_cell,1,0)` formulas used by these workbooks. Formula references
may point to the answer key on `PreTest`. It compares the plain answers itself;
it does not execute arbitrary Excel formulas or depend on cached totals. The
second block of raw answers is used through those explicit cell references.

Scores are percentages, `correct / question_count * 100`. Blank answers in an
otherwise started attempt count as incorrect. An entirely blank attempt is
unknown, and does not become zero or remove a previously stored score. A zero
score from an actual attempt is retained. The displayed change is **post minus
pre in percentage points**, calculated only when both are present.

Each attempt is stored in the existing configured database's `tests` collection
using its established `eid`, `pid`, `type` (`pre` or `post`) and `score` fields.
Additional fields retain the answers, question points/count, number answered,
source name/sheet/column, file name/SHA-256, scoring method and save actor/time.
This remains compatible with `TrainingTestRepository` and participant merging.
Event evaluation summaries remain separate and are not recalculated or replaced.
The original workbook is not retained on the server.

Saving differing scores requires **Replace differing saved scores**. Unselected
participants and missing attempts are preserved. An expiring, session-bound
preview and a fingerprint of the event roster/existing scores reject stale saves.
All selected scores are saved in one MongoDB transaction with a unique attempt
index; Atlas supports this, and there is no nontransactional fallback. Upload,
save and CSV export require authentication; forms also require a CSRF token.

The saved table links to participant pages. Its CSV contains event ID, participant
ID, name, represented country code, pre/post percentages and change in percentage
points. Missing scores are empty fields. Upload limit: 5 MB compressed, 30 MB
expanded, 1000 rows and 300 columns per recognized sheet.

Verification (no Atlas connection required):

```sh
python -m pytest -q tests/test_prepost_import.py tests/test_prepost_routes.py tests/test_event_evaluation_service.py tests/test_event_evaluations.py tests/test_event_detail_route.py tests/test_training_test_model.py
```
