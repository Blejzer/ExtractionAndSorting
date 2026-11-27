# Participant pre/post tests

Open an event and select **Pre/post tests**. Upload the `.xlsx` workbook, review the
participant links and select **Save scores**. The event roster must already exist.
Each unrecognized name needs an explicit roster selection or **Skip entry**. A
unique full name matches regardless of capitalization or extra whitespace;
accent-only variants are suggestions for manual confirmation. `MF` columns are
excluded. No participants or attendance links are created by this import.

Supported layouts include participant columns with consecutive `Pitanje 1…N` /
`Question 1…N` rows, and Microsoft Forms exports with one participant per row,
`SELECT YOUR NAME:`, total points and individual `Points - …` columns. Sheet names
may contain spaces (`Pre Test`, `Post Test`). The `Prost Test` spelling used by
PFE26M2 is recognized as a post-test sheet. Totals, answer-key columns and summary
rows are excluded.

Where a percentage is already supplied, it is used with its reported precision.
Excel fractions formatted as percentages (e.g. `0.8333`) become percent values
(`83.33`). Percent text with a decimal comma is also accepted. Missing percentages
are calculated from question points / number of questions, or from total points /
maximum points. Forms question columns use a maximum of one point each unless an
explicit maximum is given; weighted scores without a known maximum are rejected.

The supported comparison and partial-credit formulas are calculated directly from
their plain answer/key references when cached totals are unavailable. Arbitrary
Excel formulas are never executed. The supplied partial-credit formula awards
credit for correct selections and subtracts wrong selections, floored at zero.
Partial credit is the default when alternatives coexist. Select **all-or-nothing
(bez parcijale)** during upload to use the alternative sheets. A preview lists the
actual selected worksheet names; the scoring versions are never combined.

Scores are percentages. Blank answers in an
otherwise started attempt count as incorrect. An entirely blank attempt is
unknown, and does not become zero or remove a previously stored score. A zero
score from an actual attempt is retained. The displayed change is **post minus
pre in percentage points**, calculated only when both are present.

## AI interpretation for unfamiliar layouts

Select **Use AI interpretation for unfamiliar layouts** before previewing an
unrecognized workbook. Standard supported formats still use the local parser.
The AI option is off by default: when selected and needed, worksheet names,
cell addresses, values, formats and formula text are sent to OpenAI. The event's
database roster, credentials and other application records are not sent.

Configure `OPENAI_API_KEY` or the existing `OPENAIAPI` server variable.
`PREPOST_AI_MODEL` overrides the existing `MODEL` variable; the fallback model is
`gpt-4.1-mini`. The selected model must support Chat Completions structured JSON
output. Requests have a bounded workbook snapshot and timeout, with no automatic
retry loop. This uses the existing `requests` dependency.

The model proposes a structured plan of **source cell references**, not scores
or participant IDs. Local code validates the references, reads actual workbook
values and verifies units/maxima. It rejects extra/invented score fields,
unverifiable scales, invalid or duplicate cells and percentages outside 0–100.
Only an explicit maximum-points cell or sequential question range can justify a
points denominator. Arbitrary formulas and workbook instructions are never
executed. Formula results must be cached by Excel, except for a supported simple
`score / literal maximum` division; otherwise recalculate/save the file in Excel.

The preview shows AI layout notes and the score's worksheet/cell. Coded or missing
names require manual roster selection; the agent cannot borrow names from another
sheet or pair columns by position. The normal expiring preview, stale-data check,
explicit skips/replacements and atomic save still apply. Provider/model failures
leave the database unchanged and return a safe message.

The interpretation summarizes available pre/post counts and means, and computes
paired change only for linked participants. Unknown scores stay missing; an
average difference between two independent groups is not presented as an
individual improvement. The saved summary updates after manual links are saved.
These are descriptive comparisons of test results, not causal training claims.

Each attempt is stored in the existing configured database's `tests` collection
using its established `eid`, `pid`, `type` (`pre` or `post`) and `score` fields.
Only percentage scores and source metadata are saved: source name/sheet/row or
column, file name/SHA-256, scoring method and save actor/time. Individual answers,
question points and raw counts are not stored or displayed. Replacing an older
imported attempt clears those legacy details for that attempt.
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
python -m pytest -q tests/test_prepost_ai.py tests/test_prepost_import.py tests/test_prepost_layouts.py tests/test_prepost_routes.py tests/test_event_evaluation_service.py tests/test_event_evaluations.py tests/test_event_detail_route.py tests/test_training_test_model.py
```
