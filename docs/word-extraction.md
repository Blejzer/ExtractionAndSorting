# Temporary Word participant extraction

Open **Import → Word participant extraction** (`/imports/word`) after deploying
this feature. Extraction and export are separate from database import: reviewing
Word files does not change participant or event records. Downloaded CSV
tables can then be imported for a selected event using the workflow below.
The participant merge feature remains separate.

1. Upload one or more text-based `.docx` documents. Old `.doc`, scans, and
   password-protected documents must be converted first.
2. Review the extracted table. A document may contain several people: supported
   layouts include registration tables, roster tables, tab-separated lists,
   numbered biographies, and labeled paragraphs. A supplemental roster within
   the same file combines with an entry only when name tokens and a known DOB
   agree, without conflicting countries. Entries across files remain separate.
3. Correct values in the table. **Show all extracted columns** exposes the
   additional profile, biography, service, vetting, and travel/document fields.
   All columns are included in downloads. Source locations, original dates,
   conflicting alternatives, and unclassified text remain available for review.
4. Select a representing country for each entry when it is not stated. Countries
   are resolved against the existing `countries` collection; biographies,
   institutions, citizenship, place of birth, and filenames do not determine
   representing country. Historical country labels are normalized for lookup.
5. **Save edits and check again**, or download CSV with current edits and
   a fresh database check. Matching PIDs link to existing participant profiles.
6. **Clear this batch** removes the temporary extracted draft.

## Import an extracted Excel or CSV table

Open **Import extracted participants for an event** (`/imports/word/import`).
Both older exports and current exports are supported, including edits made in
Excel. CSV must be UTF-8. Workbook formulas must be pasted as values first.

1. Upload the extracted `.xlsx` or `.csv` and choose an existing event, or
   **Create a new event**. For a new event, enter its ID or use the file's single
   explicit event reference, then complete title, dates, and place in the preview.
2. Review the familiar Master Tracker import preview. Existing people are marked
   **Returning participant: PID**; changed profile fields are yellow and have
   **Use file value** controls. Nonempty stored fields are retained unless selected.
   Empty stored fields default to supplied values, and their checkboxes can be
   unchecked. CSV match statuses/PIDs are ignored; matches are checked against
   the database again. Accent and name-order variants can resolve to the same PID.
3. Resolve any ambiguous candidates by selecting the existing PID and saving
   the preview. A changed match must be reviewed before importing. Confirming
   a genuinely new person is allowed when no existing identity matches; an exact
   existing identity cannot be imported as a new duplicate.
4. Select the rows to attach. A row stating a different event starts unchecked.
   Repeated name/DOB/country identities share one PID when imported; omitted fields
   on a later copy do not erase an earlier copy's data. Select only the intended
   copies when their values differ.
5. Correct highlighted errors. New people need name, representing-country CID,
   DOB, and gender. Unstated place/country of birth remain empty and reload safely.
   Country references use catalog CIDs; they are not inferred from biographies or
   citizenship. Travel/banking details can be partial, with errors checked only
   for supplied fields. Grade uses the same Normal default as the Master Tracker.
6. Click **Import selected participants** to commit. New profiles receive new
   PIDs, returning people retain theirs, and attendance is saved in both the
   event roster and participant-event links. Existing event metadata and attendance
   remain intact. Omitted travel/banking fields and other events' snapshots remain
   intact. Complete original row evidence, including additional fields such as
   service numbers, arrival/departure, and vetting, is stored in
   `participant_events.word_import_sources`.

The import uses a MongoDB transaction for profile, PID counter, snapshot, and
event writes. A failed transaction rolls back all of them. Successful previews
become small receipts and cannot be committed twice. Preview saves/commits are
serialized across workers sharing the draft directory, including double submits.
Database unavailability blocks import rather than classifying everyone as new.

## Matching

The tool reads projected raw documents from `participants` and `countries`
through the application's existing MongoDB connection. It avoids full model
hydration of legacy records and does not require another connection variable.

| Result | Meaning |
| --- | --- |
| Exact match | One stored participant has the same normalized name tokens, country CID, and DOB. |
| Multiple exact matches | More than one stored PID satisfies those checks. Review existing duplicates. |
| Possible match | Name similarity, matching email, or matching name suggests a candidate, but identity is incomplete or conflicting. |
| No candidate found | The search found no candidate. This does not establish that the person is new. |
| Review required | Name/country needs correction or selection before a useful check. |
| Not checked | The database is unavailable. Extraction and export remain usable. |

Accents, case, and surname-first order are normalized for name comparison.
Approximate spellings never produce an exact match. Missing or conflicting DOBs
and countries remain visible, and matching never makes an automatic database
change. Passport, police/service, and case numbers are independent columns;
they are never used as participant PIDs.

Dotted dates are day/month/year. Slash dates use unambiguous evidence from the
document; mixed or absent evidence leaves ambiguous DOBs blank with the source
value retained. The upload form offers explicit MM/DD/YYYY or DD/MM/YYYY
overrides. Invalid dates remain review items. Other date fields retain source
text. Gender is normalized only when explicitly stated.

## Limits and temporary storage

- Authenticated sessions only; state-changing forms require a CSRF token.
- Up to 100 files, 10 MB per file, 50 MB per request, 12 MB document XML, and
  1,000 extracted entries per batch. Oversized batches are rejected.
- Original documents are processed in memory and are not saved.
- Extracted drafts live in `UPLOADS_DIR/word-extraction` (or the optional
  `WORD_EXTRACTION_DIR`), outside MongoDB and session cookies. Directory/file
  permissions are restricted; a draft belongs to its uploading browser session.
- A draft expires 30 minutes after its last save. Expired JSON files are removed
  on the next request to draft storage; this is not a background cleanup job.
- Review/download responses use `Cache-Control: no-store`.
- CSV formula prefixes are escaped, and the export preserves the full text.

Set `WORD_EXTRACTION_ENABLED=0` and restart the app to hide the Import link and
disable all extraction and extracted-import endpoints after this one-time task.
Existing Master Tracker upload, statistics, and participant merge flows remain
available.

Tests use synthetic documents and a read-only database stub:

```bash
python -m pytest tests/test_word_extraction.py tests/test_word_import.py -q
```

Optional transaction checks create/drop uniquely named test databases on a
disposable replica set (`WORD_IMPORT_TEST_MONGO_URI`), leaving other databases
untouched:

```bash
python -m pytest tests/test_word_import_mongodb.py -q
```

Do not commit uploaded documents, extracted personal data, or temporary drafts.
