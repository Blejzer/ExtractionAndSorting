# Temporary Word participant extraction

Open **Import → Word participant extraction** (`/imports/word`) after deploying
this feature. It is separate from the Excel importer and the participant merge
feature. It does not insert, update, delete, or merge MongoDB records, nor does
it attach participants to events.

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
4. Select a representing country when it is not stated. The bulk selector
   applies only to entries with neither a country label nor a CID. Countries
   are resolved against the existing `countries` collection; biographies,
   institutions, citizenship, place of birth, and filenames do not determine
   representing country. Historical country labels are normalized for lookup.
5. **Save edits and check again**, or download Excel/CSV with current edits and
   a fresh database check. Matching PIDs link to existing participant profiles.
6. **Clear this batch** removes the temporary extracted draft.

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
- XLSX cells are explicitly text; CSV formula prefixes are escaped. XLSX rejects
  fields over Excel's cell limit rather than silently truncating them; CSV
  preserves the full text.

Set `WORD_EXTRACTION_ENABLED=0` and restart the app to hide the Import link and
disable all tool endpoints after this one-time task. Existing Excel import and
statistics routes remain unchanged.

Tests use synthetic documents and a read-only database stub:

```bash
python -m pytest tests/test_word_extraction.py -q
```

Do not commit uploaded documents, extracted personal data, or temporary drafts.
