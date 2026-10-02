# Import validation recommendations

Every populated field should be checked. Optional fields should remain optional
unless a business rule requires them. Show errors next to the fields, allow an
invalid draft to be saved for correction, and revalidate the values being persisted
before starting any database writes.

## Implemented in this change

| Fields | Checks |
| --- | --- |
| Workbook event cost | Find `GRAND TOTAL` in COST Overview, normalize label whitespace and casing, require one amount to its right; never use an unlabelled B15. Reject missing cached formula results and ambiguous totals. Zero is valid. |
| Event ID, title, place | Non-empty text; maximum 500 characters. Duplicate event IDs remain blocked at upload. |
| Event start/end | Valid dates, start on or before end. |
| Event cost/type/participant IDs | Finite non-negative cost; supported event type; list of non-empty participant IDs. A missing cost remains allowed for legacy/custom previews. Workbook validation requires the grand total. |
| Participant name, country references | Trim strings; required name (maximum 255 characters); country reference lengths; citizenship list element types. |
| Gender, grade, authority | Validate against the model's accepted enum/boolean values. |
| Date of birth | Required for a new participant, valid date, cannot be in the future. Returning participants retain the existing exception for missing legacy DOB. |
| Email, phone | Validate supplied values; trim whitespace; lowercase email; normalize phone format. Blank optional contact details are allowed. Case-only email differences do not appear as profile changes. |
| Other profile text | String type, trim whitespace, maximum 5,000 characters. |
| Travel | Accepted transportation/document enums, required departure/return text, details required for Other. Validate optional document dates and require expiry after issue. Preserve the travel document number in the event snapshot. |
| Banking | String types and limits for supplied bank/account/BIC fields; supported account currency enum. |
| Separate event snapshots | Validate every snapshot before writes; require references to the event and a participant in the preview. Keep all edited travel and banking fields synchronized with the snapshot. |

The preview shows field errors. Upload validates the effective saved profile,
including the selected changes for returning participants; invalid unselected file
values may still be reviewed and ignored. Invalid numeric/boolean edits stay visible
instead of silently reverting to the previous value.

## Recommended next rules

1. **Country references:** Resolve names to existing country IDs and use country
   dropdowns for representing country, birth country and citizenships. Reject an
   unresolved reference rather than silently substituting a country. Length checks
   alone do not prove the referenced country exists.
2. **Banking:** Validate actual IBANs against country-specific lengths and MOD-97
   checksums. Normalize spaces/casing and validate BIC as 8 or 11 characters. Confirm
   whether the account field also accepts non-IBAN accounts before enforcing these
   checks. Require bank name, account, currency and BIC when reimbursement is needed.
3. **Travel dates:** Warn if a document expires before the event ends or was issued
   in the future. Configure any required validity buffer by destination. Document
   numbers need country/document-specific rules, not a universal numeric pattern.
4. **Field controls:** Use select inputs for enums and booleans, date inputs for
   dates, and an email input for email. Keep server validation authoritative and
   allow saving incomplete drafts. Define shorter limits for short profile fields
   if the business needs them.
5. **Raw import values:** Retain invalid source dates, phone numbers, enums and XML
   records for correction. Some older parsing paths currently coerce invalid
   values to blanks or skip invalid XML records before preview validation sees them.
   Surface unsupported fields rather than silently dropping them.
6. **Workbook handling:** Accept `.xlsx` only unless a real `.xls` reader is added
   (openpyxl cannot read `.xls`). Validate file content as well as extension, report
   corrupt/password-protected files cleanly, set file/decompressed size limits and
   use unique upload filenames.
7. **Money:** Add an explicit event currency and store amounts as Decimal128 or
   integer minor units to avoid floating-point rounding in future calculations.

## Verification notes

Regression checks use a workbook with B15 = 440.32 and a moved, merged
`GRAND   TOTAL` label whose amount is 64711.19. The reported original document was
not attached, so its exact layout and cached formulas still need verification.
The dev suite also has 15 existing failures: legacy fixtures/APIs, country cache,
custom-XML date serialization and participant event details. These reproduce on
the unchanged dev source after initializing its missing participant-cache globals;
the focused tests for this change pass.
