"""Validate optional travel-document dates without changing their values."""

from utils.dates import coerce_datetime

DOCUMENT_DATE_FIELDS = ("travel_doc_issue_date", "travel_doc_expiry_date")


def document_date_errors(record) -> dict[str, str]:
    parsed = {}
    errors = {}
    for field, label in zip(DOCUMENT_DATE_FIELDS, ("issue", "expiry")):
        raw = record.get(field)
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            continue
        value = coerce_datetime(raw)
        if value is None:
            errors[field] = f"Document {label} date '{raw}' is not a valid date. Use YYYY-MM-DD."
        else:
            parsed[field] = value.date()
    issue = parsed.get("travel_doc_issue_date")
    expiry = parsed.get("travel_doc_expiry_date")
    if issue and expiry and expiry <= issue:
        message = f"Document expiry date {expiry.isoformat()} must be after issue date {issue.isoformat()}. Correct these dates in the preview."
        errors.update({field: message for field in DOCUMENT_DATE_FIELDS})
    return errors
