"""Read participant forms/lists from DOCX without executing document content."""

from collections import defaultdict
from copy import deepcopy
from datetime import date, datetime
from io import BytesIO
import re
from xml.etree import ElementTree as ET
from zipfile import BadZipFile, ZipFile
from zlib import error as ZlibError

from domain.reporting import country_code, normalize_text
from utils.biography_employment import employment_from_bio, employment_field_errors
from utils.professional_experience import extract_professional_experience, extract_service_start


FIELDS = {
    "name": "Name", "country_label": "Country as stated", "representing_country": "Country CID",
    "dob": "Date of birth", "dob_raw": "Original DOB", "gender": "Gender", "grade": "Grade",
    "pob": "Place of birth", "birth_country": "Birth country", "citizenships": "Citizenships",
    "organization": "Organization", "position": "Position", "unit": "Unit", "rank": "Rank",
    "bio_short": "Biography", "email": "Email", "phone": "Phone",
    "diet_restrictions": "Diet restrictions", "intl_authority": "International authority",
    "event_reference": "Event reference", "police_joined": "Service Start",
    "service_number": "Police/service number", "case_id": "Case ID", "disposition": "Disposition",
    "certificate_date": "Certificate date", "certificate_expires": "Certificate expiry",
    "travel_doc_type": "Travel document type", "travel_doc_number": "Travel document number",
    "travel_doc_issue_date": "Document issue date", "travel_doc_expiry_date": "Document expiry",
    "travel_doc_issued_by": "Document issued by", "travel_doc_issue_place": "Document place of issue", "visa": "Visa",
    "transportation": "Transportation", "transport_other": "Transportation (other)", "traveling_from": "Traveling from", "returning_to": "Returning to",
    "arrival": "Arrival", "departure": "Departure", "travel_notes": "Shared travel notes",
    "bank_name": "Bank name", "iban": "IBAN", "iban_type": "IBAN currency", "swift": "SWIFT",
    "vetting": "Vetting", "notes": "Notes", "unmapped_text": "Other source text",
}

_ALIASES = {
    "name": (r"name\s*(?:&|and)\s*last\s*name", r"full\s*name", r"name", r"ime\s*i\s*prezime"),
    "country_label": (r"representing\s*country", r"country", r"dr[zž]ava"),
    "dob": (r"date\s*of\s*birth(?:\s*\(dob\))?", r"d\.?\s*o\.?\s*b\.?", r"birth\s*date", r"datum\s*ro[dđ]enja"),
    "pob": (r"place\s*of\s*birth", r"p\.?\s*o\.?\s*b\.?"),
    "birth_country": (r"country\s*of\s*birth", r"birth\s*country"),
    "citizenships": (r"citizenships?", r"nationality"),
    "position": (r"(?:current\s+)?position(?:\s*[/&]\s*(?:job\s*)?title)?", r"job\s*title", r"funkcija"),
    "organization": (r"organi[sz]ation(?:\s*[/&]\s*institution)?", r"institution(?:\s*[/&]\s*organi[sz]ation)?",
                     r"agency", r"employer"),
    "unit": (r"unit\s*name", r"unit(?:\s*[/&]\s*department)?", r"department(?:\s*[/&]\s*unit)?",
             r"division", r"section", r"sector"), "rank": (r"rank", r"[cč]in"),
    "bio_short": (r"short\s*professional\s*biography", r"short\s*bio(?:graphy)?", r"biography", r"bio"),
    "gender": (r"gender", r"sex", r"pol"), "grade": (r"grade",), "email": (r"e[ -]?mail(?:\s*address)?",),
    "phone": (r"phone(?:\s*number)?", r"telephone", r"tel\.?", r"mobile(?:\s*number)?"),
    "diet_restrictions": (r"diet(?:ary)?\s*restrictions?",), "intl_authority": (r"international\s*authority", r"authority"),
    "travel_doc_number": (r"passport\s*(?:number|no\.?)", r"travel(?:ing)?\s*document\s*number", r"id\s*card\s*(?:number|no\.?)"),
    "travel_doc_type": (r"travel(?:ing)?\s*document\s*type",),
    "travel_doc_issue_date": (r"travel(?:ing)?\s*document\s*(?:issue|issuance)\s*date", r"passport\s*issue\s*date",
                              r"issued\s*on", r"date\s*of\s*issue"),
    "travel_doc_expiry_date": (r"travel(?:ing)?\s*document\s*expiry\s*date", r"passport\s*expiry\s*date", r"expires\s*on"),
    "travel_doc_issued_by": (r"travel(?:ing)?\s*document\s*issued\s*by", r"passport\s*issued\s*by", r"issued\s*by"),
    "travel_doc_issue_place": (r"place\s*of\s*issue",),
    "visa": (r"visa",), "transportation": (r"transportation", r"transport"),
    "transport_other": (r"transportation\s*\(other\)", r"transportation\s*(?:type\s*)?other",),
    "traveling_from": (r"travel(?:l)?ing\s*from",), "returning_to": (r"returning\s*to",),
    "arrival": (r"arrival",), "departure": (r"departure",), "vetting": (r"vetting",), "notes": (r"notes?",),
    "bank_name": (r"bank\s*name",), "iban_type": (r"iban\s*type",), "iban": (r"iban",), "swift": (r"swift",),
    # Keep the export field key compatible with earlier CSVs/drafts.
    "police_joined": (r"(?:police\s+|prosecution\s+|professional\s+)?service\s*start",
                      r"date\s*(?:he|she)\s*joined\s*(?:police|prosecution)",
                      r"date\s*joined\s*(?:police|prosecution|service)",),
    "case_id": (r"case\s*id",), "disposition": (r"disp(?:osition)?",),
    "certificate_date": (r"cert(?:ificate)?\s*date",), "certificate_expires": (r"expires", r"certificate\s*expiry"),
}
_NAME_PASSPORT_NOTE = r"\(\s*as\s+in\s+(?:the\s+)?passport\s*\)"


def _label_pattern(pattern, field):
    # Older forms also join labels to values with hyphens or typographic dashes.
    separator = r"(?:\s*[:\-–—]\s*|\s+|$)"
    if field == "name":
        # Word may join the label, instruction and filled value without spaces.
        separator = r"(?:\s*" + _NAME_PASSPORT_NOTE + r"\s*[:\-–—]?\s*|\s*[:\-–—]\s*|\s+|$)"
    return re.compile(r"^\s*(?:" + pattern + r")" + separator + r"(.*)$", re.I | re.S)


_LABELS = sorted(((_label_pattern(pattern, field), field)
                  for field, patterns in _ALIASES.items() for pattern in patterns), key=lambda item: -len(item[0].pattern))
_EXTRA_COUNTRIES = {
    "republic of kosovo": "XK", "republic of kosova": "XK",
    "republik of kosovo": "XK", "republik of kosova": "XK",
    "makedonija": "MK", "republic of macedonia": "MK",
    "r.macedonia": "MK", "r. macedonia": "MK",
    "republic of serbia": "RS",
    "bosnia and hercegovina": "BA",
    "bosnia and herzegovina – republic of srpska entity": "BA",
    "slovenia": "SI", "slovenija": "SI", "si": "SI", "svn": "SI",
}
_NS = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
MAX_FILE_BYTES = 10 * 1024 * 1024
MAX_XML_BYTES = 12 * 1024 * 1024
MAX_RECORDS = 1000


class WordExtractionError(ValueError):
    pass


def _clean_name_value(value):
    # Remove only the known form instruction, preserving real name punctuation.
    return re.sub(r"^\s*(?:(?:&|and)\s*last\s*name\s*)?" + _NAME_PASSPORT_NOTE + r"\s*:?\s*",
                  "", str(value or ""), flags=re.I | re.S).strip()


def label_value(text):
    for pattern, field in _LABELS:
        match = pattern.match(text)
        if match:
            value = match[1].strip()
            return field, _clean_name_value(value) if field == "name" else value
    return None, ""


def word_country(value):
    key = normalize_text(value).strip(" :")
    country, separator, region = key.partition(",")
    if separator and region.strip() in {"europe & eurasia", "europe and eurasia", "europe", "eurasia"}:
        key = country.strip()
    return _EXTRA_COUNTRIES.get(key) or country_code(key)


def name_key(value):
    return " ".join(sorted(re.findall(r"[^\W\d_]+", normalize_text(value), re.UNICODE)))


def _text(element):
    parts = []
    def visit(node):
        # Deleted revisions and field instructions are not participant text.
        if node.tag in (_NS + "del", _NS + "instrText"):
            return
        if node.tag == _NS + "t":
            parts.append(node.text or "")
        elif node.tag == _NS + "tab":
            parts.append("\t")
        elif node.tag in (_NS + "br", _NS + "cr"):
            parts.append("\n")
        else:
            for child in node:
                visit(child)
    visit(element)
    return "".join(parts).replace("\xa0", " ").strip()


def read_blocks(content):
    if len(content) > MAX_FILE_BYTES:
        raise WordExtractionError("The Word file exceeds the 10 MB limit.")
    try:
        with ZipFile(BytesIO(content)) as archive:
            info = archive.getinfo("word/document.xml")
            if info.file_size > MAX_XML_BYTES or info.flag_bits & 1:
                raise WordExtractionError("The document is encrypted or its text exceeds the extraction limit.")
            with archive.open(info) as stream:
                xml = stream.read(MAX_XML_BYTES + 1)
            if len(xml) > MAX_XML_BYTES:
                raise WordExtractionError("The document text exceeds the extraction limit.")
        if re.search(br"<!\s*(?:DOCTYPE|ENTITY)\b", xml.replace(b"\x00", b""), re.I):
            raise WordExtractionError("Documents containing XML entity declarations are unsupported.")
        root = ET.fromstring(xml)
    except (BadZipFile, KeyError, ET.ParseError, RuntimeError, NotImplementedError, ZlibError, EOFError) as exc:
        raise WordExtractionError("This is not a readable .docx file. Re-save it as Word .docx.") from exc
    body = root.find(_NS + "body")
    if body is None:
        raise WordExtractionError("The Word document has no body text.")
    blocks = []
    def walk(parent):
        for node in parent:
            if node.tag == _NS + "p":
                text = _text(node)
                if text:
                    blocks.append({"kind": "paragraph", "text": text, "ref": f"Paragraph {len(blocks) + 1}"})
            elif node.tag == _NS + "tbl":
                rows = [["\n".join(_text(p) for p in cell.iter(_NS + "p") if _text(p)).strip()
                         for cell in row.findall(_NS + "tc")] for row in node.findall(_NS + "tr")]
                blocks.append({"kind": "table", "rows": rows, "ref": f"Table {sum(b['kind'] == 'table' for b in blocks) + 1}"})
            else:
                walk(node)
    walk(body)
    if len(blocks) > 10000:
        raise WordExtractionError("The document contains too many text blocks.")
    return blocks


def _slash_order(blocks):
    text = "\n".join(b.get("text", "") if b["kind"] == "paragraph" else "\n".join(" ".join(row) for row in b["rows"]) for b in blocks)
    orders = set()
    for first, second, _ in re.findall(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", text):
        first, second = int(first), int(second)
        if first > 12 and 1 <= second <= 12:
            orders.add("dmy")
        elif second > 12 and 1 <= first <= 12:
            orders.add("mdy")
    return next(iter(orders)) if len(orders) == 1 else "auto"


def parse_date(value, slash_order="auto"):
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value or "").strip().strip("() ").rstrip(".")
    if not text:
        return ""
    try:
        match = re.fullmatch(r"(\d{4})\s*([/-])\s*(\d{1,2})\s*\2\s*(\d{1,2})", text)
        if match:
            year, _, month, day = match.groups()
            return date(int(year), int(month), int(day)).isoformat()
        if re.match(r"^\d{4}-\d{2}-\d{2}(?:$|[T ])", text):
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
        match = re.fullmatch(r"(\d{1,2})\s*([./])\s*(\d{1,2})\s*\2\s*(\d{4})", text)
        if match:
            first, separator, second, year = match.groups()
            first, second, year = int(first), int(second), int(year)
            if separator == ".":
                day, month = first, second
            elif first > 12 or slash_order == "dmy":
                day, month = first, second
            elif second > 12 or slash_order == "mdy" or first == second:
                month, day = first, second
            else:
                return ""
            return datetime(year, month, day).date().isoformat()
        if re.search(r"\b(?:19|20)\d{2}\b", text) and re.search(r"[a-z]", text, re.I):
            without_year = re.sub(r"\b(?:19|20)\d{2}\b", "", text)
            if not re.search(r"\b\d{1,2}(?:st|nd|rd|th)?\b", without_year, re.I):
                return ""  # A month/year alone must not acquire today's day.
            from dateutil.parser import parse
            return parse(text, fuzzy=False).date().isoformat()
    except (ValueError, OverflowError):
        pass
    return ""


def _record(filename, context, ref):
    return {"fields": {key: "" for key in FIELDS} | deepcopy(context), "sources": [f"{filename}: {ref}"],
            "source_file": filename, "evidence": {}, "warnings": [], "matches": [], "match_status": "Not checked"}


def _set(record, field, value, source):
    value = _clean_name_value(value) if field == "name" else str(value or "").strip()
    if not value:
        return
    old = record["fields"].get(field, "")
    # A form's explicit country takes precedence over an inherited section heading.
    if field == "country_label" and not record["evidence"].get(field):
        old = ""
    evidence = record["evidence"].setdefault(field, [])
    if not any(e["value"] == value for e in evidence):
        evidence.append({"value": value, "source": source})
    issue = employment_field_errors({field: value}).get(field)
    if issue:
        record["warnings"].append(issue)
        return  # The source value remains in evidence; do not invent a replacement.
    if not old:
        record["fields"][field] = value
    elif old != value and not (field == "name" and name_key(old) == name_key(value)):
        record["warnings"].append(f"Different {FIELDS[field].lower()} values in source; review the alternatives.")


def _cell_chunks(cell):
    """Keep wrapped values intact, splitting additional field labels in a cell."""
    chunks = []
    for line in re.split(r"[\n\t]+", cell):
        field, value = label_value(line)
        starts_label = field and (not value or re.match(r"^\s*[^:\-–—\n]{1,80}[:\-–—]", line))
        if chunks and field == "unit" and not value and not re.search(r"[:\-–—]", line):
            previous_field, previous_value = label_value(chunks[-1])
            if previous_field == "unit" and previous_value:
                starts_label = False  # A wrapped Anti-Drugs / Unit value, not a new label.
        if not chunks or starts_label:
            chunks.append(line)
        else:
            chunks[-1] += "\n" + line
    return chunks


def _table_country_heading(rows):
    """Recognize a country-only table without treating other form values as headings."""
    cells = [chunk.strip() for row in rows for cell in row
             for chunk in _cell_chunks(cell) if chunk.strip()]
    if len(cells) == 1:
        field, value = label_value(cells[0])
        if field == "country_label" and value:
            return value
        if word_country(cells[0]):
            return cells[0].strip(" :")
    if (len(cells) == 2 and label_value(cells[0]) == ("country_label", "")
            and label_value(cells[1])[0] is None):
        return cells[1]
    return ""


def enrich_biography(record):
    """Fill only missing supported fields; keep derivations visible and editable."""
    fields = record["fields"]
    record.setdefault("warnings", [])
    record.setdefault("evidence", {})
    bio = fields.get("bio_short", "")
    previous = record.setdefault("biography_fields", {})
    # Invalidate old automatic values after a BIO edit, preserving user edits.
    if record.get("_biography_text") != bio:
        for field, item in previous.items():
            if fields.get(field) == item["value"]:
                fields[field] = ""
            evidence = record.get("evidence", {}).get(field, [])
            record.get("evidence", {})[field] = [
                entry for entry in evidence if entry.get("source") != "Biography: " + item["evidence"]
            ]
        previous.clear()
    record["_biography_text"] = bio
    for field, item in list(previous.items()):
        if fields.get(field) != item["value"]:
            previous.pop(field)
    old_warnings = record.pop("biography_warnings", [])
    record["warnings"] = [warning for warning in record["warnings"] if warning not in old_warnings]
    record["biography_warnings"] = []
    suggestions, ambiguous = employment_from_bio(bio)
    for field in ambiguous:
        warning = f"Multiple current {field} statements in the biography; review the original."
        record["biography_warnings"].append(warning)
        if warning not in record["warnings"]:
            record["warnings"].append(warning)

    event_id = fields.get("event_reference", "")
    event_year = record.get("service_event_reference", {})
    reference_year = event_year.get("year") if event_year.get("eid") == event_id else None
    reference_source = "event start date" if reference_year else "event reference"
    event = re.fullmatch(r"PFE(\d{2})M\d+", event_id, re.I)
    if reference_year is None and event and not (event_year.get("eid") == event_id and event_year.get("checked")):
        reference_year = 2000 + int(event[1])
    reference = date(reference_year, 1, 1) if reference_year else date.today()
    experience = extract_professional_experience(bio, reference_date=reference)
    start = extract_service_start(bio, reference_date=reference)
    if start.joining_year is not None and start.method != "Needs review":
        suggestions["police_joined"] = {"value": str(start.joining_year), "evidence": start.evidence}
    for field, item in suggestions.items():
        if not fields.get(field):
            fields[field] = item["value"]
            previous[field] = item
            evidence = record.setdefault("evidence", {}).setdefault(field, [])
            entry = {"value": item["value"], "source": "Biography: " + item["evidence"]}
            if entry not in evidence:
                evidence.append(entry)

    display, method = experience.display_years, experience.method
    derived_years = (experience.method in ("Years since joining (estimate)", "Career timeline (estimate)", "Years since service start (estimate)")
                     or experience.joining_year is not None and experience.years is None)
    if derived_years:
        if reference_year is None:
            display, method = "Unknown", "Event date needed for estimate"
        elif experience.years is not None:
            display = f"{experience.years} (estimate; event year only)"
    record["professional_experience"] = {
        "display_years": display, "method": method, "scope": experience.scope,
        "reference": str(reference_year) + f" ({reference_source})" if derived_years and reference_year else "",
        "evidence": experience.evidence,
    }


def extract_docx(content, filename, slash_order="auto"):
    blocks = read_blocks(content)
    order = _slash_order(blocks) if slash_order == "auto" else slash_order
    context, records, current, pending = {}, [], None, None
    event = re.search(r"\bPFE\d{2}M\d+\b", "\n".join(b.get("text", "") for b in blocks) + "\n" + filename, re.I)
    if event:
        context["event_reference"] = event[0].upper()

    def finish():
        nonlocal current, pending
        if current and current["fields"].get("name"):
            records.append(current)
            if len(records) > MAX_RECORDS:
                raise WordExtractionError(f"The document contains more than {MAX_RECORDS:,} entries. Split it into smaller files.")
        current, pending = None, None

    for block_index, block in enumerate(blocks):
        ref = block["ref"]
        if block["kind"] == "table":
            rows = block["rows"]
            heading = _table_country_heading(rows)
            following = blocks[block_index + 1] if block_index + 1 < len(blocks) else None
            next_name = False
            if following:
                following_cells = ([following["text"]] if following["kind"] == "paragraph"
                                   else [cell for row in following["rows"] for cell in row])
                next_name = any(label_value(chunk)[0] == "name"
                                for cell in following_cells for chunk in _cell_chunks(cell))
            first_cell = next((cell for row in rows for cell in row if cell.strip()), "")
            bare_heading = label_value(first_cell)[0] is None
            # Explicit country tables can also be continuation fields after a name.
            # A bare value awaiting another field is not a representing-country heading.
            if (heading and not (bare_heading and pending is not None)
                    and (current is None or not current["fields"]["name"] or next_name or bare_heading)):
                finish()
                context["country_label"] = heading
                context.pop("travel_notes", None)
                continue
            # A new Name label starts a form; an employment-only table continues
            # the current form rather than discarding its unnamed fields.
            if any(label_value(chunk)[0] == "name" for row in rows for cell in row for chunk in _cell_chunks(cell)):
                finish()
            headers = [label_value(c) for c in rows[0]] if rows else []
            if (len(rows) > 1 and sum(field is not None and not value for field, value in headers) >= 2
                    and all(not cell or field is not None and not value for cell, (field, value) in zip(rows[0], headers))
                    and any(field == "name" for field, _ in headers)):
                for index, cells in enumerate(rows[1:], 2):
                    record = _record(filename, context, f"{ref}, row {index}")
                    for (field, _), value in zip(headers, cells):
                        if field:
                            _set(record, field, value, record["sources"][0])
                    if record["fields"]["name"]:
                        records.append(record)
                        if len(records) > MAX_RECORDS:
                            raise WordExtractionError(f"The document contains more than {MAX_RECORDS:,} entries. Split it into smaller files.")
                continue
            if current is None:
                current = _record(filename, context, ref)
            source = f"{filename}: {ref}"
            if source not in current["sources"]:
                current["sources"].append(source)
            for row in rows:
                awaiting = pending
                for cell in (chunk for value in row for chunk in _cell_chunks(value)):
                    field, value = label_value(cell)
                    if field:
                        if field == "name" and current["fields"]["name"]:
                            finish()
                            current = _record(filename, context, ref)
                        _set(current, field, value, f"{filename}: {ref}")
                        awaiting = field if not value else None
                        if field == "travel_doc_number" and re.match(r"passport\b", cell, re.I):
                            _set(current, "travel_doc_type", "Passport", f"{filename}: {ref}")
                    elif cell and awaiting:
                        if awaiting == "name" and not _clean_name_value(cell):
                            continue
                        _set(current, awaiting, cell, f"{filename}: {ref}")
                        awaiting = None
                    elif cell:
                        current["fields"]["unmapped_text"] += ("\n" if current["fields"]["unmapped_text"] else "") + cell
                pending = awaiting
            continue
        text = block["text"]
        code = word_country(text)
        if code:
            finish()
            context["country_label"] = text.strip(" :")
            context.pop("travel_notes", None)
            continue
        if re.match(r"^(?:Let\b|Automobilima\b|Flight\b|By car\b)", text, re.I) and current is None:
            context["travel_notes"] = text
            continue
        # Tab-separated lists explicitly carry name / DOB / rank / position.
        cells = [value.strip() for value in text.split("\t")]
        if len(cells) >= 2 and re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", cells[1]):
            finish()
            current = _record(filename, context, ref)
            for field, value in zip(("name", "dob", "rank", "position"), cells):
                _set(current, field, value, f"{filename}: {ref}")
            current["fields"]["unmapped_text"] = "\t".join(cells[4:])
            finish()
            continue
        numbered = re.match(r"^\d+[.)]\s+(.+)$", text)
        police = re.match(r"^(Lt\.?\s*Col\.?|Capt\.?|Lt\.?|Po\.?)\s+(.+?)\s*[-–]\s*(?:PK|KP)\s*#\s*(\d+)\s*-?\s*$", text, re.I)
        if numbered or police:
            finish()
            current = _record(filename, context, ref)
            if police:
                _set(current, "rank", police[1], f"{filename}: {ref}")
                _set(current, "name", police[2], f"{filename}: {ref}")
                _set(current, "service_number", police[3], f"{filename}: {ref}")
            else:
                value = numbered[1]
                born = re.match(r"(.+?),\s*ro[dđ]en[a]?\s+(\d{1,2}\.\s*\d{1,2}\.\s*\d{4})", value, re.I)
                dated_name = re.match(r"(.+?)\s*\((\d{1,2}\.\s*\d{1,2}\.\s*\d{4})\.?(?:\))", value)
                if born:
                    _set(current, "name", born[1], f"{filename}: {ref}")
                    _set(current, "dob", born[2], f"{filename}: {ref}")
                    _set(current, "bio_short", value, f"{filename}: {ref}")
                elif dated_name:
                    _set(current, "name", dated_name[1], f"{filename}: {ref}")
                    _set(current, "dob", dated_name[2], f"{filename}: {ref}")
                else:
                    _set(current, "name", value, f"{filename}: {ref}")
            continue
        field, value = label_value(text)
        if field:
            if field == "country_label" and value and current is None:
                context["country_label"] = value
                context.pop("travel_notes", None)
                continue
            if field == "name":
                finish()
                current = _record(filename, context, ref)
            if current:
                _set(current, field, value, f"{filename}: {ref}")
                pending = field if not value else None
                if field == "travel_doc_number" and re.match(r"passport\b", text, re.I):
                    _set(current, "travel_doc_type", "Passport", f"{filename}: {ref}")
            continue
        if current:
            current["sources"].append(f"{filename}: {ref}")
            if pending:
                if pending == "name" and not _clean_name_value(text):
                    continue
                _set(current, pending, text, f"{filename}: {ref}")
                pending = None
            else:
                field = "bio_short" if current["fields"]["bio_short"] else "unmapped_text"
                current["fields"][field] += ("\n" if current["fields"][field] else "") + text
    finish()
    for record in records:
        fields = record["fields"]
        fields["dob_raw"] = fields["dob"]
        fields["dob"] = parse_date(fields["dob"], order)
        if fields["dob_raw"] and not fields["dob"]:
            record["warnings"].append("DOB is invalid or ambiguous; enter YYYY-MM-DD after checking the original.")
        if fields["gender"].upper() in ("M", "F", "MALE", "FEMALE"):
            fields["gender"] = "Male" if fields["gender"].upper() in ("M", "MALE") else "Female"
    # A supplemental roster may repeat the prose entries in the same document.
    # Only identical name tokens AND a known, identical DOB combine automatically.
    combined, index = [], {}
    for record in records:
        fields = record["fields"]
        key = (name_key(fields["name"]), fields["dob"])
        previous = index.get(key) if fields["dob"] else None
        if previous and previous["fields"]["country_label"] and fields["country_label"]:
            first, second = previous["fields"]["country_label"], fields["country_label"]
            if normalize_text(first) != normalize_text(second) and not (word_country(first) and word_country(first) == word_country(second)):
                previous = None
        if previous:
            for field, value in fields.items():
                if field not in ("dob", "dob_raw"):
                    _set(previous, field, value, record["sources"][0])
            previous["sources"].extend(record["sources"])
            for field, evidence in record["evidence"].items():
                previous["evidence"].setdefault(field, []).extend(e for e in evidence if e not in previous["evidence"].get(field, []))
            previous["warnings"].extend(record["warnings"])
        else:
            combined.append(record)
            if fields["dob"]:
                index[key] = record
    for record in combined:
        enrich_biography(record)
        record["warnings"] = list(dict.fromkeys(record["warnings"]))
        if not record["fields"]["country_label"]:
            record["warnings"].append("Representing country is not stated; select it before confirming a database match.")
    if not combined:
        raise WordExtractionError("No named participants found. Use text-based forms, tables, or numbered lists.")
    return combined


def extract_files(files, slash_order="auto"):
    records, errors = [], []
    if slash_order not in ("auto", "mdy", "dmy"):
        raise WordExtractionError("Choose a valid slash-date format.")
    for filename, content in files:
        if not filename.lower().endswith(".docx"):
            errors.append({"file": filename, "message": "Only .docx files are supported; re-save old .doc files in Word."})
            continue
        try:
            records.extend(extract_docx(content, filename, slash_order))
        except WordExtractionError as exc:
            errors.append({"file": filename, "message": str(exc)})
        if len(records) > MAX_RECORDS:
            raise WordExtractionError(f"The batch contains more than {MAX_RECORDS:,} entries. Upload fewer documents together.")
    repeated = defaultdict(list)
    for record in records:
        if record["fields"]["dob"]:
            repeated[(name_key(record["fields"]["name"]), record["fields"]["dob"])].append(record)
    for group in repeated.values():
        if len(group) > 1:
            for record in group:
                record["warnings"].append("This name and DOB also appear in another uploaded file; entries are kept separate for review.")
    return {"records": records, "file_errors": errors}
