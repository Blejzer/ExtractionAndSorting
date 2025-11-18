# utils/excel.py
from __future__ import annotations
from openpyxl import load_workbook

# ─────────────────────────────────────────────────────────────────────────────
# 1) Strict normalizer used by the importer wherever needed
# ─────────────────────────────────────────────────────────────────────────────
def normalize_doc_type_strict(value) -> str:
    """
    Project rule: if value is exactly 'Passport' -> 'Passport', else 'ID Card'.
    No variants, no fuzzy logic.
    """
    return "Passport" if (str(value).strip() if value is not None else "") == "Passport" else "ID Card"


# ─────────────────────────────────────────────────────────────────────────────
# 2) Simple matrix: Sheet → Table → Columns (Excel header -> target field)
#    Keep this as the single source of truth for structure.
# ─────────────────────────────────────────────────────────────────────────────
SHEET_PARTICIPANTS = "Participants"
SHEET_MAIN_ONLINE = "MAIN ONLINE"
SHEET_PARTICIPANTS_LIST = "Participants List"

_WORKBOOK_CACHE = {}

COUNTRY_TABLES = [
    "tableAlb", "tableBih", "tableCro", "tableKos", "tableMne",
    "tableNmk", "tableSer", "tableInst", "tableFac", "tblTech",
]

REQUIRED_TABLES = {
    "participantslista",
    "participantslist",
    "tableAlb", "tableBih", "tableCro",
    "tableKos", "tableMne", "tableNmk",
    "tableSer", "tableInst", "tableFac", "tblTech",
}

# Country table columns (Participants sheet). All country tables share this.
_COUNTRY_TABLE_COLS = {
    "Name and Last Name": "name_full",
    "Expenses": "expenses",
    "Arrival date": "arrival_date",
    "Arrival time": "arrival_time",
    "Departure date": "departure_date",
    "Departure time": "departure_time",
    "Travel": "travel",
    "Traveling from": "traveling_from",
    "Grade (0 - BL, 1 - Pass, 2 - Excel)": "grade",
    "Notes": "notes",
}

MATRIX = {
    SHEET_PARTICIPANTS: {
        t: _COUNTRY_TABLE_COLS.copy()
        for t in COUNTRY_TABLES
    },
    SHEET_MAIN_ONLINE: {
        "ParticipantsList": {
            "No": "row_no",
            "Country": "representing_country_raw",
            "Gender": "gender",
            "Name": "first_name",
            "Middle name": "middle_name",
            "Last name": "last_name",
            "Date of Birth (DOB)": "dob",
            "Place Of Birth (POB)": "pob",
            "Country of Birth": "birth_country_raw",
            "Citizenship(s)": "citizenships_raw",
            "Phone number": "phone",
            "Email address": "email",
            "Traveling document type": "travel_doc_type",       # apply normalize_doc_type_strict in importer
            "Traveling document number": "travel_doc_number",
            "Traveling document issuance date": "travel_doc_issue_date",
            "Traveling document expiration date": "travel_doc_expiry_date",
            "Traveling document issued by": "travel_doc_issued_by",
            "Transportation": "transportation",
            "Traveling from": "travelling_from",
            "Returning to": "returning_to",
            "Diet restrictions": "diet_restrictions",
            "Organization": "organization",
            "Unit\tPosition": "unit_position",                  # note: TAB between Unit and Position
            "Rank": "rank",
            "Authority": "intl_authority",
            "Short professional biography": "bio_short",
            "Bank name": "bank_name",
            "IBAN": "iban",
            "IBAN Type": "iban_type",
            "SWIFT": "swift",
        }
    },
    SHEET_PARTICIPANTS_LIST: {
        "ParticipantsLista": {
            "No.": "row_no",
            "Name (LAST, First, Middle)": "name_lfm",
            "Position": "position",
            "Phone": "phone",
            "email": "email",
            "Country": "country_raw",
            "Name - Position": "name_position",
        }
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# 3) Tiny helpers your import service can use
# ─────────────────────────────────────────────────────────────────────────────
def list_country_tables() -> list[str]:
    return list(COUNTRY_TABLES)

def get_mapping(sheet: str, table: str) -> dict[str, str]:
    """Return dict of {Excel header -> target field}. Empty dict if unknown."""
    return MATRIX.get(sheet, {}).get(table, {})


def get_cached_workbook(path: str):
    """
    Load an Excel workbook once per import run.
    Subsequent calls return the same object.
    """
    wb = _WORKBOOK_CACHE.get(path)
    if wb is None:
        wb = load_workbook(path, data_only=True)
        _WORKBOOK_CACHE[path] = wb
    return wb

def clear_workbook_cache():
    _WORKBOOK_CACHE.clear()

