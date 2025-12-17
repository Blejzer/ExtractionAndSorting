# utils/excel.py
from __future__ import annotations

from typing import Callable, Dict, Tuple, TYPE_CHECKING

import re

import openpyxl
import pandas as pd

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

COUNTRY_TABLES = [
    "tableAlb", "tableBih", "tableCro", "tableKos", "tableMne",
    "tableNmk", "tableSer", "tableInst", "tableFac", "tblTech",
]

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
def _norm_tablename(name: str) -> str:
    """Normalize an Excel table name to a lowercase alphanumeric key."""

    return re.sub(r"[^0-9a-zA-Z]+", "", (name or "")).lower()


def list_country_tables() -> list[str]:
    return list(COUNTRY_TABLES)

def get_mapping(sheet: str, table: str) -> dict[str, str]:
    """Return dict of {Excel header -> target field}. Empty dict if unknown."""
    return MATRIX.get(sheet, {}).get(table, {})


if TYPE_CHECKING:  # pragma: no cover - imported only for typing
    from openpyxl.workbook import Workbook
    from openpyxl.worksheet.worksheet import Worksheet
    from services.xlsx_tables_inspector import TableRef


class WorkbookCache:
    """Cache a workbook and derived table DataFrames for a single XLSX path."""

    def __init__(self, path: str):
        self.path = path
        self._workbook: Workbook | None = None
        self._table_cache: Dict[Tuple[str, str], pd.DataFrame] = {}

    def get_workbook(self) -> Workbook:
        """Return (and memoize) the loaded openpyxl workbook for ``path``."""
        if self._workbook is None:
            self._workbook = openpyxl.load_workbook(self.path, data_only=True)
        return self._workbook

    def get_sheet(self, title: str) -> Worksheet:
        """Return a worksheet from the cached workbook."""
        return self.get_workbook()[title]

    def get_table_df(
        self,
        table: "TableRef",
        builder: Callable[["Worksheet"], pd.DataFrame],
    ) -> pd.DataFrame:
        """Return a memoized DataFrame for ``table`` using ``builder`` if needed."""
        key = (table.sheet_title, table.ref)
        if key not in self._table_cache:
            worksheet = self.get_sheet(table.sheet_title)
            self._table_cache[key] = builder(worksheet)
        return self._table_cache[key]

    def clear(self) -> None:
        """Drop cached workbook + table data (mainly for tests)."""
        self._workbook = None
        self._table_cache.clear()
