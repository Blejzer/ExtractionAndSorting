"""Excel helpers focused on workbook reuse and memoization."""
from __future__ import annotations

from typing import Callable, Dict, Tuple, TYPE_CHECKING

import openpyxl
import pandas as pd

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
