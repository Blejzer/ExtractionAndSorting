"""Read monetary totals without relying on workbook row numbers."""

import math
import re
from decimal import Decimal, InvalidOperation


def parse_cost(value) -> float:
    if isinstance(value, bool) or value is None:
        raise ValueError("Cost must be a finite, non-negative number.")
    text = str(value).strip()
    text = re.sub(r"^(?:EUR|USD|€|\$)\s*|\s*(?:EUR|USD|€|\$)$", "", text, flags=re.I)
    text = re.sub(r"\s", "", text)
    # Accept the usual decimal and thousands separators, without guessing
    # ambiguous single separators such as 64,711.
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            if not re.fullmatch(r"\d{1,3}(?:\.\d{3})+,\d+", text):
                raise ValueError("Invalid cost separators. Use 64711.19.")
            text = text.replace(".", "").replace(",", ".")
        else:
            if not re.fullmatch(r"\d{1,3}(?:,\d{3})+\.\d+", text):
                raise ValueError("Invalid cost separators. Use 64711.19.")
            text = text.replace(",", "")
    elif "," in text:
        if not re.fullmatch(r"\d+,\d{1,2}", text):
            raise ValueError("Use an unambiguous cost, for example 64711.19.")
        text = text.replace(",", ".")
    if not re.fullmatch(r"\d+(?:\.\d+)?", text):
        raise ValueError("Cost must be a finite, non-negative number.")
    try:
        amount = Decimal(text)
        result = float(amount)
    except (InvalidOperation, OverflowError, ValueError) as exc:
        raise ValueError("Cost must be a finite, non-negative number.") from exc
    if not math.isfinite(result):
        raise ValueError("Cost must be a finite, non-negative number.")
    return result


def read_grand_total(sheet) -> float | None:
    totals = []
    for row in sheet.iter_rows():
        for cell in row:
            label = re.sub(r"\s+", " ", str(cell.value or "")).strip().rstrip(":").strip()
            if label.casefold() != "grand total":
                continue
            amounts = []
            for candidate in row[cell.column:]:
                value = candidate.value
                if value is None or str(value).strip().upper() in {"", "EUR", "USD", "€", "$"}:
                    continue
                try:
                    amounts.append(parse_cost(value))
                except ValueError as exc:
                    raise ValueError(f"COST Overview!{candidate.coordinate}: {exc}") from exc
            if len(amounts) != 1:
                raise ValueError(
                    f"COST Overview!{cell.coordinate}: GRAND TOTAL must have one amount to its right. "
                    "If it is a formula, recalculate and save the workbook in Excel first."
                )
            totals.extend(amounts)
    if len(totals) > 1:
        raise ValueError("COST Overview contains multiple GRAND TOTAL rows; keep one event total.")
    return totals[0] if totals else None
