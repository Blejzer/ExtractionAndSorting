# services/xlsx_tables_inspector.py
from __future__ import annotations

import posixpath
import re
import sys
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

# XML namespaces
NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
NS_REL  = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
NS_PKG  = "http://schemas.openxmlformats.org/package/2006/relationships"

# -----------------------
# Data structures
# -----------------------

@dataclass(frozen=True)
class SheetRef:
    title: str
    xml_path: str  # e.g. 'xl/worksheets/sheet1.xml'

@dataclass(frozen=True)
class TableRef:
    name: str            # original displayName/name of the table (e.g., 'tableAlb')
    name_norm: str       # normalized (lowercase, alnum-only)
    sheet_title: str     # user-facing sheet title (e.g., 'Participants')
    ref: str             # range like 'B6:K42'
    table_xml_path: str  # e.g. 'xl/tables/table1.xml'

# -----------------------
# Helpers
# -----------------------

def _norm_name(s: str | None) -> str:
    """normalize names for matching: lowercase, remove all non-alphanumerics."""
    if not s:
        return ""
    return re.sub(r"[^0-9a-zA-Z]+", "", s).lower()

def _read_xml(zf: zipfile.ZipFile, path: str) -> Optional[ET.Element]:
    try:
        with zf.open(path) as fh:
            return ET.fromstring(fh.read())
    except KeyError:
        return None

def _resolve_rel_target(base_xml_path: str, target: str) -> str:
    """
    Resolve a relationship target (often '../tables/table1.xml') relative to the base path.
    Returns a normalized posix path within the zip (e.g., 'xl/tables/table1.xml').
    """
    base_dir = posixpath.dirname(base_xml_path)
    if target.startswith("/"):
        return target.lstrip("/")
    return posixpath.normpath(posixpath.join(base_dir, target))

# -----------------------
# Core logic
# -----------------------

def list_sheets(path: str) -> List[SheetRef]:
    """
    Return a list of worksheets with their human title and XML path inside the XLSX.
    """
    with zipfile.ZipFile(path) as zf:
        wb = _read_xml(zf, "xl/workbook.xml")
        if wb is None:
            return []

        # Map r:id -> sheet title
        rid_to_title: Dict[str, str] = {}
        for sheet in wb.findall(f".//{{{NS_MAIN}}}sheets/{{{NS_MAIN}}}sheet"):
            title = sheet.get("name") or ""
            rid = sheet.get(f"{{{NS_REL}}}id")  # r:id
            if rid and title:
                rid_to_title[rid] = title

        # Map r:id -> target xml path (worksheets/sheetN.xml) via workbook rels
        rels = _read_xml(zf, "xl/_rels/workbook.xml.rels")
        if rels is None:
            return []

        out: List[SheetRef] = []
        for rel in rels.findall(f".//{{{NS_PKG}}}Relationship"):
            rid = rel.get("Id")
            typ = rel.get("Type", "")
            tgt = rel.get("Target", "")
            if not (rid and tgt and typ.endswith("/worksheet")):
                continue
            # Resolve to zip path
            sheet_xml = tgt.lstrip("/") if tgt.startswith("/") else posixpath.normpath(posixpath.join("xl", tgt))
            title = rid_to_title.get(rid)
            if title:
                out.append(SheetRef(title=title, xml_path=sheet_xml))
        return out

def list_tables(path: str) -> List[TableRef]:
    """
    Scan all worksheets, follow their relationships to table parts, and return all tables.
    Works without openpyxl; reads the XLSX zip directly.
    """
    tables: List[TableRef] = []
    with zipfile.ZipFile(path) as zf:
        sheets = list_sheets(path)

        for s in sheets:
            # Find tablePart r:ids inside the sheet XML
            sheet_xml = _read_xml(zf, s.xml_path)
            if sheet_xml is None:
                continue
            table_parts = sheet_xml.findall(f".//{{{NS_MAIN}}}tableParts/{{{NS_MAIN}}}tablePart")
            if not table_parts:
                continue

            # Read this sheet's relationships to map r:id -> table Target
            rels_path = posixpath.join(
                posixpath.dirname(s.xml_path),
                "_rels",
                posixpath.basename(s.xml_path) + ".rels",
            )
            rels = _read_xml(zf, rels_path)
            if rels is None:
                continue

            rid_to_target: Dict[str, str] = {}
            for rel in rels.findall(f".//{{{NS_PKG}}}Relationship"):
                rid = rel.get("Id")
                typ = rel.get("Type", "")
                tgt = rel.get("Target", "")
                if rid and tgt and typ.endswith("/table"):
                    rid_to_target[rid] = tgt

            for tp in table_parts:
                rid = tp.get(f"{{{NS_REL}}}id")  # r:id
                if not rid or rid not in rid_to_target:
                    continue
                tgt = rid_to_target[rid]
                table_xml_path = _resolve_rel_target(s.xml_path, tgt)

                # Load table xml and read attributes
                tbl_xml = _read_xml(zf, table_xml_path) or _read_xml(zf, tgt.lstrip("/"))
                if tbl_xml is None:
                    continue
                display_name = tbl_xml.get("displayName") or tbl_xml.get("name") or ""
                ref = tbl_xml.get("ref") or ""
                if not (display_name and ref):
                    continue

                tables.append(
                    TableRef(
                        name=display_name,
                        name_norm=_norm_name(display_name),
                        sheet_title=s.title,
                        ref=ref,
                        table_xml_path=table_xml_path,
                    )
                )
    return tables

# -----------------------
# Pretty debug printing
# -----------------------

def print_report(path: str) -> None:
    sheets = list_sheets(path)
    print("Worksheets:")
    for s in sheets:
        print(f"  - {s.title}  ({s.xml_path})")

    tabs = list_tables(path)
    if not tabs:
        print("\nNo tables detected.")
    else:
        print("\nTables (normalized → original @ sheet [ref] {xml}):")
        for t in tabs:
            print(f"  - {t.name_norm:20s} → {t.name} @ {t.sheet_title} [{t.ref}] {{{t.table_xml_path}}}")

# -----------------------
# CLI usage
# -----------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python services/xlsx_tables_inspector.py <path-to-xlsx>")
        sys.exit(2)
    print_report(sys.argv[1])
