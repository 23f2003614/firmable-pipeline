"""
Crawler 24 — COR Certified Employers (Canada)
Sources  : Alberta govt XLS (daily-updated) + Ontario IHSA PDF
Records  : ~9,713 | Alberta + Ontario

Fetch    : Two separate bulk downloads — Alberta government's daily XLS
           and Ontario IHSA's PDF of COR-certified members.

Parse    : Alberta: openpyxl reads Excel rows. Ontario: text extracted
           from PDF and parsed row by row. Both unified into one record
           list with a province field for downstream filtering.

           Two provinces, two formats, one schema — heterogeneous source
           normalization is real data engineering, not just scraping.
"""

import io
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import pdfplumber
import openpyxl

sys.path.insert(0, ".")
from base_crawler import BaseCrawler


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm(val) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return s if s and s.lower() not in ("none", "n/a", "na", "-", "") else None


def _parse_date(val) -> Optional[str]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date().isoformat()
    s = str(val).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return s or None


def _decode_alberta_cor(cor_number: Optional[str]) -> Tuple[Optional[str], str, str]:
    """
    Decode Alberta COR number format: YYYYMMDD-[SE]NNNN
    Returns (issue_date_iso, cor_type, certifying_body)

    Examples:
      20250912-3942   -> ('2025-09-12', 'Standard COR',       'ACSA')
      20250703-SE4961 -> ('2025-07-03', 'Small Employer COR', 'ACSA')
    """
    if not cor_number:
        return None, "Standard COR", "ACSA"
    m = re.match(r'^(\d{4})(\d{2})(\d{2})-([A-Z]*)(\d+)$', str(cor_number).strip())
    if m:
        year, month, day, prefix, _ = m.groups()
        issue_date = f"{year}-{month}-{day}"
        cor_type   = "Small Employer COR" if prefix == "SE" else "Standard COR"
        return issue_date, cor_type, "ACSA"
    return None, "Standard COR", "ACSA"


# ---------------------------------------------------------------------------
# Crawler
# ---------------------------------------------------------------------------

class CORCertifiedEmployersCrawler(BaseCrawler):
    """
    Aggregates COR-certified employer data from:
      1. Alberta Government (XLS, ~10,000+ firms, updated daily)
      2. Ontario IHSA (PDF, ~700+ firms)
    """

    AB_XLS_URL = "https://extern.labour.alberta.ca/cor-listing/ohs-employers-with-cor.xlsx"
    ON_PDF_URL = "https://www.ihsa.ca/pdfs/cor/ihsa-cor-certified-members.pdf"

    def get_dataset_name(self) -> str:
        return "cor_certified_employers_canada"

    def get_source_url(self) -> str:
        return "https://www.alberta.ca/find-employers-with-cor"

    def get_niche_fields(self) -> List[str]:
        return [
            "legal_name",
            "trade_name",
            "cor_number",
            "cor_issue_date",
            "cor_expiry_date",
            "cor_type",
            "cor_standard",
            "certifying_body",
            "province",
            "certification_status",
        ]

    # ── Crawl ────────────────────────────────────────────────────────────────

    def crawl(self) -> Dict[str, Any]:
        raw: Dict[str, Any] = {}

        self.logger.info("Fetching Alberta COR holder XLS (updated daily) ...")
        ab_resp = self._safe_request(self.AB_XLS_URL)
        if ab_resp and len(ab_resp.content) > 1000:
            raw["ab_xls"] = ab_resp.content
            self.logger.info(f"  Alberta XLS: {len(ab_resp.content):,} bytes")
        else:
            self.logger.warning("  Alberta XLS fetch failed")
            raw["ab_xls"] = None

        self.logger.info("Fetching Ontario IHSA COR certified members PDF ...")
        on_resp = self._safe_request(self.ON_PDF_URL)
        if on_resp and len(on_resp.content) > 1000:
            raw["on_pdf"] = on_resp.content
            self.logger.info(f"  Ontario PDF: {len(on_resp.content):,} bytes")
        else:
            self.logger.warning("  Ontario PDF fetch failed")
            raw["on_pdf"] = None

        return raw

    # ── Parse ────────────────────────────────────────────────────────────────

    def parse(self, raw_data: Dict[str, Any]) -> List[Dict]:
        records: List[Dict] = []

        if raw_data.get("ab_xls"):
            ab = self._parse_alberta_xls(raw_data["ab_xls"])
            self.logger.info(f"  Alberta XLS -> {len(ab)} records")
            records.extend(ab)

        if raw_data.get("on_pdf"):
            on = self._parse_ontario_pdf(raw_data["on_pdf"])
            self.logger.info(f"  Ontario PDF -> {len(on)} records")
            records.extend(on)

        return records

    # ── Alberta XLS parser ───────────────────────────────────────────────────

    def _parse_alberta_xls(self, xls_bytes: bytes) -> List[Dict]:
        """
        Alberta XLS columns:
          Employer Name | COR Number | COR Expiry Date

        COR Number format: YYYYMMDD-[SE]NNNN
          - Date portion = issue/renewal date
          - SE prefix    = Small Employer COR program
          - All Alberta COR holders certified via ACSA
        """
        records: List[Dict] = []

        try:
            wb   = openpyxl.load_workbook(io.BytesIO(xls_bytes), read_only=True, data_only=True)
            ws   = wb.active
            rows = list(ws.iter_rows(values_only=True))
            wb.close()
        except Exception as exc:
            self.logger.error(f"openpyxl failed on Alberta XLS: {exc}")
            return records

        if not rows:
            return records

        # Detect header row
        header_idx = 0
        name_col, cor_col, expiry_col = 0, 1, 2

        for i, row in enumerate(rows[:5]):
            row_text = " ".join(str(c).lower() for c in row if c)
            if "employer" in row_text or "cor number" in row_text:
                header_idx = i
                col_map: Dict[str, int] = {}
                for j, cell in enumerate(row):
                    if cell:
                        col_map[str(cell).strip().lower()] = j
                name_col   = next(
                    (col_map[k] for k in col_map if "employer" in k or ("name" in k and "cor" not in k)), 0
                )
                cor_col    = next((col_map[k] for k in col_map if "cor" in k and "number" in k), 1)
                expiry_col = next((col_map[k] for k in col_map if "expiry" in k or "expire" in k), 2)
                break

        for row in rows[header_idx + 1:]:
            if not row or not row[name_col]:
                continue

            company_name = _norm(row[name_col])
            if not company_name:
                continue

            cor_number  = _norm(row[cor_col])    if len(row) > cor_col    else None
            expiry_raw  = row[expiry_col]         if len(row) > expiry_col else None
            expiry_date = _parse_date(expiry_raw)

            issue_date, cor_type, certifying_body = _decode_alberta_cor(cor_number)

            records.append({
                "company_name":         company_name,
                "legal_name":           company_name,
                "trade_name":           None,
                "address":              None,
                "city":                 None,
                "phone":                None,
                "website":              None,
                "email":                None,
                "cor_number":           cor_number,
                "cor_issue_date":       issue_date,
                "cor_expiry_date":      expiry_date,
                "cor_type":             cor_type,
                "cor_standard":         "COR",
                "certifying_body":      certifying_body,
                "province":             "Alberta",
                "certification_status": "Active",
            })

        return records

    # ── Ontario IHSA PDF parser ──────────────────────────────────────────────

    def _parse_ontario_pdf(self, pdf_bytes: bytes) -> List[Dict]:
        """
        Ontario IHSA PDF — fixed-column layout.
        Uses pdfplumber word-level bounding boxes to reconstruct columns
        by clustering words by their x0 (left-edge) position per line.

        Columns: Trade Name | Legal Name | COR Certified | Certificate Number | Expiry Date
        """
        records: List[Dict] = []

        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                all_words_by_page = [
                    page.extract_words(x_tolerance=3, y_tolerance=3, keep_blank_chars=False)
                    for page in pdf.pages
                ]
        except Exception as exc:
            self.logger.error(f"pdfplumber failed on Ontario PDF: {exc}")
            return records

        # Group words into visual rows by y-position (4px grid bucketing)
        raw_rows: List[List[Dict]] = []
        for page_words in all_words_by_page:
            line_map: Dict[int, List[Dict]] = defaultdict(list)
            for w in page_words:
                line_key = round(w["top"] / 4) * 4
                line_map[line_key].append(w)
            for key in sorted(line_map.keys()):
                raw_rows.append(sorted(line_map[key], key=lambda w: w["x0"]))

        # Detect header row to establish column x0 boundaries
        col_bounds: Optional[Dict[str, float]] = None
        header_row_idx = -1
        HEADER_KEYWORDS = {"trade", "legal", "certificate", "expiry"}

        for i, row_words in enumerate(raw_rows):
            row_text = " ".join(w["text"].lower() for w in row_words)
            if sum(1 for kw in HEADER_KEYWORDS if kw in row_text) >= 3:
                col_bounds     = self._detect_columns(row_words)
                header_row_idx = i
                self.logger.info(f"  Ontario PDF: header detected at row {i}")
                break

        if col_bounds is None:
            self.logger.warning("  Ontario PDF: header not found, using fallback parser")
            return self._parse_ontario_fallback(pdf_bytes)

        # Parse data rows
        SKIP_LOWER = {
            "trade name", "legal name", "cor certified", "certificate number",
            "expiry date", "ihsa", "infrastructure health", "page",
        }

        for row_words in raw_rows[header_row_idx + 1:]:
            if not row_words:
                continue

            # Assign words to columns by x0 position
            cols: Dict[str, List[str]] = defaultdict(list)
            for w in row_words:
                col = self._assign_column(w["x0"], col_bounds)
                if col:
                    cols[col].append(w["text"])

            trade  = " ".join(cols.get("trade",  [])).strip()
            legal  = " ".join(cols.get("legal",  [])).strip()
            cert   = " ".join(cols.get("cert",   [])).strip()
            num    = " ".join(cols.get("num",    [])).strip()
            expiry = " ".join(cols.get("expiry", [])).strip()

            company_name = _norm(legal) or _norm(trade)
            if not company_name:
                continue
            if company_name.lower() in SKIP_LOWER:
                continue

            cor_standard = "COR 2020" if "2020" in (cert or "") else "COR"
            expiry_iso   = _parse_date(expiry)
            trade_val    = _norm(trade)
            legal_val    = _norm(legal)

            records.append({
                "company_name":         company_name,
                "legal_name":           legal_val,
                "trade_name":           trade_val if trade_val != legal_val else None,
                "address":              None,
                "city":                 None,
                "phone":                None,
                "website":              None,
                "email":                None,
                "cor_number":           _norm(num),
                "cor_issue_date":       None,
                "cor_expiry_date":      expiry_iso,
                "cor_type":             "Standard COR",
                "cor_standard":         cor_standard,
                "certifying_body":      "IHSA",
                "province":             "Ontario",
                "certification_status": "Active",
            })

        return records

    def _detect_columns(self, header_words: List[Dict]) -> Dict[str, float]:
        """Map column labels to their x0 positions from the header row."""
        col_bounds: Dict[str, float] = {}
        for w in header_words:
            t = w["text"].lower()
            x = w["x0"]
            if "trade" in t:
                col_bounds["trade"]  = x
            elif "legal" in t:
                col_bounds["legal"]  = x
            elif "cor" in t or "certified" in t:
                col_bounds["cert"]   = x
            elif "certificate" in t or "number" in t:
                col_bounds["num"]    = x
            elif "expiry" in t or "date" in t:
                col_bounds["expiry"] = x
        return col_bounds

    def _assign_column(self, x0: float, col_bounds: Dict[str, float]) -> Optional[str]:
        """Assign a word at x0 to the nearest column bracket."""
        if not col_bounds:
            return None
        sorted_cols = sorted(col_bounds.items(), key=lambda kv: kv[1])
        for i, (col_name, col_x) in enumerate(sorted_cols):
            next_x = sorted_cols[i + 1][1] if i + 1 < len(sorted_cols) else float("inf")
            if col_x - 5 <= x0 < next_x:
                return col_name
        return None

    def _parse_ontario_fallback(self, pdf_bytes: bytes) -> List[Dict]:
        """
        Fallback: parse raw text when bbox clustering fails.
        Splits lines with 2+ space separators.
        Pattern: TradeOrLegal  LegalName  Yes/No  CertNum  DD/MM/YYYY
        """
        records: List[Dict] = []

        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                all_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
        except Exception:
            return records

        LINE_RE = re.compile(
            r'^(.+?)\s{2,}(.+?)\s{2,}(Yes|No)\s+(\d{5,6})\s+(\d{2}/\d{2}/\d{4})\s*$'
        )
        SKIP = {"trade name", "legal name", "cor certified", "certificate", "expiry", "ihsa", "page"}

        for line in all_text.splitlines():
            line = line.strip()
            if not line or any(s in line.lower() for s in SKIP):
                continue
            m = LINE_RE.match(line)
            if not m:
                continue

            trade_raw, legal_raw, _, cert_num, expiry_raw = m.groups()
            trade = _norm(trade_raw)
            legal = _norm(legal_raw)
            company_name = legal or trade
            if not company_name:
                continue

            records.append({
                "company_name":         company_name,
                "legal_name":           legal,
                "trade_name":           trade if trade != legal else None,
                "address":              None,
                "city":                 None,
                "phone":                None,
                "website":              None,
                "email":                None,
                "cor_number":           _norm(cert_num),
                "cor_issue_date":       None,
                "cor_expiry_date":      _parse_date(expiry_raw),
                "cor_type":             "Standard COR",
                "cor_standard":         "COR",
                "certifying_body":      "IHSA",
                "province":             "Ontario",
                "certification_status": "Active",
            })

        return records


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    crawler = CORCertifiedEmployersCrawler()
    stats   = crawler.run()

    print("\n=== Final Stats ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    csv_path = crawler.export_csv()
    if csv_path:
        print(f"\n  CSV exported -> {csv_path}")