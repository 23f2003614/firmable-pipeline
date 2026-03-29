"""
Crawler 21 — CanadaGAP Certified Operations (Canada)
Source   : https://www.canadagap.ca/certification/certified-companies/
Records  : ~1,890 | Canada

Fetch    : Auto-discovers the latest monthly PDF URL from the CanadaGAP
           website — no hardcoded links. Downloads on every run.

Parse    : Extracts text from PDF. Consistent tabular layout enables
           reliable row parsing — company name, certification body,
           province, commodity group.

           Self-updating like C01 USDA — always fetches current month's
           data with zero code changes when a new file is published.
"""
import re
import io
import os
import sys
from datetime import datetime, timezone, date, timedelta
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from base_crawler import BaseCrawler


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PDF_INDEX_URL = "https://www.canadagap.ca/certification/certified-companies/"
_DIRECT_PDF = (
    "https://www.canadagap.ca/uploads/293/certified-companies/22276/"
    "2026_03-mar-1-canadagap-certified.pdf"
)

_PROVINCES = {"AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT"}
_CB_TOKENS  = {"NSF","MSVS","TSLC","BNQ","CU","dicentra","GFTC"}

_DATE_RE = re.compile(
    r"\b(\d{1,2}-(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{2,4})\b",
    re.IGNORECASE,
)
_OPTION_RE = re.compile(
    r"\b(A1|A2|A3|B|C|D|E|F)(?:/(A1|A2|A3|B|C|D|E|F))?\b"
)
# Definitive location anchor: "PROV, CA" — the ", CA" suffix only appears at
# the location token and never elsewhere in the PDF line.
_LOC_RE = re.compile(
    r"(?P<pre>.+?)\s+"
    r"(?P<prov>" + "|".join(_PROVINCES) + r")"
    r",\s*CA\b"
    r"(?P<post>.*)",
    re.IGNORECASE | re.DOTALL,
)
_LEGAL_SUFFIXES_RE = re.compile(
    r"\b(Ltd\.?|Inc\.?|Corp\.?|Limited|L\.P\.|LLP|Incorporated|"
    r"Ltee\.?|ltee\.?|Enr\.?)\b",
    re.IGNORECASE,
)

# Known multi-word Canadian city names
_MULTI_WORD_CITIES = {
    "Thunder Bay", "St. Thomas", "St Thomas", "Holland Landing",
    "Jordan Station", "Niagara On The Lake", "Niagara-on-the-Lake",
    "Niagara Falls", "Bradford West Gwillimbury", "Fort Saskatchewan",
    "St. Williams", "St Williams", "Grande Prairie", "Red Deer",
    "Medicine Hat", "Moose Jaw", "Swift Current", "North Battleford",
    "Prince Albert", "Portage La Prairie", "Saint John", "New Glasgow",
    "Corner Brook", "Ste-Clotilde de Chateauguay", "Saint-Eugene-De-Guiges",
    "Canton de Hatley", "Grand Forks", "Port Alberni", "Prince George",
    "Vernon River", "Spruce Grove", "Bow Island", "Picture Butte",
    "Milk River", "Fort McMurray", "Stony Plain", "High River",
}

# Commodity keywords (English + French)
_POTATO_KW = ["potato", "pomme de terre", "patate"]
_FRUIT_KW = [
    "apple", "peach", "pear", "plum", "cherry", "nectarine", "apricot",
    "blueberr", "strawberr", "raspberry", "berry", "grape", "prune",
    "pomme", "cerise", "abricot", "peche", "bleuet", "fraise",
    "framboise", "raisin", "blackberr", "haskap", "rhubarb",
    "sweet cherr", "currant", "gooseberr", "mure", "groseille",
]
_VEG_KW = [
    "carrot", "onion", "tomato", "pepper", "cucumber", "broccoli",
    "cauliflower", "corn", "bean", "squash", "spinach", "lettuce",
    "asparagus", "garlic", "choy", "bok", "gai lan", "leek", "celery",
    "beet", "rutabaga", "parsnip", "turnip", "radish", "pumpkin",
    "gourd", "zucchini", "brussels", "kale", "dill", "cilantro",
    "fenugreek", "sweet potato", "yam", "sweet corn", "vegetable",
    # French
    "ail", "oignon", "chou", "laitue", "celeri", "navet", "radis",
    "citrouille", "poivron", "concombre", "pois", "epinard", "poireau",
    "legume", "haricot", "mais", "carotte",
]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _parse_date(raw: str) -> str:
    for fmt in ("%d-%b-%y", "%d-%b-%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw.strip()


def _cert_status(expiry_str: str) -> str:
    if not expiry_str:
        return "UNKNOWN"
    try:
        exp = datetime.strptime(expiry_str, "%Y-%m-%d").date()
        today = date.today()
        if exp < today:
            return "EXPIRED"
        if exp <= today + timedelta(days=60):
            return "EXPIRING_SOON"
        return "ACTIVE"
    except ValueError:
        return "UNKNOWN"


def _derive_operation_type(scope: str) -> str:
    if not scope:
        return "Other"
    s = scope.lower()
    types = []
    if "production" in s or "producteur" in s:
        types.append("Producer")
    if "greenhouse" in s and "Producer" in types:
        types = ["Greenhouse Producer" if t == "Producer" else t for t in types]
    if "repacking" in s or "remballage" in s:
        types.append("Repacker")
    elif "packing" in s or "emballage" in s:
        types.append("Packer")
    if "wholesale" in s or "wholesaling" in s or "commerce en gros" in s:
        types.append("Wholesaler")
    if "broker" in s or "brokerage" in s or "courtage" in s:
        types.append("Broker")
    if "storage" in s or "entreposage" in s:
        types.append("Storage")
    if "transport" in s:
        types.append("Transporter")
    return " / ".join(types) if types else "Other"


def _derive_commodity_group(scope: str) -> str:
    if not scope:
        return "Unknown"
    s = scope.lower()
    if "greenhouse" in s or "serre" in s:
        return "Greenhouse"
    hp = any(k in s for k in _POTATO_KW)
    hf = any(k in s for k in _FRUIT_KW)
    hv = any(k in s for k in _VEG_KW)
    if hp and not hf and not hv:
        return "Potatoes"
    if hf and not hv and not hp:
        return "Fruits"
    if hv and not hf and not hp:
        return "Vegetables"
    if hf or hv or hp:
        return "Mixed"
    if any(k in s for k in [
        "fruit", "legume", "fresh produce", "wholesale", "repacking",
        "brokerage", "commerce", "fruits et legumes",
    ]):
        return "Mixed"
    return "Other"


def _best_city(text: str) -> str:
    """Return city from trailing fragment, preferring known multi-word cities."""
    text = text.strip()
    for mc in sorted(_MULTI_WORD_CITIES, key=len, reverse=True):
        if text.lower().endswith(mc.lower()):
            return mc
    return text.split()[-1] if text.split() else text


def _split_name_city(pre_prov: str) -> Tuple[str, Optional[str], str]:
    """
    Split text before province into (legal_name, trade_name, city).
    Handles: o/a, (dba ...), slash-trade-name, and plain legal-suffix patterns.
    """
    pre = pre_prov.strip()

    # 1. o/a (also "o/ a" PDF line-break artefact)
    oa = re.search(r"\bo/?a\b\s*", pre, re.IGNORECASE)
    if oa:
        legal = pre[: oa.start()].strip()
        rest  = pre[oa.end():].strip()
        city  = _best_city(rest)
        city_words = city.split()
        rest_words = rest.split()
        trade_name = (
            " ".join(rest_words[: len(rest_words) - len(city_words)])
            if len(rest_words) > len(city_words)
            else None
        )
        return legal, trade_name or None, city

    # 2. (dba ...)
    dba = re.search(r"\(\s*dba\s+", pre, re.IGNORECASE)
    if dba:
        legal = pre[: dba.start()].strip()
        rest  = pre[dba.end():]
        close = rest.find(")")
        if close != -1:
            trade_name = rest[:close].strip()
            after      = rest[close + 1:].strip()
        else:
            trade_name = rest.strip()
            after      = ""
        city = _best_city(after) if after.strip() else ""
        return legal, trade_name or None, city

    # 3. slash trade-name: "Corp / TradeName City"
    slash = re.search(r"(?<=[A-Za-z.])\s*/\s*(?=[A-Z])", pre)
    if slash:
        legal = pre[: slash.start()].strip()
        rest  = pre[slash.end():].strip()
        city  = _best_city(rest)
        city_words = city.split()
        rest_words = rest.split()
        trade_name = (
            " ".join(rest_words[: len(rest_words) - len(city_words)])
            if len(rest_words) > len(city_words)
            else None
        )
        return legal, trade_name or None, city

    # 4. no trade-name marker: split at last legal suffix
    suffix_matches = list(_LEGAL_SUFFIXES_RE.finditer(pre))
    if suffix_matches:
        last_suf  = suffix_matches[-1]
        legal     = pre[: last_suf.end()].strip()
        city_part = pre[last_suf.end():].strip().strip(".,-").strip()
        city      = _best_city(city_part) if city_part else ""
        return legal, None, city

    # 5. fallback: last word is city
    # Guard: if pre has no legal-entity keywords it IS the city name itself
    if not any(kw in pre.lower() for kw in
               ["ltd", "inc", "corp", "farm", "ferme", "senc", "enr", "llp"]):
        return "", None, pre.strip()
    words = pre.split()
    if len(words) >= 2:
        return " ".join(words[:-1]), None, words[-1]
    return pre, None, ""


# ---------------------------------------------------------------------------
# Line-level parser
# ---------------------------------------------------------------------------

def _parse_line(line: str) -> Optional[Dict]:
    """
    Parse one flat PDF text line into a structured record.

    Token order (after text extraction collapses columns):
      <LEGAL> [o/a|dba|/] [TRADE]  <CITY>  <PROV>, CA
      <SCOPE>  <CERT_BODY>  <OPTION>  <ISSUE_DATE>  <EXPIRY_DATE>
    """
    line = line.strip()
    if not line or len(line) < 20:
        return None

    # Step 1: extract dates
    dates = _DATE_RE.findall(line)
    if len(dates) < 2:
        return None
    issue_date  = _parse_date(dates[-2])
    expiry_date = _parse_date(dates[-1])
    work = _DATE_RE.sub("", line).strip()

    # Step 2: extract certification body
    cert_body = None
    for cb in sorted(_CB_TOKENS, key=len, reverse=True):
        m = re.search(r"\b" + re.escape(cb) + r"\b", work)
        if m:
            cert_body = cb
            work = (work[: m.start()] + work[m.end():]).strip()
            break

    # Step 3: extract option code (rightmost match)
    opt_match = None
    for m in _OPTION_RE.finditer(work):
        opt_match = m
    option_code = opt_match.group(0) if opt_match else None
    if opt_match:
        work = (work[: opt_match.start()] + work[opt_match.end():]).strip()

    # Step 4: split at "<PROV>, CA" anchor
    loc_m = _LOC_RE.match(work)
    if not loc_m:
        return None

    province  = loc_m.group("prov").upper()
    pre_prov  = loc_m.group("pre").strip()
    scope_raw = loc_m.group("post").strip().strip(",.").strip()

    # Step 5: company / trade / city split
    company_name, trade_name, city = _split_name_city(pre_prov)

    # clean company name
    company_name = company_name.strip().strip("-./,").strip()
    for cb in _CB_TOKENS:
        company_name = re.sub(r"\b" + re.escape(cb) + r"\b", "", company_name).strip()
    company_name = re.sub(
        r"\s+(A1|A2|A3|B|C|D|E|F)(\/\w+)?\s*$", "", company_name
    ).strip()
    if not company_name or len(company_name) < 2:
        return None

    # strip stray trailing option letter from scope (BUG-4)
    if scope_raw and option_code:
        scope_raw = re.sub(
            r"\s+\b(A1|A2|A3|B|C|D|E|F)(\/[A-Z0-9]+)?\s*$", "", scope_raw
        ).strip()

    city = city.strip().strip(",").strip() or None

    return {
        "company_name"         : company_name,
        "trade_name"           : trade_name,
        "city"                 : city,
        "province"             : province,
        "country_code"         : "CA",
        "address"              : f"{city}, {province}, Canada" if city else f"{province}, Canada",
        "state"                : province,
        "scope"                : scope_raw or None,
        "certification_option" : option_code,
        "certification_body"   : cert_body,
        "cert_issue_date"      : issue_date or None,
        "cert_expiry_date"     : expiry_date or None,
        "cert_status"          : _cert_status(expiry_date),
        "operation_type"       : _derive_operation_type(scope_raw),
        "commodity_group"      : _derive_commodity_group(scope_raw),
        "is_multisite"         : "No",   # updated in second pass
        "website"              : None,
        "phone"                : None,
        "email"                : None,
        "data_quality_note"    : (
            "Company name partially truncated in source PDF — "
            "certification body registry is definitive source"
            if re.search(r'\.\.|\.{2,}', company_name)
               or company_name.startswith('&')
               or company_name.startswith('.')
            else None
        ),
    }


# ---------------------------------------------------------------------------
# Crawler class
# ---------------------------------------------------------------------------

class CanadaGAPCrawler(BaseCrawler):
    """
    Crawls the official CanadaGAP monthly PDF (consolidated list from all
    six accredited certification bodies) and parses every certified operation.
    """

    def get_dataset_name(self) -> str:
        return "canadagap_certified_operations"

    def get_source_url(self) -> str:
        return _PDF_INDEX_URL

    def get_niche_fields(self) -> List[str]:
        return [
            "trade_name",
            "scope",
            "certification_option",
            "certification_body",
            "cert_issue_date",
            "cert_expiry_date",
            "cert_status",
            "operation_type",
            "commodity_group",
            "is_multisite",
            "province",
            "city",
            "country_code",
            "data_quality_note",
        ]

    # ── schema migration ──────────────────────────────────────────────────
    def _store(self, records):
        """
        Override base _store with two responsibilities:

        1. FRESH-CRAWL RESET — This dataset is a complete monthly snapshot from
           CanadaGAP.  Every run replaces the entire table so old records never
           accumulate.  We delete all rows whose dedup_hash is NOT in the current
           crawl batch, then let base._store() upsert the live records.

        2. SCHEMA MIGRATION — If the table was created by an older run that
           pre-dates a new column (e.g. data_quality_note), ALTER TABLE adds it
           before any INSERT/UPDATE so we never hit "no such column".
        """
        import sqlite3 as _sq
        if not records:
            return 0, 0
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        conn = _sq.connect(self.db_path)
        cur  = conn.cursor()
        table = self.get_dataset_name()

        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        )
        table_exists = cur.fetchone() is not None

        if table_exists:
            # ── 1. Schema migration: add any missing columns ──────────────
            cur.execute(f'PRAGMA table_info("{table}")')
            existing_cols = {row[1] for row in cur.fetchall()}
            for col in records[0].keys():
                if col not in existing_cols:
                    self.logger.info(
                        f"Schema migration: adding column '{col}' to '{table}'"
                    )
                    cur.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" TEXT')

            # ── 2. Purge stale rows not in this crawl batch ───────────────
            live_hashes = {r["dedup_hash"] for r in records if r.get("dedup_hash")}
            if live_hashes:
                placeholders = ",".join("?" * len(live_hashes))
                cur.execute(
                    f'DELETE FROM "{table}" WHERE dedup_hash NOT IN ({placeholders})',
                    list(live_hashes),
                )
                deleted = cur.rowcount
                if deleted:
                    self.logger.info(
                        f"Purged {deleted} stale rows not in current crawl batch"
                    )

            conn.commit()

        conn.close()
        return super()._store(records)

    def crawl(self) -> bytes:
        pdf_url = self._discover_pdf_url()
        self.logger.info(f"PDF URL: {pdf_url}")
        resp = self._safe_request(pdf_url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; FirmableBot/1.0; B2B Research)",
            "Accept"    : "application/pdf,*/*",
            "Referer"   : _PDF_INDEX_URL,
        })
        if resp is None:
            raise RuntimeError(f"Failed to download PDF from {pdf_url}")
        self.logger.info(f"Downloaded PDF ({len(resp.content):,} bytes)")
        return resp.content

    def _discover_pdf_url(self) -> str:
        try:
            resp = self._safe_request(_PDF_INDEX_URL)
            if resp:
                m = re.search(
                    r'href="(https://www\.canadagap\.ca/uploads/\d+/'
                    r'certified-companies/\d+/[^"]+\.pdf)"',
                    resp.text,
                )
                if m:
                    return m.group(1)
        except Exception as exc:
            self.logger.warning(f"PDF discovery failed ({exc}); using hard-coded URL")
        return _DIRECT_PDF

    def parse(self, raw_data: bytes) -> List[Dict]:
        text = self._extract_pdf_text(raw_data)
        if not text:
            raise RuntimeError("PDF text extraction returned empty content.")

        records = []
        name_count = {}

        for raw_line in text.splitlines():
            rec = _parse_line(raw_line)
            if rec is None:
                continue
            key = rec["company_name"].lower()
            name_count[key] = name_count.get(key, 0) + 1
            records.append(rec)

        multi_keys = {k for k, v in name_count.items() if v > 1}
        for rec in records:
            rec["is_multisite"] = "Yes" if rec["company_name"].lower() in multi_keys else "No"

        self.logger.info(f"Parsed {len(records)} records from PDF")
        return records

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        # pdfplumber (preferred)
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                text = "\n".join(
                    (page.extract_text(layout=False) or "") for page in pdf.pages
                )
            self.logger.info(f"pdfplumber: {len(text):,} chars")
            return text
        except ImportError:
            self.logger.warning("pdfplumber not installed; trying pypdf")
        except Exception as exc:
            self.logger.warning(f"pdfplumber failed ({exc}); trying pypdf")

        # pypdf
        try:
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(pdf_bytes))
            text   = "\n".join((p.extract_text() or "") for p in reader.pages)
            self.logger.info(f"pypdf: {len(text):,} chars")
            return text
        except ImportError:
            pass
        except Exception as exc:
            self.logger.warning(f"pypdf failed ({exc})")

        # PyPDF2 (older environments)
        try:
            import PyPDF2
            reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
            text   = "\n".join((p.extract_text() or "") for p in reader.pages)
            self.logger.info(f"PyPDF2: {len(text):,} chars")
            return text
        except ImportError:
            raise RuntimeError(
                "No PDF library found. Install one:\n"
                "  pip install pdfplumber   (recommended)\n"
                "  pip install pypdf"
            )
        except Exception as exc:
            raise RuntimeError(f"All PDF extraction attempts failed: {exc}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    crawler  = CanadaGAPCrawler()
    stats    = crawler.run()
    csv_path = crawler.export_csv()

    print("\n" + "=" * 55)
    print("CanadaGAP Certified Operations — Run Summary")
    print("=" * 55)
    for k, v in stats.items():
        print(f"  {k:<28} {v}")
    if csv_path:
        print(f"\n  CSV  -> {csv_path}")
    print("=" * 55)