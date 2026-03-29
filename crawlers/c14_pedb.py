"""
Crawler 14 — US Colocation & Data Center Facilities (PeeringDB)
Source   : https://www.peeringdb.com/api/fac?country=US&status=ok
Records  : ~1,367 | US

Fetch    : Paginates through PeeringDB's public REST API — no auth,
           no rate limits. Fetches pages until response is empty.

Parse    : Maps PeeringDB fields to schema. Extracts facility name,
           address, org name, net_count, ix_count, policy type, tech email.

           Cleanest API in the project — fully open, no tricks required.
           Authoritative internet infrastructure data, completely free.
"""

import os
import sys
import time
import csv
import sqlite3
import logging
import hashlib
import io
import re
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Any

# ---------------------------------------------------------------------------
# Bootstrap: import project BaseCrawler or fall back to inline definition
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
try:
    from base_crawler import BaseCrawler
    _BASE_IMPORTED = True
except ModuleNotFoundError:
    _BASE_IMPORTED = False

if not _BASE_IMPORTED:
    class BaseCrawler(ABC):
        def __init__(self, db_path="data/firmable.db", log_dir="logs"):
            self.db_path     = db_path
            self.log_dir     = log_dir
            self.session_id  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            self.stats       = dict(
                total_parsed=0, total_new=0,
                total_updated=0, total_duplicates=0, total_errors=0
            )
            self._setup_logging()

        def _setup_logging(self):
            os.makedirs(self.log_dir, exist_ok=True)
            self.logger = logging.getLogger(self.get_dataset_name())
            self.logger.setLevel(logging.DEBUG)
            if not self.logger.handlers:
                fh = logging.FileHandler(
                    os.path.join(
                        self.log_dir,
                        f"{self.get_dataset_name()}_{self.session_id}.log",
                    ),
                    encoding="utf-8",
                )
                fh.setFormatter(
                    logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
                )
                self.logger.addHandler(fh)
                try:
                    stream = io.TextIOWrapper(
                        sys.stdout.buffer, encoding="utf-8",
                        errors="replace", line_buffering=True,
                    )
                except Exception:
                    stream = sys.stdout
                ch = logging.StreamHandler(stream)
                ch.setLevel(logging.INFO)
                ch.setFormatter(
                    logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
                )
                self.logger.addHandler(ch)

        @abstractmethod
        def get_dataset_name(self) -> str: ...
        @abstractmethod
        def get_source_url(self) -> str: ...
        @abstractmethod
        def get_niche_fields(self) -> List[str]: ...
        @abstractmethod
        def crawl(self) -> Any: ...
        @abstractmethod
        def parse(self, raw_data: Any) -> List[Dict]: ...

        def run(self) -> Dict:
            self.logger.info("=" * 50)
            self.logger.info(f"STARTING: {self.get_dataset_name()}")
            self.logger.info(f"Source  : {self.get_source_url()}")
            self.logger.info("=" * 50)
            try:
                self.logger.info("Step 1: Fetching data...")
                raw = self.crawl()
                self.logger.info("Step 2: Parsing records...")
                recs = self.parse(raw)
                self.stats["total_parsed"] = len(recs)
                self.logger.info(f"  Parsed {len(recs)} records")
                self.logger.info("Step 3: Cleaning data...")
                cleaned = [self._clean(r) for r in recs if r.get("company_name")]
                self.logger.info(f"  {len(cleaned)} records after cleaning")
                self.logger.info("Step 4: Deduplicating...")
                unique = self._deduplicate(cleaned)
                self.stats["total_duplicates"] = len(cleaned) - len(unique)
                self.logger.info(f"  {len(unique)} unique records")
                self.logger.info("Step 5: Saving to database...")
                new, upd = self._store(unique)
                self.stats["total_new"]     = new
                self.stats["total_updated"] = upd
            except Exception as exc:
                self.stats["total_errors"] += 1
                self.logger.error(f"FAILED: {exc}", exc_info=True)
                raise
            self.logger.info(
                f"DONE: {self.stats['total_new']} new, "
                f"{self.stats['total_updated']} updated"
            )
            return self.stats

        def _clean(self, record: Dict) -> Dict:
            cleaned = {}
            for k, v in record.items():
                if isinstance(v, str):
                    v = " ".join(v.strip().split())
                    if v.lower() in ("n/a", "na", "none", "null", "-", ""):
                        v = None
                cleaned[k] = v
            if cleaned.get("phone"):
                ph = "".join(
                    c for c in cleaned["phone"] if c.isdigit() or c in "+-(). "
                )
                cleaned["phone"] = ph.strip() or None
            if cleaned.get("website") and not str(cleaned["website"]).startswith("http"):
                cleaned["website"] = "https://" + cleaned["website"]
            if cleaned.get("email"):
                cleaned["email"] = str(cleaned["email"]).lower()
            cleaned["dataset"]       = self.get_dataset_name()
            cleaned["source_url"]    = self.get_source_url()
            cleaned["crawl_date"]    = datetime.now(timezone.utc).isoformat()
            cleaned["crawl_session"] = self.session_id
            return cleaned

        def _deduplicate(self, records: List[Dict]) -> List[Dict]:
            seen, unique = set(), []
            for rec in records:
                raw_key = (
                    (rec.get("company_name") or "").lower()
                    + (rec.get("address")      or "").lower()
                    + (rec.get("state")        or "").lower()
                    + self.get_dataset_name()
                )
                h = hashlib.md5(raw_key.encode()).hexdigest()
                if h not in seen:
                    seen.add(h)
                    rec["dedup_hash"] = h
                    unique.append(rec)
            return unique

        def _store(self, records: List[Dict]) -> Tuple[int, int]:
            if not records:
                return 0, 0
            os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
            conn   = sqlite3.connect(self.db_path)
            cur    = conn.cursor()
            table  = self.get_dataset_name()
            cols   = list(records[0].keys())
            col_defs = ", ".join(f'"{c}" TEXT' for c in cols)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS "{table}" (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    {col_defs},
                    first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_seen  TEXT DEFAULT CURRENT_TIMESTAMP,
                    is_new     INTEGER DEFAULT 1
                )
            """)
            cur.execute(
                f'CREATE INDEX IF NOT EXISTS "idx_{table}_hash" '
                f'ON "{table}" (dedup_hash)'
            )
            new_cnt = upd_cnt = 0
            for rec in records:
                cur.execute(
                    f'SELECT id FROM "{table}" WHERE dedup_hash = ?',
                    (rec["dedup_hash"],)
                )
                if cur.fetchone():
                    sets = ", ".join(f'"{k}" = ?' for k in rec.keys())
                    cur.execute(
                        f'UPDATE "{table}" SET {sets}, '
                        f'last_seen = CURRENT_TIMESTAMP, is_new = 0 '
                        f'WHERE dedup_hash = ?',
                        list(rec.values()) + [rec["dedup_hash"]]
                    )
                    upd_cnt += 1
                else:
                    col_str = ", ".join(f'"{k}"' for k in rec.keys())
                    val_str = ", ".join("?" * len(rec))
                    cur.execute(
                        f'INSERT INTO "{table}" ({col_str}) VALUES ({val_str})',
                        list(rec.values())
                    )
                    new_cnt += 1
            conn.commit()
            conn.close()
            return new_cnt, upd_cnt

        def _safe_request(self, url, params=None, headers=None, max_retries=3):
            import requests
            for attempt in range(max_retries):
                try:
                    resp = requests.get(
                        url, params=params, headers=headers, timeout=30
                    )
                    resp.raise_for_status()
                    return resp
                except requests.exceptions.HTTPError:
                    code = resp.status_code
                    if code == 429:
                        wait = 2 ** (attempt + 2)
                        self.logger.warning(f"Rate limited. Waiting {wait}s...")
                        time.sleep(wait)
                    elif code >= 500:
                        time.sleep(2 ** attempt)
                    else:
                        self.logger.error(f"HTTP {code}: {url}")
                        return None
                except Exception as exc:
                    self.logger.warning(
                        f"Request failed ({attempt + 1}/{max_retries}): {exc}"
                    )
                    time.sleep(2 ** attempt)
            return None

        def _rate_limit(self, seconds: float = 1.0):
            time.sleep(seconds)

        def export_csv(self, path=None):
            if not path:
                os.makedirs("data/cleaned", exist_ok=True)
                path = f"data/cleaned/{self.get_dataset_name()}.csv"
            if not os.path.exists(self.db_path):
                self.logger.warning(
                    f"Database not found at {self.db_path}. No data to export."
                )
                return None
            conn = sqlite3.connect(self.db_path)
            cur  = conn.cursor()
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (self.get_dataset_name(),),
            )
            if cur.fetchone() is None:
                self.logger.warning(
                    f"Table '{self.get_dataset_name()}' does not exist — "
                    "crawler returned 0 records, nothing to export."
                )
                conn.close()
                return None
            cur.execute(f'SELECT * FROM "{self.get_dataset_name()}"')
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            conn.close()
            if not rows:
                self.logger.warning("Table exists but has 0 records — CSV not written.")
                return None
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(cols)
                w.writerows(rows)
            self.logger.info(f"Exported {len(rows)} records to {path}")
            return path


# ---------------------------------------------------------------------------
# PeeringDB API constants
# ---------------------------------------------------------------------------

_API_BASE  = "https://www.peeringdb.com/api"
_FAC_URL   = f"{_API_BASE}/fac"
_PAGE_SIZE = 250       # PeeringDB hard cap per request

_HEADERS = {
    "User-Agent": (
        "FirmablePipelineBot/1.0 "
        "(B2B research crawler; contact: research@firmable.com)"
    ),
    "Accept": "application/json",
}

# ---------------------------------------------------------------------------
# Operator-name extraction helpers
# ---------------------------------------------------------------------------

# Known multi-word brand prefixes — checked before any regex stripping
_KNOWN_BRANDS = [
    "Equinix", "Digital Realty", "CyrusOne", "CoreSite", "Iron Mountain",
    "QTS", "Switch", "DataBank", "Databank", "Zayo", "NTT", "Flexential",
    "TierPoint", "Peak 10", "Peak10", "Cologix", "Aligned", "EdgeCore",
    "EdgeConneX", "Verizon", "AT&T", "CenturyLink", "Lumen", "Cogent",
    "Hurricane Electric", "Internap", "Navisite", "Windstream",
    "GTT", "Latisys", "ViaWest", "Expedient", "WilTel",
    "Markley", "Sabey", "RagingWire", "T5",
    "Google", "Amazon", "Microsoft", "Meta", "Apple", "Oracle",
]

# Right-side patterns to strip from facility_name -> operator brand
# Tried in order; first match wins
_STRIP_PATTERNS = [
    # "Equinix DA1 - Dallas"  or  "CoreSite - Denver CO1"
    re.compile(r"\s+[A-Z]{1,4}\d{1,3}[A-Z]?\s*-.*$"),
    # " - City" or " - City, State"
    re.compile(r"\s+-\s+[A-Za-z][\w\s,\.]+$"),
    # "Digital Realty SFO (200 Paul)"  -> strip "(xxx)"
    re.compile(r"\s+\([^)]*\)\s*$"),
    # "CoreSite Denver CO1" -> strip trailing site code
    re.compile(r"\s+[A-Z]{2,4}\d{1,3}[A-Z]?\s*$"),
    # Trailing city / state word(s)
    re.compile(
        r"\s+(Ashburn|Dallas|Chicago|Miami|Atlanta|Phoenix|Denver|Seattle|"
        r"Boston|Austin|Newark|Charlotte|Columbus|Portland|Nashville|"
        r"Raleigh|Richmond|Houston|Minneapolis|Cincinnati|Louisville|"
        r"Indianapolis|Tampa|Orlando|Pittsburgh|Sacramento|Boise|"
        r"San Jose|San Francisco|Los Angeles|New York|Las Vegas|"
        r"Salt Lake City|Omaha|Memphis|Virginia|California|Texas|"
        r"Florida|Ohio|Georgia|Illinois|New Jersey).*$",
        re.IGNORECASE,
    ),
]


def _derive_org_name(facility_name: str, embedded_org: str) -> str:
    """
    Return the best operator/company name for a facility record.

    Priority:
      1. Embedded org.name if it genuinely differs from facility_name
      2. Known brand prefix match
      3. Progressive right-side strip of site-code / city suffix
      4. Raw facility_name as last resort
    """
    fn = facility_name.strip()
    eo = embedded_org.strip() if embedded_org else ""

    # 1. Use PeeringDB-provided org name when it's distinct
    if eo and eo.lower() != fn.lower():
        return eo

    # 2. Known brand match (longest match first to avoid "QTS" matching "QTS Realty")
    sorted_brands = sorted(_KNOWN_BRANDS, key=len, reverse=True)
    fn_lower = fn.lower()
    for brand in sorted_brands:
        if fn_lower.startswith(brand.lower()):
            return brand

    # 3. Strip site-code / city suffixes
    candidate = fn
    for pat in _STRIP_PATTERNS:
        trimmed = pat.sub("", candidate).strip().rstrip(" ,;-")
        if trimmed and len(trimmed) >= 3 and trimmed != candidate:
            candidate = trimmed
            break

    if candidate and candidate != fn:
        return candidate

    return fn


def _str(v) -> str:
    """Safely convert any value to a stripped string."""
    if v is None:
        return ""
    if isinstance(v, (list, dict)):
        return str(v)
    return str(v).strip()


# ---------------------------------------------------------------------------
class DataCenterMapCrawler(BaseCrawler):
    """
    Crawler #14 - US Colocation & Data Center Facilities via PeeringDB API.

    Crawl strategy
    --------------
    PeeringDB limits list responses to 250 rows. We paginate using
    ?limit=250&skip=N until the page comes back empty, collecting every
    US facility. At depth=2 each record includes its parent org object,
    so no per-record detail requests are needed.

    Expected yield: ~1,600 - 2,000+ verified US data center records.
    """

    def get_dataset_name(self) -> str:
        return "peeringdb_us_facilities"   

    def get_source_url(self) -> str:
        return "https://www.peeringdb.com/api/fac?country=US&status=ok"

    def get_niche_fields(self) -> List[str]:
        return [
            "facility_name", "org_name", "colo_url",
            "tech_email", "tech_phone", "sales_email", "sales_phone",
            "latitude", "longitude", "npanxx",
            "net_count", "ix_count",
            "diverse_serving_substations", "available_voltage_services",
            "property_class", "notes",
            "peeringdb_id", "peeringdb_url",
        ]

    # ---------------------------------------------------------- Phase 1
    def crawl(self) -> List[Dict]:
        """Paginate through all US facilities in PeeringDB."""
        all_facilities: List[Dict] = []
        skip = 0
        page = 1

        while True:
            params = {
                "country": "US",
                "status":  "ok",
                "depth":   2,
                "limit":   _PAGE_SIZE,
                "skip":    skip,
            }
            self.logger.info(
                f"  Page {page}: requesting skip={skip}, limit={_PAGE_SIZE} ..."
            )
            resp = self._safe_request(_FAC_URL, params=params, headers=_HEADERS)
            if not resp:
                self.logger.error(f"  No response on page {page} — stopping")
                break

            batch = resp.json().get("data", [])
            if not batch:
                self.logger.info("  Empty page — pagination complete")
                break

            all_facilities.extend(batch)
            self.logger.info(
                f"  Page {page}: +{len(batch)} records "
                f"| running total: {len(all_facilities)}"
            )

            if len(batch) < _PAGE_SIZE:
                break   # final partial page

            skip += _PAGE_SIZE
            page += 1
            self._rate_limit(0.5)   # 0.5 s courtesy delay between pages

        self.logger.info(
            f"  Done — {len(all_facilities)} US facilities fetched"
        )
        if not all_facilities:
            raise RuntimeError(
                "PeeringDB returned 0 facilities — "
                "check API availability or rate-limit status."
            )
        return all_facilities

    # ---------------------------------------------------------- Phase 2
    def parse(self, raw_facilities: List[Dict]) -> List[Dict]:
        records = []
        total   = len(raw_facilities)
        for idx, fac in enumerate(raw_facilities, 1):
            if idx % 250 == 0:
                self.logger.info(f"  Parsing {idx}/{total} ...")
            rec = self._parse_facility(fac)
            if rec:
                records.append(rec)
        self.logger.info(f"  Parsing complete — {len(records)} valid records")
        return records

    # ---------------------------------------------------------- field parser
    def _parse_facility(self, fac: Dict) -> Dict:
        fac_id   = fac.get("id", "")
        fac_name = _str(fac.get("name", ""))
        if not fac_name:
            return {}

        org_obj   = fac.get("org") or {}
        embed_org = _str(org_obj.get("name", "")) if isinstance(org_obj, dict) else ""
        org_name  = _derive_org_name(fac_name, embed_org)

        addr1   = _str(fac.get("address1", ""))
        addr2   = _str(fac.get("address2", ""))
        address = ", ".join(filter(None, [addr1, addr2]))

        tech_phone  = _str(fac.get("tech_phone",  ""))
        sales_phone = _str(fac.get("sales_phone", ""))
        tech_email  = _str(fac.get("tech_email",  ""))
        sales_email = _str(fac.get("sales_email", ""))

        website = _str(fac.get("website", "")) or _str(fac.get("colo_url", ""))

        div_sub = fac.get("diverse_serving_substations")
        div_sub = "Yes" if div_sub is True else ("No" if div_sub is False else "")

        volt = fac.get("available_voltage_services")
        volt = "; ".join(str(v) for v in volt if v) if isinstance(volt, list) else _str(volt)

        prop = fac.get("property_class")
        prop = "; ".join(str(v) for v in prop if v) if isinstance(prop, list) else _str(prop)

        notes = _str(fac.get("notes", ""))
        if len(notes) > 500:
            notes = notes[:497] + "..."

        pdb_id  = str(fac_id)
        pdb_url = f"https://www.peeringdb.com/fac/{pdb_id}" if pdb_id else ""

        return {
            # Core B2B fields
            "company_name": org_name,
            "website":      website,
            "phone":        tech_phone or sales_phone,
            "email":        tech_email or sales_email,
            "address":      address,
            "city":         _str(fac.get("city",    "")),
            "state":        _str(fac.get("state",   "")).upper(),
            "zip":          _str(fac.get("zipcode", "")),
            "country":      _str(fac.get("country", "US")),
            # Niche-specific fields
            "facility_name":               fac_name,
            "org_name":                    org_name,
            "colo_url":                    _str(fac.get("colo_url", "")),
            "tech_email":                  tech_email,
            "tech_phone":                  tech_phone,
            "sales_email":                 sales_email,
            "sales_phone":                 sales_phone,
            "latitude":                    _str(fac.get("latitude",  "")),
            "longitude":                   _str(fac.get("longitude", "")),
            "npanxx":                      _str(fac.get("npanxx",    "")),
            "net_count":                   _str(fac.get("net_count", "")),
            "ix_count":                    _str(fac.get("ix_count",  "")),
            "diverse_serving_substations": div_sub,
            "available_voltage_services":  volt,
            "property_class":              prop,
            "notes":                       notes,
            "peeringdb_id":                pdb_id,
            "peeringdb_url":               pdb_url,
        }


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    crawler  = DataCenterMapCrawler(db_path="data/firmable.db", log_dir="logs")
    stats    = crawler.run()

    print("\n-- Final Stats --")
    for k, v in stats.items():
        print(f"  {k:<28} {v}")

    csv_path = crawler.export_csv()
    if csv_path:
        print(f"\n  CSV exported -> {csv_path}")
    else:
        print("\n  No CSV exported (0 records or export failed)")