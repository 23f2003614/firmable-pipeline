"""
Crawler #10: PHCC — Plumbing-Heating-Cooling Contractors Association
Source  : https://www.phccweb.org/tools-resources/find-a-contractor/
Method  : WordPress AJAX endpoint (action=phcc_contractor_finder) queried
          by zip code across all 51 US state-capital zip codes.
          Each response is a JSON list of contractor objects.

"""

import sys, os, re, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from crawlers.base_crawler import BaseCrawler
except ModuleNotFoundError:
    # Allow running standalone — minimal base shim
    import hashlib, csv, sqlite3, logging, io
    from abc import ABC, abstractmethod
    from datetime import datetime, timezone
    from typing import List, Dict, Any, Tuple

    class BaseCrawler(ABC):
        def __init__(self, db_path="data/firmable.db", log_dir="logs"):
            self.db_path   = db_path
            self.log_dir   = log_dir
            self.session_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            self.stats = dict(total_parsed=0, total_new=0, total_updated=0,
                              total_duplicates=0, total_errors=0)
            self._setup_logging()

        def _setup_logging(self):
            os.makedirs(self.log_dir, exist_ok=True)
            log_file = os.path.join(self.log_dir,
                f"{self.get_dataset_name()}_{self.session_id}.log")
            self.logger = logging.getLogger(self.get_dataset_name())
            self.logger.setLevel(logging.DEBUG)
            if self.logger.handlers:
                return
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
            self.logger.addHandler(fh)
            try:
                stream = io.TextIOWrapper(
                    sys.stdout.buffer, encoding="utf-8",
                    errors="replace", line_buffering=True)
            except AttributeError:
                stream = sys.stdout
            ch = logging.StreamHandler(stream)
            ch.setLevel(logging.INFO)
            ch.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
            self.logger.addHandler(ch)

        @abstractmethod
        def get_dataset_name(self): pass
        @abstractmethod
        def get_source_url(self): pass
        @abstractmethod
        def get_niche_fields(self): pass
        @abstractmethod
        def crawl(self): pass
        @abstractmethod
        def parse(self, raw_data): pass

        def run(self):
            self.logger.info("=" * 55)
            self.logger.info(f"STARTING: {self.get_dataset_name()}")
            self.logger.info(f"Source  : {self.get_source_url()}")
            self.logger.info("=" * 55)
            try:
                raw   = self.crawl()
                recs  = self.parse(raw)
                self.stats["total_parsed"] = len(recs)
                clean = [self._clean(r) for r in recs if r.get("company_name")]
                uniq  = self._deduplicate(clean)
                self.stats["total_duplicates"] = len(clean) - len(uniq)
                new, upd = self._store(uniq)
                self.stats["total_new"]     = new
                self.stats["total_updated"] = upd
            except Exception as e:
                self.stats["total_errors"] += 1
                self.logger.error(f"FAILED: {e}", exc_info=True)
                raise
            self.logger.info(
                f"DONE — new: {self.stats['total_new']:,}  "
                f"updated: {self.stats['total_updated']:,}")
            return self.stats

        def _clean(self, record):
            cleaned = {}
            for key, val in record.items():
                if isinstance(val, str):
                    val = " ".join(val.strip().split())
                    if val.lower() in ("n/a", "na", "none", "null", "-", ""):
                        val = None
                cleaned[key] = val
            if cleaned.get("phone"):
                phone = "".join(
                    c for c in cleaned["phone"] if c.isdigit() or c in "+-(). ")
                cleaned["phone"] = phone.strip() or None
            if cleaned.get("website") and not str(cleaned["website"]).startswith("http"):
                cleaned["website"] = "https://" + cleaned["website"]
            if cleaned.get("email"):
                cleaned["email"] = str(cleaned["email"]).lower()
            cleaned["dataset"]     = self.get_dataset_name()
            cleaned["source_url"]  = self.get_source_url()
            cleaned["crawl_date"]  = datetime.now(timezone.utc).isoformat()
            cleaned["crawl_session"] = self.session_id
            return cleaned

        def _deduplicate(self, records):
            seen, unique = set(), []
            for rec in records:
                raw_key = (
                    (rec.get("company_name") or "").lower() +
                    (rec.get("phone") or "").lower() +          # <-- phone added
                    (rec.get("address") or "").lower() +
                    self.get_dataset_name()
                )
                h = hashlib.md5(raw_key.encode()).hexdigest()
                if h not in seen:
                    seen.add(h)
                    rec["dedup_hash"] = h
                    unique.append(rec)
            return unique

        def _store(self, records):
            if not records:
                return 0, 0
            os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
            conn   = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            table  = self.get_dataset_name()
            columns = list(records[0].keys())
            col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS "{table}" (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    {col_defs},
                    first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_seen  TEXT DEFAULT CURRENT_TIMESTAMP,
                    is_new     INTEGER DEFAULT 1
                )
            ''')
            cursor.execute(
                f'CREATE INDEX IF NOT EXISTS "idx_{table}_hash" '
                f'ON "{table}" (dedup_hash)')
            new_count = upd_count = 0
            for rec in records:
                cursor.execute(
                    f'SELECT id FROM "{table}" WHERE dedup_hash = ?',
                    (rec["dedup_hash"],))
                existing = cursor.fetchone()
                if existing:
                    sets = ", ".join(f'"{k}" = ?' for k in rec)
                    cursor.execute(
                        f'UPDATE "{table}" SET {sets}, '
                        f'last_seen = CURRENT_TIMESTAMP, is_new = 0 '
                        f'WHERE dedup_hash = ?',
                        list(rec.values()) + [rec["dedup_hash"]])
                    upd_count += 1
                else:
                    cols = ", ".join(f'"{k}"' for k in rec)
                    vals = ", ".join("?" * len(rec))
                    cursor.execute(
                        f'INSERT INTO "{table}" ({cols}) VALUES ({vals})',
                        list(rec.values()))
                    new_count += 1
            conn.commit()
            conn.close()
            return new_count, upd_count

        def _safe_request(self, url, params=None, headers=None, max_retries=3):
            import requests
            if not headers:
                headers = {
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
                    "Accept": ("text/html,application/xhtml+xml,"
                               "application/xml;q=0.9,*/*;q=0.8"),
                    "Accept-Language": "en-US,en;q=0.5",
                }
            for attempt in range(max_retries):
                try:
                    resp = requests.get(url, params=params,
                                        headers=headers, timeout=30)
                    resp.raise_for_status()
                    return resp
                except Exception as e:
                    self.logger.warning(
                        f"Request failed (attempt {attempt+1}/{max_retries}): {e}")
                    time.sleep(2 ** attempt)
            return None

        def _rate_limit(self, seconds=1.0):
            time.sleep(seconds)

        def export_csv(self, path=None):
            if not path:
                os.makedirs("data/cleaned", exist_ok=True)
                path = f"data/cleaned/{self.get_dataset_name()}.csv"
            if not os.path.exists(self.db_path):
                self.logger.warning("DB not found — nothing to export.")
                return None
            conn   = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (self.get_dataset_name(),))
                if not cursor.fetchone():
                    self.logger.warning("Table does not exist — 0 records.")
                    conn.close()
                    return None
                cursor.execute(f'SELECT * FROM "{self.get_dataset_name()}"')
                columns = [d[0] for d in cursor.description]
                rows    = cursor.fetchall()
            except Exception as e:
                self.logger.error(f"Export failed: {e}")
                rows = []
            finally:
                conn.close()
            if not rows:
                self.logger.warning("0 records — CSV not written.")
                return None
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(columns)
                w.writerows(rows)
            self.logger.info(f"Exported {len(rows):,} records to {path}")
            return path


# ────────────────────────────────────────────────────────────────────────────
# Area-code → US State lookup  (comprehensive, covers all NANP US area codes)
# ────────────────────────────────────────────────────────────────────────────
AREA_CODE_STATE: dict[str, str] = {
    # Alabama
    "205": "AL", "251": "AL", "256": "AL", "334": "AL", "938": "AL",
    # Alaska
    "907": "AK",
    # Arizona
    "480": "AZ", "520": "AZ", "602": "AZ", "623": "AZ", "928": "AZ",
    # Arkansas
    "479": "AR", "501": "AR", "870": "AR",
    # California
    "209": "CA", "213": "CA", "279": "CA", "310": "CA", "323": "CA",
    "341": "CA", "408": "CA", "415": "CA", "424": "CA", "442": "CA",
    "510": "CA", "530": "CA", "559": "CA", "562": "CA", "619": "CA",
    "626": "CA", "628": "CA", "650": "CA", "657": "CA", "661": "CA",
    "669": "CA", "707": "CA", "714": "CA", "747": "CA", "760": "CA",
    "805": "CA", "818": "CA", "820": "CA", "831": "CA", "840": "CA",
    "858": "CA", "909": "CA", "916": "CA", "925": "CA", "949": "CA", "951": "CA",
    # Colorado
    "303": "CO", "719": "CO", "720": "CO", "970": "CO",
    # Connecticut
    "203": "CT", "475": "CT", "860": "CT", "959": "CT",
    # Delaware
    "302": "DE",
    # DC
    "202": "DC",
    # Florida
    "239": "FL", "305": "FL", "321": "FL", "352": "FL", "386": "FL",
    "407": "FL", "448": "FL", "561": "FL", "656": "FL", "689": "FL",
    "727": "FL", "754": "FL", "772": "FL", "786": "FL", "813": "FL",
    "850": "FL", "863": "FL", "904": "FL", "941": "FL", "954": "FL",
    # Georgia
    "229": "GA", "404": "GA", "470": "GA", "478": "GA", "678": "GA",
    "706": "GA", "762": "GA", "770": "GA", "912": "GA", "943": "GA",
    # Hawaii
    "808": "HI",
    # Idaho
    "208": "ID", "986": "ID",
    # Illinois
    "217": "IL", "224": "IL", "309": "IL", "312": "IL", "331": "IL",
    "447": "IL", "618": "IL", "630": "IL", "708": "IL", "773": "IL",
    "779": "IL", "815": "IL", "847": "IL", "872": "IL",
    # Indiana
    "219": "IN", "260": "IN", "317": "IN", "463": "IN", "574": "IN",
    "765": "IN", "812": "IN", "930": "IN",
    # Iowa
    "319": "IA", "515": "IA", "563": "IA", "641": "IA", "712": "IA",
    # Kansas
    "316": "KS", "620": "KS", "785": "KS", "913": "KS",
    # Kentucky
    "270": "KY", "364": "KY", "502": "KY", "606": "KY", "859": "KY",
    # Louisiana
    "225": "LA", "318": "LA", "337": "LA", "504": "LA", "985": "LA",
    # Maine
    "207": "ME",
    # Maryland
    "240": "MD", "301": "MD", "410": "MD", "443": "MD", "667": "MD",
    # Massachusetts
    "339": "MA", "351": "MA", "413": "MA", "508": "MA", "617": "MA",
    "774": "MA", "781": "MA", "857": "MA", "978": "MA",
    # Michigan
    "231": "MI", "248": "MI", "269": "MI", "313": "MI", "517": "MI",
    "586": "MI", "616": "MI", "679": "MI", "734": "MI", "810": "MI",
    "906": "MI", "947": "MI", "989": "MI",
    # Minnesota
    "218": "MN", "320": "MN", "507": "MN", "612": "MN", "651": "MN",
    "763": "MN", "952": "MN",
    # Mississippi
    "228": "MS", "601": "MS", "662": "MS", "769": "MS",
    # Missouri
    "314": "MO", "417": "MO", "557": "MO", "573": "MO", "636": "MO",
    "660": "MO", "816": "MO",
    # Montana
    "406": "MT",
    # Nebraska
    "308": "NE", "402": "NE", "531": "NE",
    # Nevada
    "702": "NV", "725": "NV", "775": "NV",
    # New Hampshire
    "603": "NH",
    # New Jersey
    "201": "NJ", "551": "NJ", "609": "NJ", "640": "NJ", "732": "NJ",
    "848": "NJ", "856": "NJ", "862": "NJ", "908": "NJ", "973": "NJ",
    # New Mexico
    "505": "NM", "575": "NM",
    # New York
    "212": "NY", "315": "NY", "332": "NY", "347": "NY", "516": "NY",
    "518": "NY", "585": "NY", "607": "NY", "631": "NY", "646": "NY",
    "680": "NY", "716": "NY", "718": "NY", "838": "NY", "845": "NY",
    "914": "NY", "917": "NY", "929": "NY", "934": "NY",
    # North Carolina
    "252": "NC", "336": "NC", "704": "NC", "743": "NC", "828": "NC",
    "910": "NC", "919": "NC", "980": "NC", "984": "NC",
    # North Dakota
    "701": "ND",
    # Ohio
    "216": "OH", "220": "OH", "234": "OH", "283": "OH", "330": "OH",
    "380": "OH", "419": "OH", "440": "OH", "513": "OH", "567": "OH",
    "614": "OH", "740": "OH", "937": "OH",
    # Oklahoma
    "405": "OK", "539": "OK", "572": "OK", "580": "OK", "918": "OK",
    # Oregon
    "458": "OR", "503": "OR", "541": "OR", "971": "OR",
    # Pennsylvania
    "215": "PA", "223": "PA", "267": "PA", "272": "PA", "412": "PA",
    "484": "PA", "570": "PA", "582": "PA", "610": "PA", "717": "PA",
    "724": "PA", "814": "PA", "878": "PA",
    # Rhode Island
    "401": "RI",
    # South Carolina
    "803": "SC", "839": "SC", "843": "SC", "854": "SC", "864": "SC",
    # South Dakota
    "605": "SD",
    # Tennessee
    "423": "TN", "615": "TN", "629": "TN", "731": "TN", "865": "TN",
    "901": "TN", "931": "TN",
    # Texas
    "210": "TX", "214": "TX", "254": "TX", "281": "TX", "325": "TX",
    "346": "TX", "361": "TX", "409": "TX", "430": "TX", "432": "TX",
    "469": "TX", "512": "TX", "682": "TX", "713": "TX", "726": "TX",
    "737": "TX", "806": "TX", "817": "TX", "830": "TX", "832": "TX",
    "903": "TX", "915": "TX", "936": "TX", "940": "TX", "956": "TX",
    "972": "TX", "979": "TX",
    # Utah
    "385": "UT", "435": "UT", "801": "UT",
    # Vermont
    "802": "VT",
    # Virginia
    "276": "VA", "434": "VA", "540": "VA", "571": "VA", "703": "VA",
    "757": "VA", "804": "VA",
    # Washington
    "206": "WA", "253": "WA", "360": "WA", "425": "WA", "509": "WA",
    "564": "WA",
    # West Virginia
    "304": "WV", "681": "WV",
    # Wisconsin
    "262": "WI", "414": "WI", "534": "WI", "608": "WI", "715": "WI",
    "920": "WI",
    # Wyoming
    "307": "WY",
    # Toll-free (not state-specific)
    "800": "US", "833": "US", "844": "US",
    "855": "US", "866": "US", "877": "US", "888": "US",
}

# Full state-name lookup
STATE_NAMES: dict[str, str] = {
    "AL": "Alabama",        "AK": "Alaska",       "AZ": "Arizona",
    "AR": "Arkansas",       "CA": "California",   "CO": "Colorado",
    "CT": "Connecticut",    "DE": "Delaware",      "DC": "District of Columbia",
    "FL": "Florida",        "GA": "Georgia",       "HI": "Hawaii",
    "ID": "Idaho",          "IL": "Illinois",      "IN": "Indiana",
    "IA": "Iowa",           "KS": "Kansas",        "KY": "Kentucky",
    "LA": "Louisiana",      "ME": "Maine",         "MD": "Maryland",
    "MA": "Massachusetts",  "MI": "Michigan",      "MN": "Minnesota",
    "MS": "Mississippi",    "MO": "Missouri",      "MT": "Montana",
    "NE": "Nebraska",       "NV": "Nevada",        "NH": "New Hampshire",
    "NJ": "New Jersey",     "NM": "New Mexico",    "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota",  "OH": "Ohio",
    "OK": "Oklahoma",       "OR": "Oregon",        "PA": "Pennsylvania",
    "RI": "Rhode Island",   "SC": "South Carolina","SD": "South Dakota",
    "TN": "Tennessee",      "TX": "Texas",         "UT": "Utah",
    "VT": "Vermont",        "VA": "Virginia",      "WA": "Washington",
    "WV": "West Virginia",  "WI": "Wisconsin",     "WY": "Wyoming",
    "US": "United States",
}

# One representative zip per state for API queries
STATE_ZIPS: list[tuple[str, str]] = [
    ("AL", "36104"), ("AK", "99501"), ("AZ", "85001"), ("AR", "72201"),
    ("CA", "94203"), ("CO", "80201"), ("CT", "06101"), ("DE", "19901"),
    ("DC", "20001"), ("FL", "32301"), ("GA", "30301"), ("HI", "96801"),
    ("ID", "83701"), ("IL", "62701"), ("IN", "46201"), ("IA", "50301"),
    ("KS", "66601"), ("KY", "40601"), ("LA", "70801"), ("ME", "04330"),
    ("MD", "21401"), ("MA", "02101"), ("MI", "48901"), ("MN", "55101"),
    ("MS", "39201"), ("MO", "65101"), ("MT", "59601"), ("NE", "68501"),
    ("NV", "89701"), ("NH", "03301"), ("NJ", "08601"), ("NM", "87501"),
    ("NY", "12201"), ("NC", "27601"), ("ND", "58501"), ("OH", "43201"),
    ("OK", "73101"), ("OR", "97301"), ("PA", "17101"), ("RI", "02901"),
    ("SC", "29201"), ("SD", "57501"), ("TN", "37201"), ("TX", "78701"),
    ("UT", "84101"), ("VT", "05601"), ("VA", "23218"), ("WA", "98501"),
    ("WV", "25301"), ("WI", "53701"), ("WY", "82001"),
]

# ─── Keyword-based specialty detection ──────────────────────────────────────

_SPECIALTY_KEYWORDS: list[tuple[str, str]] = [
    # Most-specific first
    (r"\bboiler\b",                             "Boiler Services"),
    (r"\bradiant\b",                            "Radiant Heating"),
    (r"\bheat\s*pump",                          "Heat Pump"),
    (r"\bgeotherm",                             "Geothermal HVAC"),
    (r"\bsolar\b",                              "Solar Thermal"),
    (r"\bchiller\b",                            "Commercial Chillers"),
    (r"\bduct\b|ductwork|sheet\s*metal",        "Ductwork & Sheet Metal"),
    (r"\bfire\s*suppress|sprinkler",            "Fire Suppression"),
    (r"\bbackflow",                             "Backflow Prevention"),
    (r"\bwater\s*treat|softener|filtrat",       "Water Treatment"),
    (r"\bwell\b|well\s*pump",                   "Well & Pump Services"),
    (r"\bseptic\b|drain\s*field",               "Septic Systems"),
    (r"\bsewer\b|drain\b",                      "Sewer & Drain"),
    (r"\bgas\s*line|natural\s*gas|propane",     "Gas Piping"),
    (r"\bsteam\b",                              "Steam Heating"),
    (r"\brefrig",                               "Refrigeration"),
    (r"\bcommercial\b",                         "Commercial Plumbing/HVAC"),
    (r"\bindustrial\b",                         "Industrial Mechanical"),
    (r"\bplumb",                                "Plumbing"),
    (r"\bhvac|heat.*cool|cool.*heat|air\s*cond|a/?c\b|heating\s*&?\s*cool",
                                                "HVAC"),
    (r"\bheat",                                 "Heating"),
    (r"\bcool|air",                             "Cooling & Air"),
    (r"\bmechan",                               "Mechanical Contractor"),
]

_SERVICES_KEYWORDS: list[tuple[str, str]] = [
    (r"\bplumb",       "Plumbing"),
    (r"\bheating\b|htg\b|heat\b", "Heating"),
    (r"\bcooling\b|cool\b", "Cooling"),
    (r"\bhvac\b",      "HVAC"),
    (r"\bboiler\b",    "Boilers"),
    (r"\bgas\b",       "Gas Piping"),
    (r"\bduct\b",      "Ductwork"),
    (r"\bsewer\b",     "Sewer"),
    (r"\bdrain\b",     "Drains"),
    (r"\bseptic\b",    "Septic"),
    (r"\belectric\b",  "Electrical"),
    (r"\bfire\b",      "Fire Suppression"),
    (r"\bwater\s*heat","Water Heating"),
    (r"\brefrig",      "Refrigeration"),
    (r"\bair\s*qual",  "Air Quality"),
    (r"\bmechan",      "Mechanical"),
    (r"\bwell\b",      "Well Services"),
]

_COMMERCIAL_KEYWORDS = re.compile(
    r"\bcommercial\b|\bindustrial\b|\bmechanical\b|\bcontracting\b"
    r"|\bcorp\b|\binc\b|\bllc\b|\bco\.\b|\bcompany\b|\bservice[s]?\b",
    re.IGNORECASE)

_RESIDENTIAL_KEYWORDS = re.compile(
    r"\bhome\b|\bhouse\b|\bresidential\b|\bfamily\b|\bneighbor",
    re.IGNORECASE)

_EMERGENCY_KEYWORDS = re.compile(
    r"\bemergency\b|\b24[/-]7\b|\b24\s*hour|\banytime\b|\bafter.?hour",
    re.IGNORECASE)

_BAD_URL_PREFIX = re.compile(
    r"^https?://www\.phccweb\.org/[^?#]*(https?://)",
    re.IGNORECASE)


def _infer_specialty(name: str) -> str:
    n = name.lower()
    for pattern, label in _SPECIALTY_KEYWORDS:
        if re.search(pattern, n, re.IGNORECASE):
            return label
    return "Plumbing/HVAC"


def _infer_services(name: str) -> str:
    """Return a comma-separated list of inferred services."""
    found = []
    n = name.lower()
    for pattern, label in _SERVICES_KEYWORDS:
        if re.search(pattern, n, re.IGNORECASE) and label not in found:
            found.append(label)
    # If nothing detected, default to Plumbing/HVAC
    return ", ".join(found) if found else "Plumbing, HVAC"


def _infer_market_segment(name: str) -> str:
    has_comm = bool(_COMMERCIAL_KEYWORDS.search(name))
    has_res  = bool(_RESIDENTIAL_KEYWORDS.search(name))
    if has_comm and has_res:
        return "Commercial & Residential"
    if has_res:
        return "Residential"
    # Most PHCC members serve both; only flag purely residential if clear
    return "Commercial & Residential"


def _infer_emergency(name: str) -> str:
    return "Yes" if _EMERGENCY_KEYWORDS.search(name) else "Unknown"


def _fix_website(raw: str) -> str | None:
    """
    Strip the erroneous page-URL prefix that the v1 parser introduced.
    e.g. "https://www.phccweb.org/tools-resources/find-a-contractor/http://foo.com"
         → "http://foo.com"
    """
    if not raw:
        return None
    if "phccweb.org" in raw:
        import re as _re
        m = _re.search(r"(https?://(?!www\.phccweb\.org)[^\s]+)", raw, _re.IGNORECASE)
        if m:
            return m.group(1)
        return None
    if raw.startswith("/") or (not raw.startswith("http") and "." in raw):
        return "https://" + raw.lstrip("/")
    return raw or None


def _state_from_phone(phone: str) -> str | None:
    digits = re.sub(r"[^\d]", "", phone or "")
    if len(digits) < 10:
        return None
    ac = digits[:3] if not digits.startswith("1") else digits[1:4]
    return AREA_CODE_STATE.get(ac)


# ────────────────────────────────────────────────────────────────────────────

class PhccContractorCrawler(BaseCrawler):
    """
    PHCC — Plumbing-Heating-Cooling Contractors Association member directory.

    API endpoint: POST https://www.phccweb.org/wp-admin/admin-ajax.php
    Form params : action=phcc_contractor_finder  &  zipCode=<zip>

    The endpoint returns a JSON list; each object may include:
      org_name, cst_phn_number_complete_dn, cst_url_code_dn,
      cst_adr_line1_dn, cst_city_dn, cst_state_dn, cst_zip_dn,
      cst_eml_address_dn, chapter, membership_type, specialty, …

    Results are paged by zip: we send one request per state capital zip,
    which triggers a radius search returning all members in that state.
    A global seen-IDs set ensures we never write duplicates even if a
    contractor appears in multiple radius responses.
    """

    PAGE_URL = "https://www.phccweb.org/tools-resources/find-a-contractor/"
    AJAX_URL = "https://www.phccweb.org/wp-admin/admin-ajax.php"

    # ── Identity ──────────────────────────────────────────────────────────
    def get_dataset_name(self): return "phcc_plumbing_hvac_contractors"
    def get_source_url(self):   return self.PAGE_URL
    def get_niche_fields(self):
        return [
            "trade_specialty",
            "services_offered",
            "commercial_residential",
            "emergency_service",
            "phcc_chapter",
            "service_area",
            "membership_type",
            "license_state",
        ]

    # ── Crawl ─────────────────────────────────────────────────────────────
    def crawl(self):
        session = self._make_session()

        # Warm-up: load page to capture cookies / any session tokens
        self.logger.info("Warming up session via page load …")
        try:
            session.get(self.PAGE_URL, timeout=30)
        except Exception as e:
            self.logger.warning(f"Warm-up failed (non-fatal): {e}")

        all_results: list[dict] = []
        seen_ids: set[str]      = set()

        for state_abbr, zipcode in STATE_ZIPS:
            self.logger.info(f"[{state_abbr}] querying zip={zipcode} …")
            batch = self._fetch_by_zip(session, zipcode, state_abbr)

            added = 0
            for item in batch:
                uid = self._item_uid(item)
                if uid not in seen_ids:
                    seen_ids.add(uid)
                    item["_query_state"] = state_abbr   # tag for fallback use
                    all_results.append(item)
                    added += 1

            self.logger.info(
                f"  → {len(batch)} returned, {added} new  "
                f"(running total: {len(all_results):,})")
            self._rate_limit(1.2)   # polite delay between state queries

        self.logger.info(f"Total unique raw records: {len(all_results):,}")
        return all_results

    # ── Session factory ───────────────────────────────────────────────────
    def _make_session(self):
        try:
            from curl_cffi import requests as cffi_requests
            session = cffi_requests.Session(impersonate="chrome124")
            self.logger.info("Using curl_cffi (Chrome TLS fingerprint)")
            return session
        except ImportError:
            pass
        import requests
        session = requests.Session()
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"),
            "Accept":          "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer":         self.PAGE_URL,
        })
        return session

    # ── Fetch one zip ─────────────────────────────────────────────────────
    def _fetch_by_zip(self, session, zipcode: str, state: str) -> list[dict]:
        try:
            resp = session.post(
                self.AJAX_URL,
                data={"action": "phcc_contractor_finder", "zipCode": zipcode},
                headers={
                    "Content-Type":    "application/x-www-form-urlencoded",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer":          self.PAGE_URL,
                },
                timeout=35,
            )
        except Exception as e:
            self.logger.warning(f"  [{state}] POST failed: {e}")
            return []

        if not resp.ok:
            self.logger.warning(f"  [{state}] HTTP {resp.status_code}")
            return []

        try:
            data = resp.json()
        except Exception:
            self.logger.warning(f"  [{state}] Non-JSON response")
            return []

        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("contractors", "results", "data", "locations", "members"):
                if isinstance(data.get(key), list):
                    return data[key]
        return []

    # ── Unique ID for dedup during crawl ─────────────────────────────────
    @staticmethod
    def _item_uid(item: dict) -> str:
        name  = (item.get("org_name") or item.get("company_name") or
                 item.get("name") or item.get("cst_name_dn") or "").lower().strip()
        phone = re.sub(r"[^\d]", "",
                       item.get("cst_phn_number_complete_dn") or
                       item.get("phone") or "")
        cid   = str(item.get("id") or item.get("cst_key") or "")
        return cid or (name + phone) or name

    # ── Parse ─────────────────────────────────────────────────────────────
    def parse(self, raw_data: list) -> list[dict]:
        if not raw_data:
            return []

        records = []
        for item in raw_data:
            if not isinstance(item, dict):
                continue

            # ── Company name ──────────────────────────────────────────────
            name = (
                item.get("org_name")     or item.get("company_name") or
                item.get("name")         or item.get("cst_name_dn")  or
                item.get("title")        or ""
            ).strip()
            if not name:
                continue

            # ── Phone ─────────────────────────────────────────────────────
            phone = (
                item.get("cst_phn_number_complete_dn") or
                item.get("phone") or item.get("tel") or ""
            ).strip()

            # ── Website — fix the prepended-URL bug ───────────────────────
            raw_web = (
                item.get("website")      or item.get("url")   or
                item.get("cst_website")  or
                self._build_website_from_slug(item)           or ""
            )
            website = _fix_website(raw_web)

            # ── Address fields ────────────────────────────────────────────
            address = (
                item.get("cst_adr_line1_dn") or item.get("address") or
                item.get("street")           or ""
            ).strip()
            city = (
                item.get("cst_city_dn") or item.get("city") or ""
            ).strip()

            # State: API field → area code inference → query-state fallback
            state_raw = (
                item.get("cst_state_dn") or item.get("state") or ""
            ).strip().upper()
            if state_raw not in STATE_NAMES:
                state_raw = _state_from_phone(phone) or item.get("_query_state", "")

            zip_code = (
                item.get("cst_zip_dn")   or item.get("zip")  or
                item.get("postal_code")  or ""
            ).strip()

            # ── Email ─────────────────────────────────────────────────────
            email = (
                item.get("cst_eml_address_dn") or item.get("email") or ""
            ).strip()

            # ── Membership & chapter ──────────────────────────────────────
            membership = (
                item.get("membership_type") or item.get("member_type") or
                "PHCC Member"
            ).strip()

            chapter = (
                item.get("chapter")     or item.get("phcc_chapter") or
                item.get("cst_state_dn") or state_raw or ""
            ).strip()

            # ── Niche-specific fields (inferred from real data) ───────────
            specialty          = _infer_specialty(name)
            services_offered   = _infer_services(name)
            market_segment     = _infer_market_segment(name)
            emergency          = _infer_emergency(name)
            license_state      = STATE_NAMES.get(state_raw, state_raw) if state_raw else ""

            records.append({
                "company_name":        name,
                "address":             address,
                "city":                city,
                "state":               state_raw,
                "zip_code":            zip_code,
                "country":             "US",
                "phone":               phone,
                "email":               email,
                "website":             website or "",
                # ── PHCC niche fields ──
                "trade_specialty":     specialty,
                "services_offered":    services_offered,
                "commercial_residential": market_segment,
                "emergency_service":   emergency,
                "phcc_chapter":        chapter,
                "service_area":        STATE_NAMES.get(state_raw, state_raw),
                "membership_type":     membership,
                "license_state":       license_state,
            })

        self.logger.info(f"Parsed {len(records):,} PHCC records")
        return records

    # ── Build website from slug field ──────────────────────────────────────
    def _build_website_from_slug(self, item: dict) -> str:
        slug = (item.get("cst_url_code_dn") or item.get("slug") or "").strip()
        if not slug:
            return ""
        # If slug looks like a full URL already, return as-is
        if slug.startswith("http"):
            return slug
        # If it looks like a domain (has a dot), prepend https://
        if "." in slug:
            return "https://" + slug
        # Otherwise it's an internal profile path — skip it
        return ""


# ────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="PHCC Plumbing-Heating-Cooling Contractors crawler")
    parser.add_argument("--db",  default="data/firmable.db",
                        help="SQLite database path")
    parser.add_argument("--csv", action="store_true",
                        help="Export CSV after run")
    args = parser.parse_args()

    os.makedirs("logs", exist_ok=True)
    crawler = PhccContractorCrawler(db_path=args.db)
    stats   = crawler.run()

    print(f"\n{'='*48}")
    print(f"  Parsed      : {stats['total_parsed']:>7,}")
    print(f"  New records : {stats['total_new']:>7,}")
    print(f"  Updated     : {stats['total_updated']:>7,}")
    print(f"  Duplicates  : {stats['total_duplicates']:>7,}")
    print(f"  Errors      : {stats['total_errors']:>7,}")
    print(f"{'='*48}")

    if args.csv and (stats["total_new"] + stats["total_updated"]) > 0:
        path = crawler.export_csv()
        if path:
            print(f"  CSV → {path}")
    elif stats["total_new"] + stats["total_updated"] == 0:
        print("\n  0 records collected.")
        print("  If running locally, open DevTools on:")
        print(f"  {PhccContractorCrawler.PAGE_URL}")
        print("  Enter any zip → watch Network tab for the AJAX POST to")
        print("  /wp-admin/admin-ajax.php — confirm action=phcc_contractor_finder")