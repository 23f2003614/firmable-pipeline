"""
Crawler #16: ASA — American Staffing Association Member Directory
Source  : https://americanstaffing.net/asa-member-directory/
Method  : GET https://americanstaffing.net/?json=1&search_type=listing
            &search_by=location&location=LAT,LNG&bounds=...&address=STATE
          

Records : ~8,000+ staffing agency branch offices across all 50 US states + DC

"""

import sys, os, time, re
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crawlers.base_crawler import BaseCrawler

# ---------------------------------------------------------------------------
# 50 US states + DC with centre lat/lng and bounding boxes
# ---------------------------------------------------------------------------
STATE_GRID = [
    ("Alabama",         32.80, -86.79, "30.14,-88.47:35.01,-84.89"),
    ("Alaska",          64.20,-153.00, "54.68,-168.00:71.34,-130.00"),
    ("Arizona",         34.29,-111.66, "31.33,-114.82:37.00,-109.04"),
    ("Arkansas",        34.75, -92.13, "33.00,-94.62:36.50,-89.64"),
    ("California",      36.78,-119.42, "32.53,-124.48:42.01,-114.13"),
    ("Colorado",        39.11,-105.36, "36.99,-109.05:41.00,-102.04"),
    ("Connecticut",     41.60, -72.69, "40.97,-73.73:42.05,-71.79"),
    ("Delaware",        39.00, -75.50, "38.45,-75.79:39.84,-75.05"),
    ("Florida",         27.99, -81.76, "24.54,-87.63:31.00,-80.03"),
    ("Georgia",         32.16, -83.50, "30.36,-85.61:35.00,-80.84"),
    ("Hawaii",          20.80,-156.33, "18.91,-160.25:22.24,-154.81"),
    ("Idaho",           44.07,-114.47, "41.99,-117.24:49.00,-111.04"),
    ("Illinois",        40.35, -88.99, "36.97,-91.51:42.51,-87.02"),
    ("Indiana",         39.85, -86.26, "37.77,-88.10:41.76,-84.78"),
    ("Iowa",            42.00, -93.21, "40.38,-96.64:43.50,-90.14"),
    ("Kansas",          38.52, -96.73, "36.99,-102.05:40.00,-94.59"),
    ("Kentucky",        37.67, -84.86, "36.50,-89.57:39.15,-81.96"),
    ("Louisiana",       31.07, -91.96, "28.92,-94.04:33.02,-89.00"),
    ("Maine",           45.25, -69.00, "42.98,-71.08:47.46,-66.95"),
    ("Maryland",        39.05, -76.64, "37.91,-79.49:39.72,-74.99"),
    ("Massachusetts",   42.26, -71.81, "41.24,-73.51:42.89,-69.93"),
    ("Michigan",        44.35, -85.41, "41.70,-90.42:48.31,-82.41"),
    ("Minnesota",       46.39, -94.64, "43.50,-97.24:49.38,-89.49"),
    ("Mississippi",     32.74, -89.67, "30.17,-91.65:35.01,-88.10"),
    ("Missouri",        38.46, -92.29, "35.99,-95.77:40.61,-89.10"),
    ("Montana",         46.88,-110.36, "44.36,-116.05:49.00,-104.04"),
    ("Nebraska",        41.49, -99.90, "39.99,-104.05:43.00,-95.31"),
    ("Nevada",          38.50,-117.02, "35.00,-120.01:42.00,-114.04"),
    ("New Hampshire",   43.19, -71.57, "42.70,-72.56:45.31,-70.61"),
    ("New Jersey",      40.06, -74.41, "38.93,-75.56:41.36,-73.90"),
    ("New Mexico",      34.31,-106.02, "31.33,-109.05:37.00,-103.00"),
    ("New York",        42.94, -75.52, "40.50,-79.76:45.02,-71.86"),
    ("North Carolina",  35.63, -79.81, "33.84,-84.32:36.59,-75.46"),
    ("North Dakota",    47.53, -99.78, "45.93,-104.05:49.00,-96.55"),
    ("Ohio",            40.37, -82.80, "38.40,-84.82:42.32,-80.52"),
    ("Oklahoma",        35.47, -97.52, "33.62,-103.00:37.00,-94.43"),
    ("Oregon",          43.80,-120.55, "41.99,-124.57:46.24,-116.46"),
    ("Pennsylvania",    40.59, -77.21, "39.72,-80.52:42.27,-74.69"),
    ("Rhode Island",    41.70, -71.52, "41.15,-71.91:42.02,-71.12"),
    ("South Carolina",  33.84, -80.90, "32.05,-83.35:35.22,-78.54"),
    ("South Dakota",    44.44, -99.83, "42.48,-104.06:45.95,-96.44"),
    ("Tennessee",       35.86, -86.35, "34.98,-90.31:36.68,-81.65"),
    ("Texas",           31.05, -97.56, "25.84,-106.65:36.50,-93.51"),
    ("Utah",            39.32,-111.09, "36.99,-114.05:42.00,-109.04"),
    ("Vermont",         44.04, -72.71, "42.73,-73.44:45.02,-71.47"),
    ("Virginia",        37.77, -78.17, "36.54,-83.68:39.47,-75.24"),
    ("Washington",      47.50,-120.50, "45.54,-124.73:49.00,-116.92"),
    ("West Virginia",   38.49, -80.95, "37.20,-82.64:40.64,-77.72"),
    ("Wisconsin",       44.27, -89.62, "42.49,-92.89:47.31,-86.25"),
    ("Wyoming",         43.07,-107.29, "40.99,-111.06:45.01,-104.05"),
    ("Washington DC",   38.91, -77.04, "38.80,-77.12:38.99,-76.91"),
]

# Canadian province names to exclude
_CANADA_PROVINCES = {
    "ontario", "british columbia", "alberta", "quebec", "manitoba",
    "saskatchewan", "nova scotia", "new brunswick", "newfoundland",
    "prince edward island", "northwest territories", "yukon", "nunavut",
}

# Valid US state names (lower) for fast lookup
_US_STATES_LOWER = {s[0].lower() for s in STATE_GRID}


def _is_us_record(country: str, state: str) -> bool:
    """Return True only if the record is a US location."""
    c = (country or "").strip().lower()
    s = (state or "").strip().lower()

    # Explicit non-US countries
    if c and c not in ("united states", "us", "usa", "u.s.", "u.s.a.", ""):
        return False

    # Canadian provinces slipping in with blank country
    if s in _CANADA_PROVINCES:
        return False

    return True


def _normalise_phone(raw: str) -> str:
    """Strip to digits only; prefix +1 if 10 digits, else return as-is."""
    if not raw:
        return ""
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return digits or raw.strip()


class AsaStaffingCrawler(BaseCrawler):

    API_URL = "https://americanstaffing.net/"
    DIR_URL = "https://americanstaffing.net/asa-member-directory/"

    def get_dataset_name(self): return "asa_staffing_agencies"
    def get_source_url(self):   return self.DIR_URL
    def get_niche_fields(self):
        return [
            "staffing_specialization",   # occupational job-type breakdown (%)
            "placement_type",            # service type breakdown (%)
            "industries_served",         # industry text from meta (distinct from occ %)
            "member_since",
            "asa_member_type",           # Staffing Firm / Staffing Firm HQ
            "is_hq",                     # True if this record is the HQ
            "ownership_type",            # ESOP / franchise / family-owned
            "certified_professionals",   # count of ASA-certified staff
            "geographic_scope",          # local / regional / national (derived post-crawl)
            "num_offices_in_state",      # branch count for this company in this state
        ]

    def _setup_logging(self):
        import logging, io
        os.makedirs("logs", exist_ok=True)
        log_file = f"logs/{self.get_dataset_name()}_{self.session_id}.log"
        self.logger = logging.getLogger(self.get_dataset_name())
        self.logger.setLevel(logging.DEBUG)
        if self.logger.handlers:
            return
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        self.logger.addHandler(fh)
        try:
            stream = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                      errors="replace", line_buffering=True)
        except AttributeError:
            stream = sys.stdout
        ch = logging.StreamHandler(stream)
        ch.setLevel(logging.INFO)
        ch.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        self.logger.addHandler(ch)

    # ------------------------------------------------------------------ #
    def crawl(self):
        try:
            from curl_cffi import requests as cffi_requests
            session = cffi_requests.Session(impersonate="chrome124")
            self.logger.info("Using curl_cffi")
        except ImportError:
            import requests
            session = requests.Session()

        session.headers.update({
            "Accept":  "application/json, */*",
            "Referer": self.DIR_URL,
        })

        all_members = []
        seen_ids = set()

        for state_name, lat, lng, bounds in STATE_GRID:
            self.logger.info(f"[{state_name}]")
            members = self._fetch_state(session, state_name, lat, lng, bounds)

            added = 0
            for m in members:
                uid = (
                    m.get("permalink") or
                    str(m.get("ID") or m.get("id") or "")
                ).strip().lower()

                if uid and uid not in seen_ids:
                    seen_ids.add(uid)
                    all_members.append(m)
                    added += 1

            self.logger.info(f"  -> {len(members)} returned, {added} new "
                             f"(total: {len(all_members)})")
            self._rate_limit(1)

        self.logger.info(f"Total unique raw: {len(all_members)}")
        return all_members

    # ------------------------------------------------------------------ #
    def _fetch_state(self, session, state_name, lat, lng, bounds):
        base_params = {
            "json":        "1",
            "search_type": "listing",
            "search_by":   "location",
            "location":    f"{lat},{lng}",
            "bounds":      bounds,
            "address":     state_name,
            "company_name": "",
        }

        all_posts = []
        page = 1
        BASE = self.API_URL.rstrip("/")

        while True:
            url = f"{BASE}/" if page == 1 else f"{BASE}/page/{page}/"
            try:
                resp = session.get(url, params=base_params, timeout=30)
                if not resp.ok:
                    self.logger.warning(f"  HTTP {resp.status_code} on page {page}")
                    break

                data = resp.json()

                if isinstance(data, list):
                    posts = data
                    total_pages = 1
                elif isinstance(data, dict):
                    posts = (data.get("posts") or data.get("listings") or
                             data.get("members") or data.get("results") or
                             data.get("data") or [])
                    total_pages = int(data.get("total_pages") or 1)
                else:
                    break

                if not posts:
                    break

                all_posts.extend(posts)
                self.logger.debug(f"    page {page}/{total_pages} → {len(posts)} posts")

                if page >= total_pages:
                    break

                page += 1
                self._rate_limit(0.5)

            except Exception as e:
                self.logger.warning(f"  [{state_name}] page {page} error: {e}")
                break

        return all_posts

    # ------------------------------------------------------------------ #
    @staticmethod
    def _m(meta, key):
        """Safely unwrap single-element array meta fields."""
        val = meta.get(key)
        if val is None:
            return ""
        if isinstance(val, list):
            val = val[0] if val else ""
        if val is None:
            return ""
        return str(val).strip()

    @staticmethod
    def _occ_to_categories(meta):
        """
        Occupational job-type breakdown — 'staffing_specialization'.
        Example: "Industrial (40%), Office Clerical (50%)"
        """
        cats = {
            "Engineering":     "Occupational_Cat_Engineering__c",
            "Finance":         "Occupational_Cat_Finance__c",
            "Health Care":     "Occupational_Cat_Health_Care__c",
            "IT":              "Occupational_Cat_IT__c",
            "Industrial":      "Occupational_Cat_Industrial__c",
            "Legal":           "Occupational_Cat_Legal__c",
            "Management":      "Occupational_Cat_Management__c",
            "Office Clerical": "Occupational_Cat_Office_Clerical__c",
            "Sales/Marketing": "Occupational_Cat_Sales_Marketing__c",
            "Scientific":      "Occupational_Cat_Scientific__c",
            "Other":           "Occupational_Cat_Other__c",
        }
        parts = []
        for label, key in cats.items():
            raw = meta.get(key)
            if isinstance(raw, list):
                raw = raw[0] if raw else None
            try:
                pct = int(raw or 0)
            except (ValueError, TypeError):
                pct = 0
            if pct > 0:
                parts.append(f"{label} ({pct}%)")
        return ", ".join(parts)

    @staticmethod
    def _svc_to_categories(meta):
        """
        Staffing service type breakdown — 'placement_type'.
        Example: "Direct Hire (20%), Temp-to-Hire (80%)"
        """
        svcs = {
            "Direct Hire":       "Service_Cat_Direct_Hire__c",
            "Temp Help":         "Service_Cat_Temp_Help__c",
            "Temp-to-Hire":      "Service_Cat_Temp_to_Hire__c",
            "Long-Term Contract":"Service_Cat_Long_Term_Contract__c",
            "HR Consulting":     "Service_Cat_HR_Consulting__c",
            "Payrolling":        "Service_Cat_Payrolling__c",
            "RPO":               "Service_Cat_RPO__c",
            "MSP":               "Service_Cat_Managed_Service_Provider__c",
            "Managed Services":  "Service_Cat_Managed_Services__c",
            "PEO":               "Service_Cat_PEO__c",
            "Outplacement":      "Service_Cat_Outplacement__c",
            "Govt Contracting":  "Service_Cat_Government_Contracting__c",
            "VMS":               "Service_Cat_Vendor_Management_System__c",
        }
        parts = []
        for label, key in svcs.items():
            raw = meta.get(key)
            if isinstance(raw, list):
                raw = raw[0] if raw else None
            try:
                pct = int(raw or 0)
            except (ValueError, TypeError):
                pct = 0
            if pct > 0:
                parts.append(f"{label} ({pct}%)")
        return ", ".join(parts)

    # ------------------------------------------------------------------ #
    def parse(self, raw_data):
        if not raw_data:
            return []

        # ── Pass 1: build records and count per-company state presence ──
        # We need state counts to derive geographic_scope and num_offices_in_state
        from collections import defaultdict, Counter

        pre_records = []
        for item in raw_data:
            if not isinstance(item, dict):
                continue

            meta = item.get("meta", {}) or {}

            name = (self._m(meta, "Name") or
                    str(item.get("post_title", "")).strip())
            if not name:
                continue

            country = self._m(meta, "BillingCountry") or "United States"
            state   = self._m(meta, "BillingState")

            # ── US-only filter ──
            if not _is_us_record(country, state):
                self.logger.debug(f"Skipped non-US: {name} | {state} | {country}")
                continue

            occ_breakdown = self._occ_to_categories(meta)
            svc_breakdown = self._svc_to_categories(meta)

            # industries_served — a distinct field from occupational %
            # ASA meta may expose "Industry__c" or "Industries_Served__c"
            industries_text = (
                self._m(meta, "Industries_Served__c") or
                self._m(meta, "Industry__c") or
                self._m(meta, "Primary_Industry__c") or
                ""   # fall back to blank; do NOT duplicate occ_breakdown here
            )

            member_since   = self._m(meta, "Original_Join_Date__c")
            asa_type       = self._m(meta, "Account_Type__c") or "Staffing Firm"
            ownership_type = self._m(meta, "ASA_Ownership_Type__c")
            certified_pros = self._m(meta, "Certified_Professionals__c")
            is_hq          = "Yes" if "HQ" in asa_type else "No"

            phone_raw = self._m(meta, "Phone")
            phone_norm = _normalise_phone(phone_raw)

            pre_records.append({
                "company_name":           name,
                "address":                self._m(meta, "BillingStreet"),
                "city":                   self._m(meta, "BillingCity"),
                "state":                  state,
                "zip_code":               self._m(meta, "BillingPostalCode"),
                "country":                "United States",   # forced after filter
                "phone":                  phone_norm,
                "email":                  "",                # not exposed in API
                "website":                self._m(meta, "Website"),
                "member_url":             item.get("permalink", ""),
                # ── Niche fields ─────────────────────────────────────────
                "staffing_specialization": occ_breakdown,   # job-type %
                "placement_type":          svc_breakdown,   # service-type %
                "industries_served":       industries_text, # text (may be blank)
                "member_since":            member_since,
                "asa_member_type":         asa_type,
                "is_hq":                   is_hq,
                "ownership_type":          ownership_type,
                "certified_professionals": certified_pros,
                # geographic_scope and num_offices_in_state filled below
                "geographic_scope":        "",
                "num_offices_in_state":    "",
            })

        # ── Pass 2: derive geographic_scope and num_offices_in_state ──
        # geographic_scope: based on how many distinct states a company appears in
        #   1 state  → Local
        #   2-5      → Regional
        #   6+       → National
        company_states = defaultdict(set)
        for r in pre_records:
            company_states[r["company_name"].lower()].add(r["state"])

        company_offices_per_state = defaultdict(Counter)  # name → {state: count}
        for r in pre_records:
            company_offices_per_state[r["company_name"].lower()][r["state"]] += 1

        for r in pre_records:
            n = r["company_name"].lower()
            num_states = len(company_states[n])
            if num_states == 1:
                scope = "Local"
            elif num_states <= 5:
                scope = "Regional"
            else:
                scope = "National"
            r["geographic_scope"]     = scope
            r["num_offices_in_state"] = str(
                company_offices_per_state[n][r["state"]]
            )

        self.logger.info(f"Parsed {len(pre_records)} US-only ASA records "
                         f"(dropped non-US rows)")
        return pre_records

    # ── Override _deduplicate to include zip_code so branch offices are kept ──
    def _deduplicate(self, records):
        import hashlib
        seen = set()
        unique = []
        for rec in records:
            # Include zip_code so same-name same-state different-branch rows survive
            raw_key = (
                (rec.get("company_name") or "").lower() +
                (rec.get("address")      or "").lower() +
                (rec.get("city")         or "").lower() +
                (rec.get("zip_code")     or "").lower() +
                (rec.get("state")        or "").lower() +
                self.get_dataset_name()
            )
            hash_key = hashlib.md5(raw_key.encode()).hexdigest()
            if hash_key not in seen:
                seen.add(hash_key)
                rec["dedup_hash"] = hash_key
                unique.append(rec)
        return unique


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    crawler = AsaStaffingCrawler()
    stats = crawler.run()
    print(f"\n{'='*45}")
    print(f"  New records  : {stats['total_new']:,}")
    print(f"  Updated      : {stats['total_updated']:,}")
    print(f"  Duplicates   : {stats['total_duplicates']:,}")
    print(f"  Errors       : {stats['total_errors']:,}")
    print(f"{'='*45}")
    if stats["total_new"] + stats["total_updated"] > 0:
        path = crawler.export_csv()
        print(f"  CSV saved    : {path}")
    else:
        print("\n  0 records — check Response tab in DevTools for JSON structure")