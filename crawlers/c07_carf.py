"""
Crawler #7: CARF International -- Accredited Providers
Source  : https://carf.org/find-provider/
Endpoint: https://carf.org/wp-admin/admin-ajax.php
          ?action=store_search&lat=LAT&lng=LNG&max_results=25&search_radius=500

Method  : Pure requests (curl_cffi for Chrome TLS fingerprint).
          CARF uses a WordPress Store Locator plugin returning JSON.
          We query a 40-point grid at 500-mile radius, deduplicate by provider_id.


Records : ~8000 accredited US providers
Niche   : provider_id, accreditation_area, service_domain, program_types,
          population_served, accreditation_status, is_administrative_only,
          program_count, latitude, longitude
"""

import sys
import os
import re
import time
import html
from html.parser import HTMLParser

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crawlers.base_crawler import BaseCrawler


# ---------------------------------------------------------------------------
# 40-point grid covering all 50 US states + DC at 500-mile radius
# Expanded from 27 → 40 points to reduce sparse coverage in the Southeast/
# Mid-Atlantic corridor where many providers are concentrated.
# ---------------------------------------------------------------------------
US_GRID = [
    # West Coast
    ("Seattle-WA",          47.6062, -122.3321),
    ("Portland-OR",         45.5051, -122.6750),
    ("San-Francisco-CA",    37.7749, -122.4194),
    ("Los-Angeles-CA",      34.0522, -118.2437),
    ("San-Diego-CA",        32.7157, -117.1611),
    # Southwest
    ("Phoenix-AZ",          33.4484, -112.0740),
    ("Las-Vegas-NV",        36.1699, -115.1398),
    ("Albuquerque-NM",      35.0844, -106.6504),
    ("El-Paso-TX",          31.7619, -106.4850),
    # Mountain / Rockies
    ("Denver-CO",           39.7392, -104.9903),
    ("Salt-Lake-City-UT",   40.7608, -111.8910),
    ("Helena-MT",           46.5958, -112.0270),
    ("Boise-ID",            43.6150, -116.2023),
    ("Cheyenne-WY",         41.1400, -104.8202),
    # Great Plains
    ("Rapid-City-SD",       44.0805, -103.2310),
    ("Bismarck-ND",         46.8083,  -100.7837),
    ("Fargo-ND",            46.8772,   -96.7898),
    ("Sioux-Falls-SD",      43.5446,   -96.7311),
    ("Omaha-NE",            41.2565,   -95.9345),
    ("Wichita-KS",          37.6872,   -97.3301),
    # Midwest
    ("Minneapolis-MN",      44.9778,   -93.2650),
    ("Madison-WI",          43.0731,   -89.4012),
    ("Chicago-IL",          41.8781,   -87.6298),
    ("Kansas-City-MO",      39.0997,   -94.5786),
    ("Indianapolis-IN",     39.7684,   -86.1581),
    ("Columbus-OH",         39.9612,   -82.9988),
    ("Detroit-MI",          42.3314,   -83.0458),
    # South
    ("Houston-TX",          29.7604,   -95.3698),
    ("Dallas-TX",           32.7767,   -96.7970),
    ("New-Orleans-LA",      29.9511,   -90.0715),
    ("Memphis-TN",          35.1495,   -90.0490),
    ("Nashville-TN",        36.1627,   -86.7816),
    ("Birmingham-AL",       33.5186,   -86.8104),
    ("Atlanta-GA",          33.7490,   -84.3880),
    ("Jacksonville-FL",     30.3322,   -81.6557),
    ("Miami-FL",            25.7617,   -80.1918),
    # East Coast
    ("Charlotte-NC",        35.2271,   -80.8431),
    ("Washington-DC",       38.9072,   -77.0369),
    ("Philadelphia-PA",     39.9526,   -75.1652),
    ("New-York-NY",         40.7128,   -74.0060),
    ("Boston-MA",           42.3601,   -71.0589),
    # Non-contiguous
    ("Anchorage-AK",        61.2181,  -149.9003),
    ("Honolulu-HI",         21.3069,  -157.8583),
]

# CARF API max_results — must match browser exactly (25); larger = timeout
MAX_RESULTS = 25

# Keywords that indicate population served (matched against tag strings)
POPULATION_KEYWORDS = [
    "adult", "child", "youth", "geriatric", "senior", "veteran", "military",
    "women", "men", "family", "adolescent", "homeless", "criminal justice",
    "dually diagnosed", "intellectual", "disability", "deaf", "blind",
    "spanish", "hispanic", "lgbtq", "pregnant", "co-occurring",
]

# Service domain codes embedded in program type strings
DOMAIN_MAP = {
    "(BH)":  "Behavioral Health",
    "(ECS)": "Employment & Community Services",
    "(CYS)": "Children & Youth Services",
    "(MR)":  "Medical Rehabilitation",
    "(AD)":  "Aging Services / Dementia",
    "(OTP)": "Opioid Treatment Program",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _MLStripper(HTMLParser):
    """Strip HTML tags from a string."""
    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return " ".join(self.fed)


def _strip_html(text: str) -> str:
    s = _MLStripper()
    s.feed(text)
    return s.get_data().strip()


def _decode(text) -> str:
    """Decode HTML entities and strip tags."""
    if not text:
        return ""
    text = str(text)
    text = html.unescape(text)   # &#8217; → '   &amp; → &   etc.
    text = _strip_html(text)
    return " ".join(text.split())


def _slug_to_name(slug: str) -> str:
    """
    Convert CARF slug to human-readable name.
    'A-New-Leaf&#8217;s-East-Valley-Men&#8217;s-Center-399891'
    → 'A New Leaf's East Valley Men's Center'

    Steps:
      1. HTML-decode entities
      2. Strip trailing numeric provider ID (last hyphen-separated token if all digits)
      3. Replace remaining hyphens with spaces
    """
    decoded = _decode(slug)
    # Strip trailing provider ID: e.g. "A New Leaf's Center 399891" → "A New Leaf's Center"
    cleaned = re.sub(r'\s+\d{4,7}$', '', decoded).strip()
    return cleaned


def _extract_provider_id(slug: str) -> str:
    """Extract CARF's numeric provider ID from the end of the slug."""
    m = re.search(r'-(\d{4,7})$', str(slug))
    return m.group(1) if m else ""


def _extract_domains(program_str: str) -> str:
    """Return comma-separated service domains inferred from program type codes."""
    domains = []
    for code, label in DOMAIN_MAP.items():
        if code in program_str and label not in domains:
            domains.append(label)
    return ", ".join(domains)


def _extract_population(tag_str: str) -> str:
    """Extract population-served keywords from the tag/category string."""
    lower = tag_str.lower()
    found = [kw.title() for kw in POPULATION_KEYWORDS if kw in lower]
    return ", ".join(dict.fromkeys(found))   # preserve order, deduplicate


def _program_count(program_str: str) -> int:
    """Count distinct accredited programs at this location."""
    if not program_str:
        return 0
    return len([p for p in program_str.split(",") if p.strip()])


def _is_admin_only(program_str: str) -> str:
    return "Yes" if "administrative location only" in program_str.lower() else "No"


def _join_list(val) -> str:
    if isinstance(val, list):
        parts = []
        for v in val:
            if isinstance(v, dict):
                parts.append(v.get("name") or v.get("label") or str(v))
            else:
                parts.append(str(v))
        return ", ".join(p for p in parts if p)
    return _decode(str(val or ""))


# ---------------------------------------------------------------------------
class CarfAccreditedCrawler(BaseCrawler):

    API_URL  = "https://carf.org/wp-admin/admin-ajax.php"
    PAGE_URL = "https://carf.org/find-provider/"

    def get_dataset_name(self):  return "carf_accredited_providers"
    def get_source_url(self):    return self.PAGE_URL

    def get_niche_fields(self):
        return [
            "provider_id",           # CARF's own numeric ID (from slug)
            "accreditation_area",    # e.g. "Behavioral Health"
            "service_domain",        # derived: BH / ECS / CYS / MR / OTP
            "program_types",         # full list of accredited programs
            "population_served",     # extracted from tags
            "accreditation_status",  # Accredited / Accreditation with Quality
            "is_administrative_only",# Yes/No flag
            "program_count",         # # of distinct programs
            "latitude",              # geo coordinates from API
            "longitude",
        ]

    # ------------------------------------------------------------------ #
    def crawl(self):
        try:
            from curl_cffi import requests as cffi_requests
            session = cffi_requests.Session(impersonate="chrome124")
            self.logger.info("Using curl_cffi (Chrome fingerprint)")
        except ImportError:
            self.logger.warning(
                "curl_cffi not installed — falling back to requests. "
                "Install with: pip install curl_cffi"
            )
            import requests as std_requests
            session = std_requests.Session()

        session.headers.update({
            "Accept":             "*/*",
            "Referer":            self.PAGE_URL,
            "X-Requested-With":   "XMLHttpRequest",
        })

        # Fetch page first to pick up session cookies
        self.logger.info("Fetching CARF provider page for session cookies...")
        try:
            page_resp = session.get(self.PAGE_URL, timeout=30)
            self.logger.info(f"  Page status: {page_resp.status_code}")
        except Exception as e:
            self.logger.warning(f"  Page fetch failed: {e}")

        all_providers = []
        # Deduplicate by CARF provider_id (from slug) — eliminates all grid overlap
        seen_ids = set()

        for i, (label, lat, lng) in enumerate(US_GRID):
            self.logger.info(f"[{i+1}/{len(US_GRID)}] {label} ({lat}, {lng})")
            providers = self._fetch_grid_point(session, lat, lng)
            added = 0
            for p in providers:
                # Primary dedup key = CARF provider_id extracted from slug
                slug      = str(p.get("store") or p.get("title") or "")
                prov_id   = _extract_provider_id(slug)
                # Fallback if no numeric ID found: use store|city combination
                uid = prov_id or f"{slug.lower()}|{str(p.get('city','')).lower()}"

                if uid and uid not in seen_ids:
                    seen_ids.add(uid)
                    all_providers.append(p)
                    added += 1

            self.logger.info(
                f"  -> {len(providers)} returned, {added} new "
                f"(total: {len(all_providers)})"
            )
            self._rate_limit(1.5)   # slightly gentler to avoid 429s

        self.logger.info(f"Total unique providers: {len(all_providers)}")
        return all_providers

    # ------------------------------------------------------------------ #
    def _fetch_grid_point(self, session, lat, lng):
        params = {
            "action":        "store_search",
            "lat":           lat,
            "lng":           lng,
            "max_results":   MAX_RESULTS,
            "search_radius": 500,
        }
        try:
            resp = session.get(self.API_URL, params=params, timeout=60)
            if not resp.ok:
                self.logger.warning(f"  HTTP {resp.status_code}: {resp.text[:120]}")
                return []

            data = resp.json()

            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return (
                    data.get("locations") or data.get("providers") or
                    data.get("results")   or data.get("stores") or
                    data.get("data")      or []
                )
            return []

        except Exception as e:
            self.logger.warning(f"  Request error: {e}")
            return []

    # ------------------------------------------------------------------ #
    def parse(self, raw_data):
        if not raw_data:
            return []

        records = []
        for item in raw_data:
            if not isinstance(item, dict):
                continue

            # ----------------------------------------------------------
            # Name resolution
            # The WP Store Locator `store` field contains a URL slug like:
            #   "A-New-Leaf-Inc-394257"  (slug form, with trailing provider ID)
            # The `title` field (if present) contains HTML like:
            #   "<a href='...'>A New Leaf Inc</a>"
            # We prefer the decoded title; fall back to un-slugged store field.
            # ----------------------------------------------------------
            slug  = str(item.get("store") or "")
            title = _decode(item.get("title") or item.get("name") or "")

            # title often has trailing ID appended too — strip it
            title_clean = re.sub(r'\s+\d{4,7}$', '', title).strip()

            if title_clean and not title_clean.isdigit():
                company_name = title_clean
            elif slug:
                company_name = _slug_to_name(slug)
            else:
                continue  # no usable name

            if not company_name:
                continue

            provider_id = _extract_provider_id(slug) or _extract_provider_id(title)

            # ----------------------------------------------------------
            # Address
            # ----------------------------------------------------------
            address  = _decode(item.get("address")  or item.get("street")  or "")
            address2 = _decode(item.get("address2") or item.get("street2") or "")
            full_addr = " ".join(filter(None, [address, address2])).strip()

            city    = _decode(item.get("city")    or "")
            state   = _decode(item.get("state")   or "")
            zip_    = _decode(item.get("zip")     or item.get("postal_code") or "")
            country = _decode(item.get("country") or "US")
            phone   = _decode(item.get("phone")   or item.get("tel") or "")
            email   = _decode(item.get("email")   or "")
            website = _decode(item.get("url") or item.get("website") or item.get("link") or "")

            # Geo coordinates returned by the API
            lat_val = str(item.get("lat") or item.get("latitude") or "")
            lng_val = str(item.get("lng") or item.get("longitude") or "")

            # ----------------------------------------------------------
            # Tags / Categories — the richest niche data
            # WP Store Locator stores all CARF program data in `tags`.
            # Each tag is either a string or {"name": "...", "id": ...}
            # ----------------------------------------------------------
            tags = item.get("tags") or []
            if isinstance(tags, list):
                tag_strs = [
                    t.get("name", "") if isinstance(t, dict) else str(t)
                    for t in tags
                ]
                tag_str = ", ".join(t for t in tag_strs if t)
            else:
                tag_str = _decode(str(tags))

            categories = item.get("categories") or item.get("category") or ""
            cat_str    = _join_list(categories) if isinstance(categories, list) \
                         else _decode(str(categories))

            # Combine tags + categories for program type info
            program_raw = ", ".join(filter(None, [tag_str, cat_str]))

            # ----------------------------------------------------------
            # Derived niche fields
            # ----------------------------------------------------------
            # accreditation_area: top-level CARF domain (BH / ECS / CYS / MR)
            # We also check the description/custom fields for domain codes
            description = _decode(item.get("description") or item.get("content") or "")
            full_text   = " ".join(filter(None, [program_raw, description]))

            accred_area   = (
                _decode(item.get("accreditation_area") or item.get("area") or "")
                or _extract_domains(full_text)
            )
            service_domain  = _extract_domains(full_text)
            population      = (
                _decode(item.get("population_served") or item.get("populations") or "")
                or _extract_population(full_text)
            )
            status          = _decode(
                item.get("accreditation_status") or
                item.get("status") or
                "Accredited"
            )
            service_cat     = _decode(item.get("service_category") or item.get("services") or "")
            is_admin        = _is_admin_only(program_raw)
            prog_count      = _program_count(program_raw)

            records.append({
                "company_name":           company_name,
                "address":                full_addr,
                "city":                   city,
                "state":                  state,
                "zip_code":               zip_,
                "country":                country,
                "phone":                  phone,
                "email":                  email,
                "website":                website,
                # --- niche fields ---
                "provider_id":            provider_id,
                "accreditation_area":     accred_area,
                "service_domain":         service_domain,
                "program_types":          program_raw,
                "population_served":      population,
                "accreditation_status":   status,
                "is_administrative_only": is_admin,
                "program_count":          str(prog_count),
                "latitude":               lat_val,
                "longitude":              lng_val,
            })

        self.logger.info(f"Parsed {len(records)} CARF records")
        return records

    # ------------------------------------------------------------------ #
    # Override _deduplicate to use provider_id as the primary key
    # (base class uses company_name+address hash which causes false dupes
    #  when the same org has multiple locations with different addresses)
    # ------------------------------------------------------------------ #
    def _deduplicate(self, records):
        import hashlib
        seen = set()
        unique = []
        for rec in records:
            prov_id = rec.get("provider_id", "")
            if prov_id:
                # CARF provider_id is already globally unique per location
                key = f"carf_pid_{prov_id}"
            else:
                # Fallback: hash on name + address + state (same as base class)
                raw = (
                    (rec.get("company_name") or "").lower() +
                    (rec.get("address")      or "").lower() +
                    (rec.get("state")        or "").lower() +
                    self.get_dataset_name()
                )
                key = hashlib.md5(raw.encode()).hexdigest()

            if key not in seen:
                seen.add(key)
                rec["dedup_hash"] = key
                unique.append(rec)

        return unique


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    crawler = CarfAccreditedCrawler()
    stats   = crawler.run()
    print(f"\n{'='*48}")
    print(f"  New records  : {stats['total_new']:,}")
    print(f"  Updated      : {stats['total_updated']:,}")
    print(f"  Duplicates   : {stats['total_duplicates']:,}")
    print(f"  Errors       : {stats['total_errors']:,}")
    print(f"{'='*48}")
    if stats["total_new"] + stats["total_updated"] > 0:
        path = crawler.export_csv()
        print(f"  CSV saved    : {path}")
    else:
        print("\n  0 records — check logs/")