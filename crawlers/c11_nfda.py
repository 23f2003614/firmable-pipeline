"""
Crawler 11 — NFDA Member Funeral Homes
Source   : https://rallocator.nfda.org/
Records  : ~7,255 | US

Fetch    : No "get all" endpoint exists. Covers the US with 46 geographic
           anchor points at 250-mile radius each — overlapping circles
           ensure no area is missed.

Parse    : BeautifulSoup finds all li.xmp-location-listing elements.
           Deduplicates by unique GUID data-id attribute on each listing
           to remove overlaps from adjacent grid circles.

           GUID deduplication — the same funeral home appears in multiple
           radius queries. The data-id is the only reliable unique key.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
try:
    from base_crawler import BaseCrawler
except ModuleNotFoundError:
    raise SystemExit("ERROR: base_crawler.py not found.")

RAL_URL = "https://rallocator.nfda.org/"
REQUEST_DELAY = 2.0

US_GRID: List[Tuple[float, float, str]] = [
    (42.36,  -71.06, "Boston, MA, USA"),
    (40.71,  -74.01, "New York, NY, USA"),
    (39.95,  -75.16, "Philadelphia, PA, USA"),
    (38.89,  -77.03, "Washington, DC, USA"),
    (43.07,  -76.15, "Syracuse, NY, USA"),
    (44.48,  -73.21, "Burlington, VT, USA"),
    (41.76,  -72.68, "Hartford, CT, USA"),
    (33.75,  -84.39, "Atlanta, GA, USA"),
    (35.23,  -80.84, "Charlotte, NC, USA"),
    (36.17,  -86.78, "Nashville, TN, USA"),
    (35.15,  -90.05, "Memphis, TN, USA"),
    (30.33,  -81.66, "Jacksonville, FL, USA"),
    (25.77,  -80.19, "Miami, FL, USA"),
    (30.69,  -88.04, "Mobile, AL, USA"),
    (32.30,  -90.18, "Jackson, MS, USA"),
    (29.95,  -90.07, "New Orleans, LA, USA"),
    (41.85,  -87.65, "Chicago, IL, USA"),
    (39.96,  -82.99, "Columbus, OH, USA"),
    (42.33,  -83.05, "Detroit, MI, USA"),
    (44.98,  -93.27, "Minneapolis, MN, USA"),
    (38.25,  -85.76, "Louisville, KY, USA"),
    (39.10,  -94.58, "Kansas City, MO, USA"),
    (38.63,  -90.20, "St Louis, MO, USA"),
    (41.26,  -95.94, "Omaha, NE, USA"),
    (43.55,  -96.73, "Sioux Falls, SD, USA"),
    (46.88, -100.78, "Bismarck, ND, USA"),
    (29.76,  -95.37, "Houston, TX, USA"),
    (32.78,  -96.80, "Dallas, TX, USA"),
    (30.27,  -97.74, "Austin, TX, USA"),
    (35.47,  -97.52, "Oklahoma City, OK, USA"),
    (32.50,  -93.75, "Shreveport, LA, USA"),
    (39.74, -104.98, "Denver, CO, USA"),
    (40.76, -111.89, "Salt Lake City, UT, USA"),
    (35.08, -106.65, "Albuquerque, NM, USA"),
    (33.45, -112.07, "Phoenix, AZ, USA"),
    (43.62, -116.20, "Boise, ID, USA"),
    (46.60, -112.03, "Helena, MT, USA"),
    (41.14, -104.82, "Cheyenne, WY, USA"),
    (47.61, -122.33, "Seattle, WA, USA"),
    (45.52, -122.68, "Portland, OR, USA"),
    (37.77, -122.42, "San Francisco, CA, USA"),
    (34.05, -118.24, "Los Angeles, CA, USA"),
    (32.72, -117.16, "San Diego, CA, USA"),
    (36.17, -115.14, "Las Vegas, NV, USA"),
    (38.58, -121.49, "Sacramento, CA, USA"),
    (61.22, -149.90, "Anchorage, AK, USA"),
    (21.31, -157.86, "Honolulu, HI, USA"),
]

_STATE_CENTERS: Dict[str, Tuple[float, float, str]] = {
    "TX": (31.97, -99.90, "Texas, USA"),
    "CA": (36.78, -119.42, "California, USA"),
    "FL": (27.99, -81.76, "Florida, USA"),
    "NY": (42.97, -75.15, "New York, USA"),
    "IL": (40.35, -88.99, "Illinois, USA"),
    "PA": (40.59, -77.21, "Pennsylvania, USA"),
    "OH": (40.41, -82.91, "Ohio, USA"),
    "GA": (32.17, -82.90, "Georgia, USA"),
    "NC": (35.63, -79.81, "North Carolina, USA"),
    "MI": (44.31, -85.60, "Michigan, USA"),
    "WA": (47.51, -120.74, "Washington, USA"),
    "AZ": (34.05, -111.09, "Arizona, USA"),
    "TN": (35.86, -86.66, "Tennessee, USA"),
    "CO": (39.00, -105.55, "Colorado, USA"),
    "VA": (37.93, -79.02, "Virginia, USA"),
    "MN": (46.39, -94.64, "Minnesota, USA"),
    "MA": (42.23, -71.53, "Massachusetts, USA"),
    "WI": (44.27, -89.62, "Wisconsin, USA"),
    "MD": (39.05, -76.64, "Maryland, USA"),
    "MO": (38.46, -92.29, "Missouri, USA"),
    "IN": (39.85, -86.26, "Indiana, USA"),
    "SC": (33.84, -80.90, "South Carolina, USA"),
    "AL": (32.80, -86.79, "Alabama, USA"),
    "LA": (31.17, -91.87, "Louisiana, USA"),
    "KY": (37.67, -84.67, "Kentucky, USA"),
    "OR": (43.93, -120.56, "Oregon, USA"),
    "OK": (35.57, -96.93, "Oklahoma, USA"),
    "CT": (41.60, -72.70, "Connecticut, USA"),
    "UT": (39.32, -111.09, "Utah, USA"),
    "IA": (41.88, -93.10, "Iowa, USA"),
    "NV": (38.80, -116.42, "Nevada, USA"),
    "AR": (34.97, -92.37, "Arkansas, USA"),
    "MS": (32.74, -89.67, "Mississippi, USA"),
    "KS": (38.53, -96.73, "Kansas, USA"),
    "NM": (34.84, -106.25, "New Mexico, USA"),
    "NE": (41.49, -99.90, "Nebraska, USA"),
    "ID": (44.07, -114.74, "Idaho, USA"),
    "WV": (38.49, -80.95, "West Virginia, USA"),
    "HI": (21.09, -157.50, "Hawaii, USA"),
    "NH": (43.45, -71.56, "New Hampshire, USA"),
    "ME": (45.37, -69.44, "Maine, USA"),
    "RI": (41.70, -71.50, "Rhode Island, USA"),
    "MT": (46.88, -110.36, "Montana, USA"),
    "DE": (38.91, -75.53, "Delaware, USA"),
    "SD": (44.30, -99.44, "South Dakota, USA"),
    "ND": (47.53, -101.00, "North Dakota, USA"),
    "AK": (64.20, -153.37, "Alaska, USA"),
    "VT": (44.05, -72.71, "Vermont, USA"),
    "WY": (42.76, -107.30, "Wyoming, USA"),
    "DC": (38.91, -77.01, "Washington DC, USA"),
    "NJ": (40.06, -74.41, "New Jersey, USA"),
    "WV": (38.49, -80.95, "West Virginia, USA"),
}

_STATE_ABBRS = (
    "AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|"
    "MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|"
    "SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC"
)
_YEAR_RE    = re.compile(r"\b(?:est(?:ablished)?\.?\s*|founded\s*(?:in\s*)?)(\d{4})\b", re.I)
_YEAR_SINCE = re.compile(r"\bsince\s+(\d{4})\b", re.I)

_CHAIN_MAP = {
    "SCI / Dignity Memorial": r"dignity memorial|funeral partners of america",
    "Carriage Services":      r"carriage services?|carriage funeral",
    "Foundation Partners":    r"foundation partners",
    "NorthStar Memorial":     r"northstar memorial",
    "Park Lawn":              r"\bpark lawn\b",
}
_SERVICES_MAP = {
    "Traditional Burial":   r"traditional burial",
    "Cremation":            r"cremation",
    "Aquamation":           r"aquamation|alkaline hydrolysis|water cremation",
    "Green/Natural Burial": r"green burial|natural burial|eco burial",
    "Pre-Planning":         r"pre-?plan(?:ning)?|preplanning",
    "Veteran Services":     r"veteran service|military honor",
    "Grief Support":        r"grief support|bereavement counsel",
    "Live Streaming":       r"live.?stream(?:ing)?",
    "Home Funeral":         r"home funeral",
    "Pet Cremation":        r"pet cremation|companion animal cremation",
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
}


class NFDACrawler(BaseCrawler):

    def __init__(self, db_path="data/firmable.db", log_dir="logs",
                 single_state=None, radius_miles=250):
        super().__init__(db_path=db_path, log_dir=log_dir)
        self.single_state = single_state
        self.radius_miles = radius_miles
        self._session: Optional[requests.Session] = None

    def get_dataset_name(self): return "nfda_funeral_homes"
    def get_source_url(self):   return RAL_URL
    def get_niche_fields(self):
        return ["nfda_member","green_funeral_cert","pursuit_of_excellence",
                "cremation_society","services_offered","ownership_type",
                "director_name","year_established","listing_url",
                "nfda_profile_id","city","state","zip_code"]

    def _get_session(self):
        if self._session is None:
            s = requests.Session()
            s.headers.update(_HEADERS)
            # Warm up — establishes cookies (dnn_IsMobile, __Secure-s, etc.)
            s.get(RAL_URL, timeout=30)
            self._session = s
        return self._session

    def _search(self, lat: float, lng: float, label: str, radius: int) -> Optional[str]:
        """
        POST the simplified fields that the PostRedirect page would submit.
        These are the SHORT field names (type, country, address, miles, latitude, longitude)
        discovered from the PostRedirect HTML response.
        """
        s = self._get_session()

        payload = {
            "type":      "Location",
            "country":   "221",          # United States
            "search":    "",
            "address":   label,
            "name":      "",
            "miles":     str(radius),
            "Green":     "False",
            "Persuit":   "False",
            "latitude":  str(lat),
            "longitude": str(lng),
            "PortalID":  "",
        }

        post_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer":       RAL_URL,
            "Origin":        "https://rallocator.nfda.org",
        }

        for attempt in range(4):
            try:
                resp = s.post(RAL_URL, data=payload,
                              headers=post_headers, timeout=45)
                resp.raise_for_status()
                return resp.text
            except requests.exceptions.HTTPError as exc:
                code = exc.response.status_code if exc.response else 0
                if code == 429:
                    wait = min(2 ** (attempt + 4), 120)
                    self.logger.warning(f"Rate-limited. Sleeping {wait}s...")
                    time.sleep(wait)
                elif code >= 500:
                    time.sleep(2 ** (attempt + 2))
                else:
                    self.logger.error(f"HTTP {code} for {label}")
                    return None
            except Exception as exc:
                wait = 2 ** (attempt + 2)
                self.logger.warning(f"Error attempt {attempt+1}/4: {exc}")
                time.sleep(wait)
        return None

    def crawl(self) -> List[str]:
        if self.single_state:
            abbr = self.single_state.upper().strip()
            if abbr in _STATE_CENTERS:
                lat, lng, label = _STATE_CENTERS[abbr]
                grid = [(lat, lng, label)]
                radius = max(self.radius_miles, 500)
            else:
                self.logger.warning(f"Unknown state '{abbr}', using full grid")
                grid = US_GRID
                radius = self.radius_miles
        else:
            grid = US_GRID
            radius = self.radius_miles

        results = []
        for i, (lat, lng, label) in enumerate(grid, 1):
            self.logger.info(f"  [{i}/{len(grid)}] {label} r={radius}mi...")
            html = self._search(lat, lng, label, radius)
            if html:
                count = html.count('class="xmp-location-listing')
                self.logger.info(f"    -> {count} listings")
                results.append(html)
            else:
                self.logger.warning(f"    -> failed")
            if i < len(grid):
                time.sleep(REQUEST_DELAY)

        return results

    def parse(self, raw_pages: List[str]) -> List[Dict]:
        parsed, seen_guids, seen_nz = [], set(), set()
        for html in raw_pages:
            soup = BeautifulSoup(html, "html.parser")
            for li in soup.select("li.xmp-location-listing"):
                try:
                    rec = self._parse_li(li)
                except Exception as exc:
                    self.stats["total_errors"] += 1
                    continue
                if not rec.get("company_name"):
                    continue
                guid = rec.get("nfda_profile_id", "")
                if guid:
                    if guid in seen_guids: continue
                    seen_guids.add(guid)
                nz = (rec.get("company_name") or "").lower() + "|" + (rec.get("zip_code") or "")
                if nz in seen_nz: continue
                seen_nz.add(nz)
                parsed.append(rec)
        self.logger.info(f"Total unique records: {len(parsed)}")
        return parsed

    def _parse_li(self, li) -> Dict[str, Any]:
        rec: Dict[str, Any] = {
            "company_name": None, "address": None, "city": None,
            "state": None, "zip_code": None, "phone": None,
            "website": None, "email": None,
            "nfda_member": "Yes", "green_funeral_cert": "No",
            "pursuit_of_excellence": "No", "cremation_society": "No",
            "services_offered": None, "ownership_type": "Independent",
            "director_name": None, "year_established": None,
            "listing_url": None, "nfda_profile_id": None,
        }
        rec["company_name"]    = (li.get("data-title")   or "").strip() or None
        rec["phone"]           = (li.get("data-phone")   or "").strip() or None
        rec["website"]         = (li.get("data-url")     or "").strip() or None
        rec["nfda_profile_id"] = (li.get("data-id")      or "").strip() or None

        raw_addr = (li.get("data-address") or "").strip()
        if raw_addr:
            self._parse_address(rec, raw_addr)

        name_low = (rec["company_name"] or "").lower()
        for chain, pat in _CHAIN_MAP.items():
            if re.search(pat, name_low):
                rec["ownership_type"] = chain; break

        inner = li.get_text(" ", strip=True).lower()
        if "green funeral" in inner or "green practices" in inner:
            rec["green_funeral_cert"] = "Yes"
        if "pursuit of excellence" in inner:
            rec["pursuit_of_excellence"] = "Yes"
        if "cremation society" in inner:
            rec["cremation_society"] = "Yes"

        svcs = [l for l, p in _SERVICES_MAP.items() if re.search(p, inner)]
        if svcs:
            rec["services_offered"] = " | ".join(svcs)

        ym = _YEAR_RE.search(inner) or _YEAR_SINCE.search(inner)
        if ym:
            try:
                yr = int(ym.group(1))
                if 1800 <= yr <= 2025:
                    rec["year_established"] = str(yr)
            except ValueError:
                pass
        return rec

    def _parse_address(self, rec: Dict, raw: str) -> None:
        addr = re.sub(r",?\s*US\s*$", "", raw.strip()).strip()
        parts = [p.strip() for p in addr.split(",") if p.strip()]

        if len(parts) >= 3:
            rec["address"] = parts[0]
            # Last part often contains zip
            zip_m = re.search(r"(\d{5}(?:-\d{4})?)", parts[-1])
            if zip_m:
                rec["zip_code"] = zip_m.group(1)
            # Second-to-last or last often contains state
            for p in reversed(parts):
                sm = re.search(r"\b(" + _STATE_ABBRS + r")\b", p)
                if sm:
                    rec["state"] = sm.group(1)
                    break
            # City is middle part
            city_part = parts[1]
            city_clean = re.sub(r"\b(" + _STATE_ABBRS + r")\b", "", city_part).strip().strip(",")
            rec["city"] = city_clean or city_part
        elif len(parts) == 2:
            rec["address"] = parts[0]
            zip_m = re.search(r"(\d{5}(?:-\d{4})?)", parts[1])
            if zip_m: rec["zip_code"] = zip_m.group(1)
            sm = re.search(r"\b(" + _STATE_ABBRS + r")\b", parts[1])
            if sm: rec["state"] = sm.group(1)
        else:
            rec["address"] = addr
            zip_m = re.search(r"(\d{5}(?:-\d{4})?)", addr)
            if zip_m: rec["zip_code"] = zip_m.group(1)
            sm = re.search(r"\b(" + _STATE_ABBRS + r")\b", addr)
            if sm: rec["state"] = sm.group(1)


def _debug_mode():
    print("=" * 70)
    print("NFDA DEBUG — testing simplified POST (PostRedirect fields)")
    print("=" * 70)
    s = requests.Session()
    s.headers.update(_HEADERS)

    print("\n[1] Warming up session (GET homepage)...")
    r0 = s.get(RAL_URL, timeout=30)
    print(f"  HTTP {r0.status_code}, cookies: {list(s.cookies.keys())}")

    print("\n[2] POSTing simplified fields (Texas, 500mi)...")
    payload = {
        "type": "Location", "country": "221", "search": "",
        "address": "Texas, USA", "name": "", "miles": "500",
        "Green": "False", "Persuit": "False",
        "latitude": "31.968598800000002", "longitude": "-99.9018131",
        "PortalID": "",
    }
    r2 = s.post(RAL_URL, data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded",
                         "Referer": RAL_URL, "Origin": "https://rallocator.nfda.org"},
                timeout=45)
    print(f"  HTTP {r2.status_code}, Size: {len(r2.text)} chars")
    print(f"  Final URL: {r2.url}")

    soup = BeautifulSoup(r2.text, "html.parser")
    listings = soup.select("li.xmp-location-listing")
    print(f"  Listings found: {len(listings)}")

    # If still PostRedirect, show it
    if "PostRedirect" in r2.text or len(r2.text) < 5000:
        print(f"\n  Response preview (first 1000 chars):")
        print(r2.text[:1000])
    else:
        for li in listings[:3]:
            print(f"\n  name   : {li.get('data-title')}")
            print(f"  phone  : {li.get('data-phone')}")
            print(f"  address: {li.get('data-address')}")
            print(f"  website: {li.get('data-url')}")
    print("\n[DEBUG COMPLETE]")


def _parse_args():
    p = argparse.ArgumentParser(description="Crawler #01 -- NFDA Funeral Homes")
    p.add_argument("--debug",  action="store_true")
    p.add_argument("--state",  default=None, metavar="ST")
    p.add_argument("--radius", default=250, type=int)
    p.add_argument("--db",     default="data/firmable.db")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.debug:
        _debug_mode(); sys.exit(0)

    print("=" * 70)
    print("Crawler #01 -- NFDA Member Funeral Homes")
    print(f"Source  : {RAL_URL}")
    print(f"Mode    : {'State: ' + args.state.upper() if args.state else 'Full US (' + str(len(US_GRID)) + ' grid points)'}")
    print(f"Radius  : {args.radius}mi | DB: {args.db}")
    print("=" * 70)

    crawler = NFDACrawler(db_path=args.db, single_state=args.state, radius_miles=args.radius)
    stats   = crawler.run()
    csv_out = crawler.export_csv()

    print("\n-- Run Summary " + "-"*55)
    for k, v in stats.items():
        print(f"  {k:<28}: {v}")
    if csv_out:
        print(f"\n  CSV -> {csv_out}")
    print("-" * 70)