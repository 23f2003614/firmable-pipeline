"""
Crawler 15 — NARI Certified Remodelers
Source   : https://remodelingdoneright.nari.org/find-a-remodeler-search/
Records  : ~2,423 | US

Fetch    : NARI search API with 61 seed anchors — one per US state plus
           extras for dense metros (NYC, LA, Chicago). Uses miles=50,
           pageSize=100 per call. Deduplicates by organizationId.

Parse    : All data in the search response — no profile page visits needed.
           Converts list-type fields to comma-separated strings.

           Extra metro anchors — a single state-center point misses
           thousands of members concentrated in dense urban areas.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from base_crawler import BaseCrawler
from typing import Any, List, Dict

# ------------------------------------------------------------------
# Confirmed API endpoint (from DevTools Headers tab)
# ------------------------------------------------------------------
SEARCH_API = (
    "https://remodelingdoneright.nari.org"
    "/api/OrganizationSearch/SearchOrganizations"
)

# ------------------------------------------------------------------
# Seed anchors: (zip, lat, lng, label)
# 50-mile radius from each gives full national coverage
# ------------------------------------------------------------------
SEED_ANCHORS = [
    ("35203",  33.5186,  -86.8104, "Birmingham AL"),
    ("99501",  61.2181, -149.9003, "Anchorage AK"),
    ("85001",  33.4484, -112.0740, "Phoenix AZ"),
    ("72201",  34.7465,  -92.2896, "Little Rock AR"),
    ("90001",  34.0522, -118.2437, "Los Angeles CA"),
    ("94102",  37.7749, -122.4194, "San Francisco CA"),
    ("92101",  32.7157, -117.1611, "San Diego CA"),
    ("80202",  39.7392, -104.9903, "Denver CO"),
    ("06103",  41.7658,  -72.6851, "Hartford CT"),
    ("20001",  38.9072,  -77.0369, "Washington DC"),
    ("19901",  39.1582,  -75.5244, "Dover DE"),
    ("33101",  25.7617,  -80.1918, "Miami FL"),
    ("32801",  28.5383,  -81.3792, "Orlando FL"),
    ("32501",  30.4213,  -87.2169, "Pensacola FL"),
    ("30301",  33.7490,  -84.3880, "Atlanta GA"),
    ("96801",  21.3069, -157.8583, "Honolulu HI"),
    ("83701",  43.6150, -116.2023, "Boise ID"),
    ("60601",  41.8781,  -87.6298, "Chicago IL"),
    ("46201",  39.7684,  -86.1581, "Indianapolis IN"),
    ("50301",  41.5868,  -93.6250, "Des Moines IA"),
    ("66101",  39.1155,  -94.6268, "Kansas City KS"),
    ("40201",  38.2527,  -85.7585, "Louisville KY"),
    ("70112",  29.9511,  -90.0715, "New Orleans LA"),
    ("04101",  43.6591,  -70.2568, "Portland ME"),
    ("21201",  39.2904,  -76.6122, "Baltimore MD"),
    ("02101",  42.3601,  -71.0589, "Boston MA"),
    ("48201",  42.3314,  -83.0458, "Detroit MI"),
    ("55401",  44.9778,  -93.2650, "Minneapolis MN"),
    ("39201",  32.2988,  -90.1848, "Jackson MS"),
    ("63101",  38.6270,  -90.1994, "St. Louis MO"),
    ("59101",  45.7833, -108.5007, "Billings MT"),
    ("68102",  41.2565,  -95.9345, "Omaha NE"),
    ("89101",  36.1699, -115.1398, "Las Vegas NV"),
    ("03101",  42.9956,  -71.4548, "Manchester NH"),
    ("07102",  40.7357,  -74.1724, "Newark NJ"),
    ("87101",  35.0853, -106.6056, "Albuquerque NM"),
    ("10001",  40.7484,  -73.9967, "New York NY"),
    ("14201",  42.8864,  -78.8784, "Buffalo NY"),
    ("27601",  35.7796,  -78.6382, "Raleigh NC"),
    ("58102",  46.8772,  -96.7898, "Fargo ND"),
    ("44101",  41.4993,  -81.6944, "Cleveland OH"),
    ("43215",  39.9612,  -82.9988, "Columbus OH"),
    ("73102",  35.4676,  -97.5164, "Oklahoma City OK"),
    ("97201",  45.5051, -122.6750, "Portland OR"),
    ("19101",  39.9526,  -75.1652, "Philadelphia PA"),
    ("15201",  40.4406,  -79.9959, "Pittsburgh PA"),
    ("02901",  41.8240,  -71.4128, "Providence RI"),
    ("29201",  34.0007,  -81.0348, "Columbia SC"),
    ("57101",  43.5473,  -96.7283, "Sioux Falls SD"),
    ("37201",  36.1627,  -86.7816, "Nashville TN"),
    ("75201",  32.7767,  -96.7970, "Dallas TX"),
    ("77001",  29.7604,  -95.3698, "Houston TX"),
    ("78201",  29.4241,  -98.4936, "San Antonio TX"),
    ("84101",  40.7608, -111.8910, "Salt Lake City UT"),
    ("05401",  44.4759,  -73.2121, "Burlington VT"),
    ("23219",  37.5407,  -77.4360, "Richmond VA"),
    ("22201",  38.8816,  -77.0910, "Arlington VA"),
    ("98101",  47.6062, -122.3321, "Seattle WA"),
    ("25301",  38.3498,  -81.6326, "Charleston WV"),
    ("53201",  43.0389,  -87.9065, "Milwaukee WI"),
    ("82001",  41.1400, -104.8202, "Cheyenne WY"),
]


class NARIRemodelersCrawler(BaseCrawler):

    def get_dataset_name(self) -> str:
        return "nari_remodelers"

    def get_source_url(self) -> str:
        return "https://remodelingdoneright.nari.org/find-a-remodeler-search/"

    def get_niche_fields(self) -> List[str]:
        return [
            "organization_id",
            "specialties",
            "member_type",        # subOrgType: Remodeler / Specialty Contractor / Vendor
            "membership_length",  # e.g. "6-10 years"
            "certifications",
            "nari_accredited_since",
            "chapter",
        ]

    # ----------------------------------------------------------------
    # crawl() — single phase, search API only
    # ----------------------------------------------------------------
    def crawl(self) -> Any:
        all_orgs: Dict[str, Dict] = {}
        total = len(SEED_ANCHORS)

        for idx, (zip_code, lat, lng, label) in enumerate(SEED_ANCHORS, 1):
            self.logger.info(
                f"  [{idx}/{total}] {label} | unique={len(all_orgs)}"
            )
            page = 1
            while True:
                params = {
                    "orgType":     "Remodeler",
                    "zip":         zip_code,
                    "miles":       50,
                    "companyName": "",
                    "lat":         lat,
                    "lng":         lng,
                    "sortBy":      "relevance",
                    "page":        page,
                    "pageSize":    100,
                }
                resp = self._safe_request(SEARCH_API, params=params)
                if resp is None:
                    self.logger.warning(f"    No response {label} page={page}")
                    break

                try:
                    data = resp.json()
                except Exception:
                    self.logger.warning(
                        f"    Non-JSON {label} "
                        f"status={getattr(resp,'status_code','?')}"
                    )
                    break

                results     = data.get("results") or []
                pagination  = data.get("pagination") or {}
                total_pages = int(pagination.get("totalPages") or 1)

                new_count = 0
                for org in results:
                    oid = str(org.get("organizationId") or "").strip()
                    if oid and oid not in all_orgs:
                        all_orgs[oid] = org
                        new_count += 1

                self.logger.debug(
                    f"    page={page}/{total_pages} "
                    f"got={len(results)} new={new_count}"
                )

                if page >= total_pages or not results:
                    break
                page += 1
                self._rate_limit(1.0)

            self._rate_limit(0.8)

        self.logger.info(f"Crawl done — {len(all_orgs)} unique orgs.")
        return list(all_orgs.values())

    # ----------------------------------------------------------------
    # parse() — normalise to standard schema
    # ----------------------------------------------------------------
    def parse(self, raw_data: Any) -> List[Dict]:

        def coerce_list(val):
            """Convert list-of-dicts or list-of-strings to CSV string."""
            if not val:
                return None
            if isinstance(val, list):
                return ", ".join(
                    (item["name"] if isinstance(item, dict) else str(item))
                    for item in val if item
                ) or None
            return str(val) or None

        records = []
        for org in raw_data:
            records.append({
                # Standard fields
                "company_name": org.get("name"),
                "address":      org.get("address") or org.get("address1"),
                "city":         org.get("city"),
                "state":        org.get("state"),
                "zip":          org.get("zip") or org.get("postalCode"),
                "phone":        org.get("phone"),
                "email":        org.get("email"),
                "website":      org.get("website") or org.get("websiteUrl"),
                # Niche fields
                "organization_id":       str(org.get("organizationId") or ""),
                "specialties":           coerce_list(org.get("specialties")),
                "member_type":           coerce_list(org.get("subOrgType")),
                "membership_length":     coerce_list(
                    org.get("membershipLength") or org.get("membershipLengths")
                ),
                "certifications":        coerce_list(org.get("certifications")),
                "nari_accredited_since": org.get("nariAccreditedSince")
                                         or org.get("accreditedSince"),
                "chapter":               org.get("chapter") or org.get("chapterName"),
                "description":           org.get("description"),
            })
        return records


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------
if __name__ == "__main__":
    crawler = NARIRemodelersCrawler()
    stats = crawler.run()
    print("\n=== NARI Crawl Stats ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    csv_path = crawler.export_csv()
    if csv_path:
        print(f"\nCSV → {csv_path}")