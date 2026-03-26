"""
Crawler #8 — US Public Water Utilities (EPA SDWIS / Envirofacts)
=================================================================
Source  : EPA Envirofacts WATER_SYSTEM table (SDWIS/Fed)
API     : https://data.epa.gov/efservice/WATER_SYSTEM/
Docs    : https://www.epa.gov/enviro/envirofacts-data-service-api-v1

Strategy
--------
• Query Envirofacts CSV per US state  — fast, reliable, no auth needed
• Filter active CWS in Python         — single-column URL is stable
• 50 states crawled sequentially; MAX_RECORDS caps for demo runs

"""

import sys
import os
import csv
import io

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from base_crawler import BaseCrawler
from typing import Dict, List

_EF_BASE = "https://data.epa.gov/efservice"

# Set None to collect all ~51k records (~8 min). 5000 is good for demo.
MAX_RECORDS = 5000

_US_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
]


class AWWAWaterUtilitiesCrawler(BaseCrawler):

    def get_dataset_name(self) -> str:
        return "awwa_water_utilities"

    def get_source_url(self) -> str:
        return "https://data.epa.gov/efservice/WATER_SYSTEM"

    def get_niche_fields(self) -> List[str]:
        return [
            "pwsid", "pws_type", "owner_type", "primary_source",
            "population_served", "service_connections",
            "cities_served", "county", "epa_region", "gw_sw_code",
        ]

    # ------------------------------------------------------------------ crawl

    def crawl(self) -> List[Dict]:
        """
        Download WATER_SYSTEM CSV per state from Envirofacts.
        Single-column filter /state_code/{ST} is stable for all 50 states.
        Returns a flat list of raw row-dicts (all PWS types, all statuses).
        Filtering to active CWS happens inside parse().
        """
        all_rows: List[Dict] = []
        collected = 0

        for state in _US_STATES:
            if MAX_RECORDS and collected >= MAX_RECORDS:
                self.logger.info(f"MAX_RECORDS={MAX_RECORDS:,} reached — stopping.")
                break

            rows = self._fetch_state(state)
            all_rows.extend(rows)
            collected += len(rows)
            self.logger.info(
                f"  {state}: {len(rows):>5} rows fetched | "
                f"running total = {collected:,}"
            )
            self._rate_limit(0.25)

        self.logger.info(f"Crawl complete: {len(all_rows):,} raw rows.")
        return all_rows                          # plain list — no tuple wrapper

    def _fetch_state(self, state: str) -> List[Dict]:
        """GET /efservice/WATER_SYSTEM/state_code/{STATE}/CSV"""
        url = f"{_EF_BASE}/WATER_SYSTEM/state_code/{state}/CSV"
        resp = self._safe_request(url)
        if resp is None:
            self.logger.warning(f"  {state}: request failed, skipping.")
            return []
        try:
            text = resp.content.decode("utf-8", errors="replace")
            reader = csv.DictReader(io.StringIO(text))
            return [dict(r) for r in reader]
        except Exception as e:
            self.logger.warning(f"  {state}: CSV parse error — {e}")
            return []

    # ----------------------------------------------------------------- parse

    def parse(self, raw_data: List[Dict]) -> List[Dict]:
        """
        Filter raw rows to active Community Water Systems only,
        then map Envirofacts column names to the project schema.

        Actual CSV headers from Envirofacts (all lowercase):
          pwsid, pws_name, pws_activity_code, pws_type_code,
          owner_type_code, population_served_count,
          service_connections_count, primary_source_code,
          gw_sw_code, epa_region, org_name, admin_name,
          email_addr, phone_number, address_line1, address_line2,
          city_name, zip_code, state_code, cities_served,
          counties_served, ...
        """
        # ── guard: log first row keys so we can diagnose any future mismatch ─
        if raw_data:
            self.logger.debug(f"Sample row keys: {list(raw_data[0].keys())[:12]}")

        out: List[Dict] = []

        for r in raw_data:
            # Keep active (A) Community Water Systems (CWS) only
            activity = r.get("pws_activity_code", "").strip().upper()
            pws_type = r.get("pws_type_code", "").strip().upper()

            if activity != "A" or pws_type != "CWS":
                continue

            name = (r.get("pws_name") or r.get("org_name") or "").strip()
            if not name:
                continue

            def c(col: str):
                """Return cleaned value or None."""
                v = r.get(col)
                if v is None:
                    return None
                s = str(v).strip()
                return None if s in ("", "None", "N/A", "null", "NULL", "nan") else s

            out.append({
                # standard fields
                "company_name":        name,
                "address":             c("address_line1"),
                "city":                c("city_name"),
                "state":               c("state_code"),
                "zip":                 c("zip_code"),
                "phone":               c("phone_number"),
                "email":               c("email_addr"),
                "website":             None,
                "country":             "US",
                # niche fields
                "pwsid":               c("pwsid"),
                "pws_type":            c("pws_type_code"),
                "owner_type":          c("owner_type_code"),
                "primary_source":      c("primary_source_code"),
                "population_served":   c("population_served_count"),
                "service_connections": c("service_connections_count"),
                "cities_served":       c("cities_served"),
                "county":              c("counties_served"),
                "epa_region":          c("epa_region"),
                "gw_sw_code":          c("gw_sw_code"),
            })

        self.logger.info(
            f"parse(): {len(raw_data):,} raw rows -> {len(out):,} active CWS records."
        )
        return out


# ----------------------------------------------------------------- entry point

if __name__ == "__main__":
    crawler = AWWAWaterUtilitiesCrawler(
        db_path="data/firmable.db",
        log_dir="logs",
    )

    stats = crawler.run()

    print("\n" + "=" * 55)
    print("AWWA / EPA SDWIS — Run Summary")
    print("=" * 55)
    for k, v in stats.items():
        print(f"  {k:<24}: {v:,}")
    print("=" * 55)

    csv_path = crawler.export_csv()
    if csv_path:
        print(f"\n  CSV  →  {csv_path}")
    else:
        print("\n  (No CSV written — 0 records or table missing)")