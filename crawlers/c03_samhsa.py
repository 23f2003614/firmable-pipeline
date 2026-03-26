"""
Crawler #3: SAMHSA Treatment Facility Locator
Source : https://findtreatment.gov
API    : https://findtreatment.gov/locator/exportsAsJson/v2
~17,000+ facilities across all US states + DC + territories.

Key improvements over v1:
  - Uses limitType=0 (state-exact search) instead of 500-mile radius,
    completely eliminating cross-state record overlap / duplicates.
  - Captures ALL available service-code fields from the API:
      TC   Type of Care
      SET  Service Setting
      PAY  Payment / Insurance Accepted
      SG   Special Programs / Groups Offered
      SL   Language Services
      FOP  Facility Operation
      AGE  Age Groups Accepted          ← NEW
      EMS  Emergency Mental Health Svcs ← NEW
      PYAS Payment Assistance Available ← NEW
  - Captures top-level fields:
      type_facility  (SA / MH / both)   ← NEW
      latitude / longitude              ← NEW (enables geo queries)
      intake_phone   (intake hotline)   ← NEW (separate from main phone)
      hotline        (crisis hotline)   ← NEW
      address2       (street line 2)    ← NEW
  - pageSize=200 (max safe) instead of 100 — cuts API calls in half.
  - Thread-safe, state-partitioned, dedup by name+address+state.
"""

import sys, os, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crawlers.base_crawler import BaseCrawler

# State ID table from the official SAMHSA API documentation (Table 3).
# These IDs are used with limitType=0 for exact state-scoped queries —
# no radius overlap, no cross-state duplicates.
STATE_ID_MAP = {
    'ME':  1,  'MI':  2,  'MA':  3,  'WA': 15,  'WI': 16,  'MD': 18,
    'AL': 19,  'AK': 20,  'AZ': 21,  'AR': 22,  'CA': 23,  'CO': 24,
    'CT': 25,  'DE': 26,  'DC': 27,  'FL': 28,  'GA': 29,  'HI': 30,
    'ID': 31,  'IL': 32,  'IN': 33,  'IA': 34,  'KS': 35,  'KY': 36,
    'LA': 37,  'MN': 38,  'MS': 39,  'MO': 40,  'MT':  4,  'NE': 41,
    'NV': 12,  'NH': 42,  'NJ': 13,  'NM': 43,  'NY':  5,  'NC':  6,
    'ND': 44,  'OH':  7,  'OK': 45,  'OR': 46,  'PA':  8,  'RI':  9,
    'SC': 47,  'SD': 48,  'TN': 10,  'TX': 11,  'UT': 14,  'VT': 49,
    'VA': 50,  'WV': 51,  'WY': 52,
}

# A representative coordinate per state — used as sAddr anchor for limitType=0.
# The API only needs this to resolve "which state"; exact position doesn't matter
# when limitType=0 is used (it returns ALL facilities in the state).
STATE_COORDS = {
    'AL': (32.806671, -86.791130), 'AK': (61.370716, -152.404419),
    'AZ': (33.729759, -111.431221), 'AR': (34.969704, -92.373123),
    'CA': (36.116203, -119.681564), 'CO': (39.059811, -105.311104),
    'CT': (41.597782, -72.755371),  'DE': (39.318523, -75.507141),
    'DC': (38.897438, -77.026817),  'FL': (27.766279, -81.686783),
    'GA': (33.040619, -83.643074),  'HI': (21.094318, -157.498337),
    'ID': (44.240459, -114.478828), 'IL': (40.349457, -88.986137),
    'IN': (39.849426, -86.258278),  'IA': (42.011539, -93.210526),
    'KS': (38.526600, -96.726486),  'KY': (37.668140, -84.670067),
    'LA': (31.169960, -91.867805),  'ME': (44.693947, -69.381927),
    'MD': (39.063946, -76.802101),  'MA': (42.230171, -71.530106),
    'MI': (43.326618, -84.536095),  'MN': (45.694454, -93.900192),
    'MS': (32.741646, -89.678696),  'MO': (38.456085, -92.288368),
    'MT': (46.921925, -110.454353), 'NE': (41.125370, -98.268082),
    'NV': (38.313515, -117.055374), 'NH': (43.452492, -71.563896),
    'NJ': (40.298904, -74.521011),  'NM': (34.840515, -106.248482),
    'NY': (42.165726, -74.948051),  'NC': (35.630066, -79.806419),
    'ND': (47.528912, -99.784012),  'OH': (40.388783, -82.764915),
    'OK': (35.565342, -96.928917),  'OR': (44.572021, -122.070938),
    'PA': (40.590752, -77.209755),  'RI': (41.680893, -71.511780),
    'SC': (33.856892, -80.945007),  'SD': (44.299782, -99.438828),
    'TN': (35.747845, -86.692345),  'TX': (31.054487, -97.563461),
    'UT': (40.150032, -111.862434), 'VT': (44.045876, -72.710686),
    'VA': (37.769337, -78.169968),  'WA': (47.400902, -121.490494),
    'WV': (38.491226, -80.954453),  'WI': (44.268543, -89.616508),
    'WY': (42.755966, -107.302490),
}

API_URL = "https://findtreatment.gov/locator/exportsAsJson/v2"


class SamhsaCrawler(BaseCrawler):

    def get_dataset_name(self):
        return "samhsa_treatment_facilities"

    def get_source_url(self):
        return "https://findtreatment.gov"

    def get_niche_fields(self):
        return [
            # Service-code fields (from services array)
            'type_of_care',          # TC  — e.g. "Substance use treatment"
            'service_setting',       # SET — e.g. "Outpatient; Residential"
            'payment_accepted',      # PAY — insurance / funding sources
            'special_programs',      # SG  — target populations served
            'languages',             # SL  — language / interpreter services
            'facility_operation',    # FOP — private non-profit / for-profit / govt
            'age_groups_accepted',   # AGE — children / adolescents / adults / seniors
            'emergency_services',    # EMS — crisis intervention, etc.
            'payment_assistance',    # PYAS — sliding scale, free care, etc.
            # Top-level fields
            'type_facility',         # SA / MH / both
            'intake_phone',          # Separate intake hotline number
            'hotline',               # Crisis hotline number
            'address2',              # Street address line 2
            'latitude',              # GPS latitude  (enables geo queries)
            'longitude',             # GPS longitude (enables geo queries)
        ]

    # ------------------------------------------------------------------
    # State fetch — uses limitType=0 (exact state boundary, no overlap)
    # ------------------------------------------------------------------
    def _fetch_state(self, state_code, lat, lng):
        import requests
        state_id = STATE_ID_MAP.get(state_code)
        if not state_id:
            self.logger.warning(f"  No state ID for {state_code}, skipping.")
            return state_code, []

        state_records = []
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Accept': 'application/json',
            'Referer': 'https://findtreatment.gov',
        }

        page = 1
        while True:
            params = {
                'sAddr':     f'{lat},{lng}',
                'sType':     'both',    # SA + MH — captures all facility types
                'limitType': 0,         # State-scoped — NO cross-state overlap
                'limitValue': state_id, # Official state ID from API docs Table 3
                'pageSize':  200,       # Max safe page size (API limit)
                'page':      page,
                'sort':      0,
            }
            try:
                resp = requests.get(API_URL, params=params,
                                    headers=headers, timeout=30)
                if not resp.ok:
                    self.logger.warning(
                        f"  {state_code} p{page}: HTTP {resp.status_code}"
                    )
                    break
                data = resp.json()
            except Exception as e:
                self.logger.warning(f"  {state_code} p{page}: {e}")
                break

            rows = data.get('rows', [])
            total_pages = data.get('totalPages', 1)

            if not rows:
                break

            state_records.extend(rows)

            if page >= total_pages:
                break
            page += 1
            time.sleep(0.15)   # polite delay between pages

        return state_code, state_records

    # ------------------------------------------------------------------
    # Crawl — parallel across all states
    # ------------------------------------------------------------------
    def crawl(self):
        all_records = []
        lock = Lock()
        completed = [0]
        seen = set()   # global dedup by name+address+zip

        self.logger.info(
            f"Fetching {len(STATE_COORDS)} states using "
            f"limitType=0 (exact state scope, no radius overlap) …"
        )

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(self._fetch_state, state, lat, lng): state
                for state, (lat, lng) in STATE_COORDS.items()
            }
            for future in as_completed(futures):
                try:
                    state, records = future.result()
                    with lock:
                        new = []
                        for r in records:
                            key = (
                                str(r.get('name1', '')).strip().lower() +
                                str(r.get('street1', '')).strip().lower() +
                                str(r.get('zip', '')).strip()
                            )
                            if key and key not in seen:
                                seen.add(key)
                                new.append(r)
                        all_records.extend(new)
                        completed[0] += 1
                        self.logger.info(
                            f"  [{completed[0]:>2}/{len(STATE_COORDS)}] "
                            f"{state}: {len(records)} fetched, "
                            f"{len(new)} new  (running total={len(all_records)})"
                        )
                except Exception as e:
                    self.logger.warning(f"  {futures[future]} failed: {e}")

        self.logger.info(f"Crawl complete. Raw unique records: {len(all_records)}")
        return all_records

    # ------------------------------------------------------------------
    # Parse — maps raw API JSON → flat record dict
    # ------------------------------------------------------------------
    def parse(self, raw_data):
        records = []
        for item in (raw_data or []):
            try:
                name = (item.get('name1') or '').strip()
                if not name:
                    continue

                # Combine sub-unit name if present
                name2 = (item.get('name2') or '').strip()
                full_name = f"{name} - {name2}" if name2 else name

                # Helper: extract a service field by its f2 code
                services = item.get('services', []) or []
                def get_svc(code):
                    return '; '.join(
                        s.get('f3', '').strip()
                        for s in services
                        if s.get('f2', '').upper() == code.upper()
                        and s.get('f3', '').strip()
                    ) or ''

                records.append({
                    # ── Core identity ──────────────────────────────────
                    'company_name':       full_name,
                    'address':            (item.get('street1') or '').strip(),
                    'address2':           (item.get('street2') or '').strip(),
                    'city':               (item.get('city')    or '').strip(),
                    'state':              (item.get('state')   or '').strip(),
                    'zip_code':           (item.get('zip')     or '').strip(),
                    'country':            'US',

                    # ── Contact ────────────────────────────────────────
                    'phone':              (item.get('phone')   or '').strip(),
                    'intake_phone':       (item.get('intake1') or '').strip(),
                    'hotline':            (item.get('hotline1') or '').strip(),
                    'email':              '',   # API does not expose email
                    'website':            (item.get('website') or '').strip(),

                    # ── Geo ────────────────────────────────────────────
                    'latitude':           (item.get('latitude')  or '').strip(),
                    'longitude':          (item.get('longitude') or '').strip(),

                    # ── Facility meta ──────────────────────────────────
                    # type_facility: 'SA' = substance abuse,
                    #                'MH' = mental health,
                    #                'BOTH' / other = combined
                    'type_facility':      (item.get('type_facility') or '').strip(),

                    # ── Niche / service-code fields ────────────────────
                    'type_of_care':       get_svc('TC'),   # e.g. "Substance use treatment"
                    'service_setting':    get_svc('SET'),  # e.g. "Outpatient; Residential"
                    'payment_accepted':   get_svc('PAY'),  # insurance / funding
                    'special_programs':   get_svc('SG'),   # target populations
                    'languages':          get_svc('SL'),   # interpreter services
                    'facility_operation': get_svc('FOP'),  # private / public / non-profit
                    'age_groups_accepted':get_svc('AGE'),  # children / adults / seniors
                    'emergency_services': get_svc('EMS'),  # crisis intervention, etc.
                    'payment_assistance': get_svc('PYAS'), # sliding scale / free care
                })
            except Exception:
                self.stats['total_errors'] += 1

        self.logger.info(f"Parsed {len(records)} records")
        return records


# ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    crawler = SamhsaCrawler()
    stats = crawler.run()
    print(f"\n{'='*42}")
    print(f"  Parsed      : {stats['total_parsed']}")
    print(f"  New records : {stats['total_new']}")
    print(f"  Updated     : {stats['total_updated']}")
    print(f"  Duplicates  : {stats['total_duplicates']}")
    print(f"  Errors      : {stats['total_errors']}")
    print(f"{'='*42}")
    if stats['total_new'] + stats['total_updated'] > 0:
        path = crawler.export_csv()
        if path:
            print(f"  CSV saved   : {path}")