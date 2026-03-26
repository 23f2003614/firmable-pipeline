"""
Crawler #6: Brewers Association Craft Breweries (US only)
Source : https://www.brewersassociation.org/directories/breweries/
Data   : Single JSON endpoint (~12k records)

}
"""

import sys, os, re, time, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crawlers.base_crawler import BaseCrawler


US_STATES = {
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN',
    'IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV',
    'NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN',
    'TX','UT','VT','VA','WA','WV','WI','WY','DC','PR','VI','GU','AS','MP',
}

_REGION_MAP = {
    'Northeast':   {'CT','ME','MA','NH','NJ','NY','PA','RI','VT'},
    'Mid-Atlantic':{'DE','MD','VA','WV','DC'},
    'Southeast':   {'AL','AR','FL','GA','KY','LA','MS','NC','SC','TN'},
    'Midwest':     {'IL','IN','IA','KS','MI','MN','MO','NE','ND','OH','SD','WI'},
    'Southwest':   {'AZ','NM','OK','TX'},
    'Mountain':    {'CO','ID','MT','NV','UT','WY'},
    'Pacific':     {'AK','CA','HI','OR','WA'},
}

def _state_to_region(state_code: str) -> str:
    for region, states in _REGION_MAP.items():
        if state_code in states:
            return region
    return ''


class BrewersAssocCrawler(BaseCrawler):

    JSON_URL = (
        "https://www.brewersassociation.org/wp-content/themes/"
        "ba2019/json-store/breweries/breweries.json"
    )

    def get_dataset_name(self):
        return "brewers_association"

    def get_source_url(self):
        return "https://www.brewersassociation.org/directories/breweries/"

    def _drop_stale_table(self):
        """Drop old table if schema is missing new columns. Rebuilds cleanly."""
        import sqlite3
        if not os.path.exists(self.db_path):
            return
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (self.get_dataset_name(),)
        )
        if cursor.fetchone():
            cursor.execute(f'PRAGMA table_info("{self.get_dataset_name()}")')
            existing_cols = {row[1] for row in cursor.fetchall()}
            required = {'state_full', 'is_craft_brewery', 'membership_status',
                        'membership_type', 'voting_member', 'account_badges',
                        'latitude', 'longitude', 'geocode_accuracy'}
            if not required.issubset(existing_cols):
                self.logger.info("Schema outdated - dropping old table for clean rebuild...")
                cursor.execute(f'DROP TABLE "{self.get_dataset_name()}"')
                conn.commit()
                self.logger.info("Old table dropped. Will re-insert all records.")
        conn.close()

    def run(self):
        self._drop_stale_table()
        return super().run()

    def get_niche_fields(self):
        return [
            'brewery_type', 'is_craft_brewery', 'independence_certified',
            'membership_status', 'membership_type', 'voting_member',
            'account_badges', 'year_established',
            'latitude', 'longitude', 'geocode_accuracy', 'region',
        ]

    # ------------------------------------------------------------------
    # CRAWL — streaming download with retries
    # ------------------------------------------------------------------
    def crawl(self):
        import requests

        self.logger.info(f"Streaming brewery JSON -> {self.JSON_URL}")

        headers = {
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/124.0.0.0 Safari/537.36'
            ),
            'Accept': 'application/json, */*;q=0.8',
            'Accept-Encoding': 'identity',
            'Referer': 'https://www.brewersassociation.org/directories/breweries/',
            'Connection': 'keep-alive',
        }

        params = {'nocache': int(time.time() * 1000)}

        for attempt in range(1, 5):
            try:
                self.logger.info(f"  Attempt {attempt}/4 ...")
                resp = requests.get(
                    self.JSON_URL, params=params, headers=headers,
                    timeout=120, stream=True,
                )
                resp.raise_for_status()

                chunks = []
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        chunks.append(chunk)

                raw_bytes = b''.join(chunks)
                self.logger.info(f"  Downloaded {len(raw_bytes):,} bytes")

                data = json.loads(raw_bytes.decode('utf-8'))
                self.logger.info(f"  JSON records: {len(data):,}")

                if data:
                    self.logger.info(f"  Keys in record[0]: {list(data[0].keys())}")
                    if 'BillingAddress' in data[0] and data[0]['BillingAddress']:
                        self.logger.info(f"  BillingAddress keys: {list(data[0]['BillingAddress'].keys())}")

                return data

            except Exception as e:
                wait = 2 ** attempt
                self.logger.warning(f"  Attempt {attempt} failed: {e} — retrying in {wait}s")
                time.sleep(wait)
                params = {'nocache': int(time.time() * 1000)}

        self.logger.error("All download attempts exhausted.")
        return []

    # ------------------------------------------------------------------
    # PARSE — correct field mapping from confirmed JSON structure
    # ------------------------------------------------------------------
    def parse(self, raw_data):
        if not raw_data:
            return []

        items = raw_data if isinstance(raw_data, list) else list(raw_data.values())
        records = []
        skipped_non_us = 0

        for item in items:
            try:
                if not isinstance(item, dict):
                    continue

                # ── Name ──────────────────────────────────────────────
                name = str(item.get('Name') or '').strip()
                if not name:
                    continue

                # ── BillingAddress (nested) ────────────────────────────
                billing = item.get('BillingAddress') or {}

                street     = str(billing.get('street')      or '').strip()
                city       = str(billing.get('city')        or '').strip()
                state_full = str(billing.get('state')       or '').strip()
                state      = str(billing.get('stateCode')   or '').strip().upper()
                zipcode    = str(billing.get('postalCode')  or '').strip()
                country_code = str(billing.get('countryCode') or 'US').strip().upper()
                lat        = str(billing.get('latitude')    or '').strip()
                lon        = str(billing.get('longitude')   or '').strip()
                geo_acc    = str(billing.get('geocodeAccuracy') or '').strip()

                # ── US-only filter ─────────────────────────────────────
                if country_code and country_code not in ('US', 'USA', ''):
                    skipped_non_us += 1
                    continue
                if state and state not in US_STATES and len(state) == 2:
                    skipped_non_us += 1
                    continue

                # ── Filter: individual homebrewer Household records ────
                if name.endswith('Household') or name.endswith('household'):
                    continue

                # ── Filter: chain restaurant franchise locations ────────
                # brewery_type=Location + Is_Craft_Brewery__c=False
                raw_brewery_type = str(item.get('Brewery_Type__c') or '').strip()
                raw_is_craft = item.get('Is_Craft_Brewery__c')
                if raw_brewery_type == 'Location' and raw_is_craft is False:
                    continue

                # ── Contact ───────────────────────────────────────────
                phone   = str(item.get('Phone')   or '').strip()
                website = str(item.get('Website') or '').strip()
                if website and not website.startswith('http'):
                    website = 'https://' + website

                # ── Niche fields ───────────────────────────────────────
                brewery_type = str(item.get('Brewery_Type__c') or '').strip()
                # Values: Micro, Brewpub, Regional, Large, Contract,
                # Alternating Proprietor, Taproom, Planning

                is_craft = item.get('Is_Craft_Brewery__c')
                is_craft_brewery = ('Yes' if is_craft is True
                                    else 'No' if is_craft is False
                                    else str(is_craft or '').strip())

                voting = item.get('Voting_Member__c')
                voting_member = ('Yes' if voting is True
                                 else 'No' if voting is False
                                 else str(voting or '').strip())

                membership_status = str(
                    item.get('Membership_Record_Status__c') or ''
                ).strip()   # Active / Expired / Pending

                membership_type = str(
                    item.get('Membership_Record_Item__c') or ''
                ).strip()

                account_badges = str(
                    item.get('Account_Badges__c') or ''
                ).strip()
                # e.g. "Independent Craft Brewer Seal;BA Monthly Brewery Member"

                year_est = str(item.get('Year_Established__c') or '').strip()

                # Independence inferred from badges
                independence_certified = (
                    'Yes' if 'Independent' in account_badges else 'No'
                )

                region = _state_to_region(state) if state else ''

                records.append({
                    'company_name':           name,
                    'address':                street,
                    'city':                   city,
                    'state':                  state,
                    'state_full':             state_full,
                    'zip_code':               zipcode,
                    'country':                'US',
                    'phone':                  phone,
                    'email':                  '',
                    'website':                website,
                    # Niche
                    'brewery_type':           brewery_type,
                    'is_craft_brewery':       is_craft_brewery,
                    'independence_certified': independence_certified,
                    'membership_status':      membership_status,
                    'membership_type':        membership_type,
                    'voting_member':          voting_member,
                    'account_badges':         account_badges,
                    'year_established':       year_est,
                    'latitude':               lat,
                    'longitude':              lon,
                    'geocode_accuracy':       geo_acc,
                    'region':                 region,
                })

            except Exception as exc:
                self.stats['total_errors'] += 1
                self.logger.debug(f"Parse error: {exc}")

        if skipped_non_us:
            self.logger.info(f"  Skipped {skipped_non_us} non-US entries")
        self.logger.info(f"  Parsed {len(records)} US brewery records")
        return records


# ------------------------------------------------------------------
if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    crawler = BrewersAssocCrawler()
    stats = crawler.run()
    print(f"\n{'='*40}")
    print(f"  New records  : {stats['total_new']}")
    print(f"  Updated      : {stats['total_updated']}")
    print(f"  Duplicates   : {stats['total_duplicates']}")
    print(f"  Errors       : {stats['total_errors']}")
    print(f"{'='*40}")
    if stats['total_new'] + stats['total_updated'] > 0:
        path = crawler.export_csv()
        if path:
            print(f"  CSV saved    : {path}")