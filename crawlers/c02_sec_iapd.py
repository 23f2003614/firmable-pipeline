"""
Crawler #2: SEC Registered Investment Advisers (IAPD) — US-ONLY
================================================================
Runtime: ~2-3 minutes (single ZIP download, zero API calls)

The SEC bulk XLSX contains the FULL ADV Part 1 form data.
Columns are named by ADV section numbers (5I(1), 5B(1) etc).
We decode these directly — no API calls, no waiting.

Fields extracted:
  - AUM total, discretionary, non-discretionary       (Item 5I)
  - Total employees, advisory employees               (Item 5B)
  - Number of accounts, clients                       (Items 5C, 5D, 5J)
  - Advisory services (9 types)                       (Item 6A)
  - Compensation arrangements (7 types)               (Item 5F)
  - Adviser type, umbrella registration, SEC region
  - CCO name, CCO email, amendment date

Source: https://adviserinfo.sec.gov/
"""

import sys, os, re, zipfile, csv, io
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crawlers.base_crawler import BaseCrawler

US_STATES = {
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',
    'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
    'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',
    'VA','WA','WV','WI','WY','DC','PR','VI','GU','AS','MP'
}
US_COUNTRY_VARIANTS = {
    'united states', 'us', 'usa', 'u.s.', 'u.s.a.', 'united states of america'
}

# ADV Part 1 section → human-readable field name
# Source: SEC Form ADV Part 1A instructions
ADV_MAP = {
    # ── Item 5B: Employees ────────────────────────────────────────────────────
    '5B(1)': 'total_employees',
    '5B(2)': 'employees_investment_advisory',
    # ── Item 5C: Client types (Y/N flags) ────────────────────────────────────
    '5C(1)': 'clients_individuals',
    '5C(2)': 'clients_high_net_worth_individuals',
    '5C(3)': 'clients_investment_companies',
    '5C(4)': 'clients_business_development_companies',
    '5C(5)': 'clients_pooled_investment_vehicles',
    '5C(6)': 'clients_pension_profit_sharing',
    '5C(7)': 'clients_charitable_organizations',
    '5C(8)': 'clients_state_municipal_government',
    '5C(9)': 'clients_other_investment_advisers',
    '5C(10)': 'clients_insurance_companies',
    '5C(11)': 'clients_sovereign_wealth',
    '5C(12)': 'clients_corporations_businesses',
    # ── Item 5D: Number of clients ────────────────────────────────────────────
    '5D(a)': 'num_clients_under_management',
    '5D(b)': 'num_clients_not_under_management',
    # ── Item 5F: Compensation arrangements ───────────────────────────────────
    '5F(1)': 'compensation_pct_of_aum',
    '5F(2)': 'compensation_hourly_charges',
    '5F(3)': 'compensation_subscription_fees',
    '5F(4)': 'compensation_flat_fee',
    '5F(5)': 'compensation_performance_based',
    '5F(6)': 'compensation_brokerage_commissions',
    '5F(7)': 'compensation_other',
    # ── Item 5G: Wrap fee programs ────────────────────────────────────────────
    '5G':    'adviser_to_wrap_fee_programs',
    # ── Item 5I: AUM (the most important fields) ──────────────────────────────
    '5I(1)': 'aum_total',
    '5I(2)(a)': 'aum_discretionary',
    '5I(2)(b)': 'aum_non_discretionary',
    '5I(2)(c)': 'num_accounts',
    # ── Item 5J: Number of clients ────────────────────────────────────────────
    '5J(1)': 'num_clients_total',
    '5J(2)': 'num_clients_us',
    # ── Item 6A: Types of advisory services ──────────────────────────────────
    '6A(1)': 'service_financial_planning',
    '6A(2)': 'service_portfolio_mgmt_individuals',
    '6A(3)': 'service_portfolio_mgmt_businesses',
    '6A(4)': 'service_portfolio_mgmt_investment_companies',
    '6A(5)': 'service_pension_consulting',
    '6A(6)': 'service_selection_of_advisers',
    '6A(7)': 'service_publication_periodicals',
    '6A(8)': 'service_security_ratings',
    '6A(9)': 'service_market_timing',
    '6A(10)': 'service_educational_seminars',
    '6A(11)': 'service_other',
    # ── Item 6B: Wrap fee ─────────────────────────────────────────────────────
    '6B(1)': 'wrap_fee_sponsor',
    '6B(2)': 'wrap_fee_portfolio_manager',
    '6B(3)': 'wrap_fee_administrator',
    # ── Item 7A: Financial industry affiliations ──────────────────────────────
    '7A(1)': 'affiliated_broker_dealer',
    '7A(2)': 'affiliated_registered_rep',
    '7A(3)': 'affiliated_commodity_pool',
    '7A(4)': 'affiliated_futures_commission',
    '7A(5)': 'affiliated_banking_institution',
    '7A(6)': 'affiliated_insurance_company',
    '7A(7)': 'affiliated_pension_consultant',
    '7A(8)': 'affiliated_real_estate_broker',
}

# Compensation arrangement labels for human-readable summary
COMP_LABELS = {
    '5F(1)': '% of AUM',
    '5F(2)': 'Hourly charges',
    '5F(3)': 'Subscription fees',
    '5F(4)': 'Fixed/flat fee',
    '5F(5)': 'Performance-based',
    '5F(6)': 'Brokerage commissions',
    '5F(7)': 'Other',
}

# Advisory service labels
SERVICE_LABELS = {
    '6A(1)': 'Financial planning',
    '6A(2)': 'Portfolio mgmt - individuals',
    '6A(3)': 'Portfolio mgmt - businesses',
    '6A(4)': 'Portfolio mgmt - investment companies',
    '6A(5)': 'Pension consulting',
    '6A(6)': 'Selection of other advisers',
    '6A(7)': 'Publication of periodicals',
    '6A(8)': 'Security ratings',
    '6A(9)': 'Market timing',
    '6A(10)': 'Educational seminars',
    '6A(11)': 'Other',
}


class SecIapdCrawler(BaseCrawler):

    DATA_PAGE = (
        "https://www.sec.gov/data-research/sec-markets-data/"
        "information-about-registered-investment-advisers-"
        "exempt-reporting-advisers"
    )

    def get_dataset_name(self):  return "sec_investment_advisers"
    def get_source_url(self):    return "https://adviserinfo.sec.gov/"
    def get_niche_fields(self):
        return [
            'crd_number', 'sec_file_number', 'sec_region',
            'registration_status', 'adviser_type', 'umbrella_registration',
            'has_regulatory_aum',
            'aum_total', 'aum_discretionary', 'aum_non_discretionary',
            'num_accounts',
            'total_employees', 'employees_investment_advisory',
            'has_us_clients', 'has_non_us_clients',
            'advisory_services',
            'compensation_arrangements',
            'affiliated_broker_dealer',
            'affiliated_banking_institution',
            'affiliated_insurance_company',
        ]

    # ── Download ──────────────────────────────────────────────────────────────
    def crawl(self):
        import requests
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; FirmableBot/1.0; B2B Research)'}

        self.logger.info("Fetching SEC data page...")
        resp = requests.get(self.DATA_PAGE, headers=headers, timeout=30)
        if not resp.ok:
            raise RuntimeError(f"Page load failed: {resp.status_code}")

        all_zips = re.findall(r'/files[^"\'<>\s]+\.zip', resp.text)
        main_zips = [z for z in all_zips if 'exempt' not in z.lower()]

        def sort_key(path):
            m = re.search(r'ia(\d{2})(\d{2})(\d{2})\.zip', path)
            return (int(m.group(3))*10000 + int(m.group(1))*100 + int(m.group(2))) if m else 0

        latest = "https://www.sec.gov" + sorted(main_zips, key=sort_key, reverse=True)[0]
        self.logger.info(f"Downloading: {latest}")

        r = requests.get(latest, headers=headers, timeout=180)
        if not r.ok or len(r.content) < 1000:
            raise RuntimeError(f"Download failed: HTTP {r.status_code}")
        self.logger.info(f"Downloaded {len(r.content):,} bytes")

        files = {}
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            self.logger.info(f"Zip contents: {zf.namelist()}")
            for name in zf.namelist():
                nl = name.lower()
                if nl.endswith('.csv'):
                    with zf.open(name) as f:
                        files[name] = ('csv', f.read().decode('utf-8', errors='replace'))
                    self.logger.info(f"  Loaded CSV : {name}")
                elif nl.endswith('.xlsx'):
                    with zf.open(name) as f:
                        files[name] = ('xlsx', f.read())
                    self.logger.info(f"  Loaded XLSX: {name}")
        return files

    # ── Parse ─────────────────────────────────────────────────────────────────
    def parse(self, raw_data: dict) -> list:
        if not raw_data:
            return []

        main_file = list(raw_data.keys())[0]
        self.logger.info(f"Parsing: {main_file}")

        firms = self._read_file(raw_data[main_file])
        self.logger.info(f"Total parsed: {len(firms)}")

        us_firms = [f for f in firms if self._is_us(f)]
        self.logger.info(f"US-only: {len(us_firms)}")
        return us_firms

    def _read_file(self, file_tuple) -> list:
        ftype, content = file_tuple
        if ftype == 'csv':
            return self._parse_csv(content)
        elif ftype == 'xlsx':
            return self._parse_xlsx(content)
        return []

    def _parse_csv(self, text: str) -> list:
        records = []
        try:
            reader = csv.DictReader(io.StringIO(text, newline=''))
            cols = list(reader.fieldnames or [])
            self.logger.info(f"CSV columns ({len(cols)}): {cols[:15]}")
            for row in reader:
                rec = self._map_row(row)
                if rec: records.append(rec)
        except Exception as e:
            self.logger.error(f"CSV parse error: {e}")
        return records

    def _parse_xlsx(self, content: bytes) -> list:
        try:
            import openpyxl
        except ImportError:
            self.logger.error("pip install openpyxl")
            return []
        records = []
        try:
            wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
            ws = wb.active
            rows_iter = ws.iter_rows(values_only=True)
            headers = [str(h).strip() if h is not None else f'col_{i}'
                       for i, h in enumerate(next(rows_iter))]
            self.logger.info(f"XLSX columns ({len(headers)}): first 20 = {headers[:20]}")
            for row in rows_iter:
                row_dict = {k: (str(v).strip() if v is not None else '')
                            for k, v in zip(headers, row)}
                rec = self._map_row(row_dict)
                if rec: records.append(rec)
        except Exception as e:
            self.logger.error(f"XLSX parse error: {e}")
        return records

    def _map_row(self, row) -> dict | None:
        name = (
            row.get('Organization Name') or row.get('Primary Business Name') or
            row.get('Legal Name') or ''
        ).strip()
        if not name:
            return None

        # Advisory services: human-readable summary from 6A Y/N flags
        services = [label for col, label in SERVICE_LABELS.items()
                    if self._is_yes(row.get(col, ''))]
        advisory_services = '; '.join(services) if services else ''

        # Compensation: human-readable summary from 5F Y/N flags
        comps = [label for col, label in COMP_LABELS.items()
                 if self._is_yes(row.get(col, ''))]
        compensation_arrangements = '; '.join(comps) if comps else ''

        # AUM: 5I(1) is a Y/N flag (has regulatory AUM?)
        # Dollar amounts are in 5I(2)(a) discretionary and 5I(2)(b) non-discretionary
        def parse_dollars(val: str) -> int:
            v = str(val).replace('$', '').replace(',', '').strip()
            try: return int(float(v))
            except: return 0

        raw_disc    = self._g(row, '5I(2)(a)')
        raw_nondisc = self._g(row, '5I(2)(b)')
        disc_amt    = parse_dollars(raw_disc)
        nondisc_amt = parse_dollars(raw_nondisc)
        total_amt   = disc_amt + nondisc_amt

        aum_total             = f"${total_amt:,}"    if total_amt    else ''
        aum_discretionary     = f"${disc_amt:,}"     if disc_amt     else ''
        aum_non_discretionary = f"${nondisc_amt:,}"  if nondisc_amt  else ''

        return {
            'company_name':     name,
            'address':          self._g(row, 'Main Office Street Address 1', 'STREET1'),
            'city':             self._g(row, 'Main Office City', 'CITY'),
            'state':            self._g(row, 'Main Office State', 'STATE'),
            'zip_code':         self._g(row, 'Main Office Postal Code', 'ZIP'),
            'country':          self._g(row, 'Main Office Country') or 'United States',
            'phone':            self._g(row, 'Main Office Phone Number', 'PHONE'),
            'email':            '',
            'website':          self._fix_url(self._g(row, 'Website Address', 'WEBSITE')),
            # Identity & registration
            'crd_number':            self._g(row, 'Organization CRD#', 'CRD#'),
            'sec_file_number':       self._g(row, 'SEC File Number', 'SEC#'),
            'sec_region':            self._g(row, 'SEC Region'),
            'registration_status':   self._g(row, 'Registration Status', 'STATUS'),
            'adviser_type':          self._g(row, 'Firm Type', 'Adviser Type'),
            'umbrella_registration': self._g(row, 'Umbrella Registration'),
            # AUM (Item 5I — dollar amounts, sparse but accurate ~1100 firms)
            'has_regulatory_aum':     self._g(row, '5I(1)'),   # Y/N
            'aum_total':              aum_total,                # computed = disc + non-disc
            'aum_discretionary':      aum_discretionary,
            'aum_non_discretionary':  aum_non_discretionary,
            'num_accounts':           self._g(row, '5I(2)(c)'),
            # Employees (Item 5B)
            'total_employees':               self._g(row, '5B(1)', 'Total Employees'),
            'employees_investment_advisory': self._g(row, '5B(2)'),
            # Client flags (Item 5J — Y/N)
            'has_us_clients':     self._g(row, '5J(1)'),
            'has_non_us_clients': self._g(row, '5J(2)'),
            # Services & compensation (human-readable summaries)
            'advisory_services':         advisory_services,
            'compensation_arrangements': compensation_arrangements,
            # Affiliations (Item 7A — Y/N)
            'affiliated_broker_dealer':       self._g(row, '7A(1)'),
            'affiliated_banking_institution': self._g(row, '7A(5)'),
            'affiliated_insurance_company':   self._g(row, '7A(6)'),
        }


    # ── Helpers ───────────────────────────────────────────────────────────────
    @staticmethod
    def _is_yes(val: str) -> bool:
        return str(val).strip().upper() in ('Y', 'YES', '1', 'TRUE', 'X')

    @staticmethod
    def _g(row: dict, *keys, default='') -> str:
        for k in keys:
            v = row.get(k)
            if v is not None:
                s = str(v).strip()
                if s and s.lower() not in ('none', 'n/a', 'na', '-', 'null', '0', ''):
                    return s
        return default

    @staticmethod
    def _fix_url(raw: str) -> str:
        if not raw: return ''
        url = raw.strip()
        m = re.match(r'^https?://(https?://.+)$', url, re.IGNORECASE)
        if m: url = m.group(1)
        url = re.sub(r'^HTTP://', 'http://', url, flags=re.IGNORECASE)
        url = re.sub(r'^HTTPS://', 'https://', url, flags=re.IGNORECASE)
        return url

    @staticmethod
    def _is_us(rec: dict) -> bool:
        country = (rec.get('country') or '').strip().lower()
        state   = (rec.get('state') or '').strip().upper()
        if country and country not in US_COUNTRY_VARIANTS:
            return False
        if state and state not in US_STATES:
            return False
        return True


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sqlite3
    os.makedirs("logs", exist_ok=True)
    crawler = SecIapdCrawler()

    db, table = crawler.db_path, crawler.get_dataset_name()
    if os.path.exists(db):
        conn = sqlite3.connect(db)
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.commit()
        conn.close()
        print(f"  Dropped old table: {table}")

    stats = crawler.run()
    print(f"\n{'='*45}")
    print(f"  New records  : {stats['total_new']}")
    print(f"  Updated      : {stats['total_updated']}")
    print(f"  Duplicates   : {stats['total_duplicates']}")
    print(f"  Errors       : {stats['total_errors']}")
    print(f"{'='*45}")
    if stats['total_new'] + stats['total_updated'] > 0:
        path = crawler.export_csv()
        if path: print(f"  CSV saved    : {path}")