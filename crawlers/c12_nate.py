"""
Crawler 12 — NATE Certified HVAC Contractors
Source   : https://assessments.meazurelearning.com/Connect/NATE/Foundation/
Records  : ~4,077 | US

Fetch    : Hidden API endpoint discovered via Chrome DevTools Network tab —
           not publicly documented. curl_cffi provides Chrome TLS
           fingerprint to bypass bot detection.

Parse    : JSON response. Extracts company name, address, certified
           technician count, certification types held.

           API discovery via DevTools — every search form calls an API
           in the background. DevTools reveals the exact request format.
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crawlers.base_crawler import BaseCrawler

# ── Brand intelligence sets ────────────────────────────────────────────────
PREMIUM_BRANDS    = {"trane", "carrier", "lennox", "mitsubishi", "daikin", "york", "bosch"}
GEOTHERMAL_BRANDS = {"waterfurnace", "climatemaster"}
MINI_SPLIT_BRANDS = {"mitsubishi", "daikin", "fujitsu", "lg", "samsung"}
NON_BRAND_TOKENS  = {"other"}   # catch-all token — not a real manufacturer

# ── US state/country validation sets ──────────────────────────────────────
US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC","GU","PR","VI","AS","MP",
}
US_COUNTRY_NAMES = {"united states", "us", "usa", "u.s.", "u.s.a.", ""}

# ── Zip code list: one representative per USPS prefix block ───────────────
US_ZIPS = [
    # MA/CT/RI/NH/VT/ME (01-04)
    "01001","01201","01301","01501","01601","01701","01801","01901",
    "02101","02301","02601","02801","02901",
    "03031","03101","03301","03801","03901",
    "04001","04101","04401","04901",
    "05001","05401","05701","05901",
    "06001","06301","06501","06901",
    # NJ (07-08)
    "07001","07101","07301","07501","07701","07901",
    "08001","08201","08401","08601","08801",
    # NY (10-14)
    "10001","10301","10501","10701","10901",
    "11001","11201","11501","11701","11901",
    "12001","12201","12401","12601","12801","12901",
    "13001","13201","13401","13601","13801","13901",
    "14001","14201","14501","14701","14901",
    # PA (15-19)
    "15001","15201","15401","15601","15801","15901",
    "16001","16201","16401","16601","16801",
    "17001","17201","17401","17601","17801","17901",
    "18001","18201","18401","18601","18801","18901",
    "19001","19101","19301","19601","19701","19901",
    # DC/MD/VA/WV (20-26)
    "20001","20601","20901",
    "21001","21201","21401","21601","21901",
    "22001","22201","22401","22601","22801","22901",
    "23001","23201","23401","23601","23801","23901",
    "24001","24201","24401","24601","24801","24901",
    "25001","25201","25401","25601","25801","25901",
    "26001","26201","26401","26601","26801","26901",
    # NC/SC (27-29)
    "27001","27201","27401","27601","27801","27901",
    "28001","28201","28401","28601","28801","28901",
    "29001","29201","29401","29601","29801","29901",
    # GA/FL (30-34)
    "30001","30201","30401","30601","30801","30901",
    "31001","31201","31401","31601","31901",
    "32001","32201","32401","32601","32801","32901",
    "33001","33101","33301","33401","33601","33801","33901",
    "34101","34601","34950",
    # AL/TN/MS (35-39)
    "35001","35201","35401","35601","35801","35901",
    "36001","36201","36401","36601","36801","36901",
    "37001","37201","37401","37601","37801","37901",
    "38001","38201","38401","38601","38801","38901",
    "39001","39201","39401","39601","39701",
    # KY/IN/OH/MI (40-49)
    "40001","40201","40401","40601","40801","40901",
    "41001","41201","41401","41601","41801",
    "42001","42201","42401","42601","42701",
    "43001","43201","43401","43601","43801","43901",
    "44001","44201","44401","44601","44801","44901",
    "45001","45201","45401","45601","45801","45901",
    "46001","46201","46401","46601","46801","46901",
    "47001","47201","47401","47601","47801","47901",
    "48001","48201","48401","48601","48801","48901",
    "49001","49201","49401","49601","49801","49901",
    # IA/WI/MN/MO/IL (50-65)
    "50001","50201","50401","50601","50801","50901",
    "51001","51201","51401","51601",
    "52001","52201","52401","52601","52801",
    "53001","53201","53401","53601","53801","53901",
    "54001","54201","54401","54601","54801","54901",
    "55001","55301","55601","55901",
    "56001","56201","56601",
    "57001","57201","57401","57601","57701",
    "58001","58201","58501","58701",
    "59001","59201","59401","59601","59901",
    "60001","60201","60401","60601","60801","60901",
    "61001","61201","61401","61601","61801","61901",
    "62001","62201","62401","62601","62801","62901",
    "63001","63201","63401","63601","63801","63901",
    "64001","64401","64601","64801","64901",
    "65001","65201","65401","65601","65801","65901",
    # KS/NE/SD/ND (66-58)
    "66001","66201","66401","66601","66801","66901",
    "67001","67201","67401","67601","67801","67901",
    "68001","68201","68401","68601","68801","68901",
    "69001","69201","69361",
    # LA/AR/OK/TX (70-79)
    "70001","70401","70601","70801","70901",
    "71001","71201","71601","71901",
    "72001","72201","72401","72601","72801","72901",
    "73001","73401","73601","73801","73901",
    "74001","74301","74501","74701","74901",
    "75001","75201","75401","75601","75801","75901",
    "76001","76201","76401","76601","76801","76901",
    "77001","77301","77501","77701","77901",
    "78001","78201","78401","78601","78801","78901",
    "79001","79201","79401","79601","79801","79901",
    # CO/WY/MT/ID (80-83)
    "80001","80201","80401","80601","80801","80901",
    "81001","81201","81401","81601",
    "82001","82201","82401","82601","82901",
    "83001","83201","83401","83601","83801","83901",
    # UT/AZ/NM/NV (84-89)
    "84001","84201","84401","84601",
    "85001","85201","85401","85601","85901",
    "86001","86301","86401",
    "87001","87201","87401","87601","87801","87901",
    "88001","88201","88401",
    "89001","89301","89501","89701","89901",
    # CA (90-96)
    "90001","90201","90401","90601","90801","90901",
    "91001","91201","91401","91601","91801","91901",
    "92001","92201","92401","92601","92801","92901",
    "93001","93201","93401","93601","93901",
    "94001","94201","94401","94601","94901",
    "95001","95201","95401","95601","95901",
    "96001","96101",
    # OR/WA (97-99)
    "97001","97201","97301","97401","97501","97701","97901",
    "98001","98101","98201","98401","98501","98701","98901",
    "99001","99101","99301",
    # AK
    "99501","99601","99701","99801","99901",
    # HI / Pacific
    "96701","96813","96850",
]


class NateHvacCrawler(BaseCrawler):

    PAGE_URL = "https://natex.org/homeowner/find-a-contractor-with-nate-certified-technicians/"
    API_URL  = "https://assessments.meazurelearning.com/Connect/NATE/Foundation/api/LocationSearchC3Search"

    def get_dataset_name(self): return "nate_hvac_certified_contractors"
    def get_source_url(self):   return self.PAGE_URL
    def get_niche_fields(self):
        return [
            "cert_type", "certified_tech_count", "hvac_specialty",
            "manufacturer_count", "has_premium_brands", "multi_brand_dealer",
            "carries_geothermal", "carries_mini_split",
            "nate_certified_company", "affiliate_number",
            "latitude", "longitude",
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

    # ── crawl ──────────────────────────────────────────────────────────────
    def crawl(self):
        try:
            from curl_cffi import requests as cffi_requests
            session = cffi_requests.Session(impersonate="chrome124")
            self.logger.info("Using curl_cffi (Chrome fingerprint)")
        except ImportError:
            import requests
            session = requests.Session()
            self.logger.warning("curl_cffi not available — pip install curl_cffi recommended")

        session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": "https://natex.org",
            "Referer": self.PAGE_URL,
        })

        try:
            session.get(self.PAGE_URL, timeout=30)
            self.logger.info("Session warm-up complete")
        except Exception as e:
            self.logger.warning(f"Warm-up failed (non-fatal): {e}")

        all_records     = []
        seen_affiliate  = set()   # primary dedup: C3 affiliate ID
        seen_name_zip   = set()   # fallback dedup: (name, zip)

        for zipcode in US_ZIPS:
            raw   = self._fetch_by_zip(session, zipcode)
            added = 0

            for item in raw:
                if not isinstance(item, dict):
                    continue

                # ── US-only filter ────────────────────────────────────
                state   = str(item.get("StateAbbreviation") or
                               item.get("StateName") or "").strip().upper()
                country = str(item.get("CountryName") or
                               item.get("Country") or "").strip().lower()
                zip_val = str(item.get("ZipCode") or "").strip()

                is_us = (
                    state   in US_STATES or
                    country in US_COUNTRY_NAMES or
                    (zip_val.isdigit() and len(zip_val) in (4, 5))
                )
                if not is_us:
                    self.logger.debug(
                        f"Skipping non-US: {item.get('CompanyName')} | "
                        f"state={state} country={country}"
                    )
                    continue

                # ── Dedup ─────────────────────────────────────────────
                aff      = str(item.get("AffiliateNumber") or "").strip().upper()
                name_key = (
                    str(item.get("CompanyName") or "").strip().lower()
                    + "|" + zip_val
                )

                if aff and aff in seen_affiliate:
                    continue
                if not aff and name_key in seen_name_zip:
                    continue

                if aff:
                    seen_affiliate.add(aff)
                seen_name_zip.add(name_key)

                all_records.append(item)
                added += 1

            self.logger.info(
                f"zip={zipcode} → {len(raw):3d} returned | "
                f"{added:3d} new | total={len(all_records)}"
            )
            self._rate_limit(1.0)

        self.logger.info(f"Crawl complete — {len(all_records)} unique US records")
        return all_records

    # ── _fetch_by_zip ──────────────────────────────────────────────────────
    def _fetch_by_zip(self, session, zipcode):
        try:
            resp = session.post(
                self.API_URL,
                json={"CountryCode": "", "zipCode": zipcode},
                timeout=30,
            )
            if not resp.ok:
                self.logger.warning(f"HTTP {resp.status_code} for zip={zipcode}")
                return []

            data = resp.json()

            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                for key in ("Locations", "locations", "contractors",
                            "results", "data", "Items", "items"):
                    if data.get(key):
                        return data[key]
            return []

        except Exception as e:
            self.logger.warning(f"zip={zipcode} fetch error: {e}")
            return []

    # ── _parse_brands ──────────────────────────────────────────────────────
    @staticmethod
    def _parse_brands(raw: str):
        """
        Parse Manufacturers field.  Removes the catch-all 'Other' token.
        Returns (cleaned_str, lowercase_brand_set, brand_count).
        """
        if not raw or raw.strip() in ("", "None", "N/A"):
            return "", set(), 0

        tokens    = [t.strip() for t in raw.split(",") if t.strip()]
        real      = [t for t in tokens if t.lower() not in NON_BRAND_TOKENS]
        brand_set = {t.lower() for t in real}
        return ", ".join(real), brand_set, len(real)

    # ── _infer_cert_type ───────────────────────────────────────────────────
    @staticmethod
    def _infer_cert_type(api_tier: str, tech_count: int) -> str:
        """
        API returns Basic/Quality/Standard only when individual tech certs are
        tracked.  For remaining records (84%) we infer the correct designation:
          - API tier present             → use as-is
          - API blank + techs > 0        → 'NATE Certified — Techs on Staff'
          - API blank + techs = 0        → 'NATE Certified Company'
        """
        tier = (api_tier or "").strip()
        if tier:
            return tier
        return "NATE Certified — Techs on Staff" if tech_count > 0 \
               else "NATE Certified Company"

    # ── parse ──────────────────────────────────────────────────────────────
    def parse(self, raw_data):
        if not raw_data:
            return []

        records = []
        for item in raw_data:
            if not isinstance(item, dict):
                continue

            name = str(item.get("CompanyName") or "").strip()
            if not name:
                continue

            # Address
            addr1   = str(item.get("Address1") or "").strip()
            addr2   = str(item.get("Address2") or "").strip()
            address = f"{addr1} {addr2}".strip() if addr2 else addr1

            # Certified tech count
            raw_tc = (item.get("CertifiedTechnicians") or
                      item.get("NumberOfCertifiedTechnicians") or 0)
            try:
                tech_count = int(raw_tc)
            except (ValueError, TypeError):
                tech_count = 0

            # Cert type (inferred when blank)
            api_tier  = str(item.get("CertLevel") or
                            item.get("CertificationLevel") or "").strip()
            cert_type = self._infer_cert_type(api_tier, tech_count)

            # HVAC brands
            raw_brands                  = str(item.get("Manufacturers") or "").strip()
            hvac_str, brand_set, n_brands = self._parse_brands(raw_brands)

            # Derived B2B intelligence
            has_premium  = "Yes" if brand_set & PREMIUM_BRANDS    else "No"
            carries_geo  = "Yes" if brand_set & GEOTHERMAL_BRANDS else "No"
            carries_mini = "Yes" if brand_set & MINI_SPLIT_BRANDS else "No"
            multi_brand  = "Yes" if n_brands >= 3                 else "No"

            # GPS
            lat = str(item.get("Latitude")  or item.get("latitude")  or "").strip()
            lon = str(item.get("Longitude") or item.get("longitude") or "").strip()

            # Affiliate ID
            affiliate = str(item.get("AffiliateNumber") or "").strip()

            records.append({
                "company_name":           name,
                "address":                address,
                "city":                   str(item.get("City") or "").strip(),
                "state":                  str(item.get("StateAbbreviation") or
                                              item.get("StateName") or "").strip(),
                "zip_code":               str(item.get("ZipCode") or "").strip(),
                "country":                "United States",
                "phone":                  str(item.get("Phone") or "").strip(),
                "email":                  str(item.get("Email") or "").strip().lower(),
                "website":                str(item.get("Website") or "").strip(),
                # ── Niche B2B fields ─────────────────────────────────
                "cert_type":              cert_type,
                "certified_tech_count":   str(tech_count),
                "hvac_specialty":         hvac_str,
                "manufacturer_count":     str(n_brands),
                "has_premium_brands":     has_premium,
                "multi_brand_dealer":     multi_brand,
                "carries_geothermal":     carries_geo,
                "carries_mini_split":     carries_mini,
                "nate_certified_company": "Yes",
                "affiliate_number":       affiliate,
                "latitude":               lat,
                "longitude":              lon,
            })

        self.logger.info(f"Parsed {len(records)} records")
        return records


# ── entrypoint ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    crawler = NateHvacCrawler()
    stats   = crawler.run()

    print(f"\n{'='*52}")
    print(f"  New records        : {stats['total_new']:,}")
    print(f"  Updated            : {stats['total_updated']:,}")
    print(f"  Duplicates removed : {stats['total_duplicates']:,}")
    print(f"  Errors             : {stats['total_errors']:,}")
    print(f"{'='*52}")

    if stats["total_new"] + stats["total_updated"] > 0:
        path = crawler.export_csv()
        print(f"  CSV exported       : {path}")
    else:
        print("\n  0 records — check API endpoint is reachable.")
        print("  Verify: POST to API_URL with json={'CountryCode':'','zipCode':'10001'}")