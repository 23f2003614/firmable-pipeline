"""
Crawler 25 — CFIA Food Establishment Licences (Canada)
Source   : https://apps.inspection.canada.ca/webapps/foodlicenceregistry/
Records  : ~18,815 | Canada

Fetch    : curl_cffi (Chrome fingerprint) bypasses browser check on the
           CFIA bulk CSV endpoint. Downloads full dataset in one shot.

Parse    : Reads all CSV rows, cleans and maps fields. Extracts licence
           number, type, establishment name, province, commodity categories.

           Even a government CSV bulk download checks for browser-like
           requests — curl_cffi handles what plain requests cannot.
"""

import sys, os, io
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crawlers.base_crawler import BaseCrawler


class CfiaCrawler(BaseCrawler):

    # Direct bulk CSV download URL — confirmed from CFIA registry page
    CSV_URL  = (
        "https://apps.inspection.canada.ca/webapps/foodlicenceregistry/en/"
        "FoodLicenceRegistry/DownloadFoodLicenceList/"
        "?language=e&downloadType=csv"
    )
    PAGE_URL = "https://apps.inspection.canada.ca/webapps/foodlicenceregistry/en/"

    def get_dataset_name(self): return "cfia_food_licences"
    def get_source_url(self):   return self.PAGE_URL
    def get_niche_fields(self):
        return ["licence_number", "doing_business_as",
                "included_establishments", "province", "postal_code"]

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
            "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-CA,en;q=0.9",
            "Referer":         self.PAGE_URL,
        })

        self.logger.info(f"Downloading CSV from:\n  {self.CSV_URL}")
        try:
            r = session.get(self.CSV_URL, timeout=120)  # large file
            self.logger.info(
                f"HTTP {r.status_code} | "
                f"{len(r.content):,} bytes | "
                f"Content-Type: {r.headers.get('Content-Type','?')}"
            )
            if not r.ok:
                self.logger.error(f"Download failed: HTTP {r.status_code}")
                return []
        except Exception as e:
            self.logger.error(f"Download error: {e}")
            return []

        # Parse CSV
        try:
            import csv
            # Detect encoding — CFIA often uses UTF-8-BOM
            content = r.content
            if content.startswith(b'\xef\xbb\xbf'):
                text = content.decode('utf-8-sig')
            else:
                text = content.decode('utf-8', errors='replace')

            reader = csv.DictReader(io.StringIO(text))
            rows = list(reader)
            self.logger.info(f"CSV rows: {len(rows):,}")
            self.logger.info(f"CSV headers: {reader.fieldnames}")
            return rows

        except Exception as e:
            self.logger.error(f"CSV parse error: {e}")
            # Save raw content for debugging
            with open("cfia_debug_raw.csv", "wb") as f:
                f.write(r.content)
            self.logger.info("Raw content saved to cfia_debug_raw.csv")
            return []

    # ------------------------------------------------------------------ #
    def parse(self, raw_data):
        if not raw_data:
            return []

        import re
        records = []

        for row in raw_data:
            if not isinstance(row, dict):
                continue

            # Normalise headers — CFIA CSV headers may vary slightly
            
            def get(row, *keys):
                for k in keys:
                    for rk in row:
                        if k.lower() in rk.lower():
                            v = row[rk]
                            return str(v).strip() if v else ""
                return ""

            name = get(row, "Legal name", "Dénomination sociale", "Name")
            if not name:
                continue

            licence_num = get(row, "Food licence number", "Numéro de licence")
            dba         = get(row, "Also doing business", "Fait aussi des affaires")
            included    = get(row, "Included establishment", "Établissement inclus")
            raw_address = get(row, "Address", "Adresse")

            # Parse address: "2087 Highway 2, Milford, Nova Scotia, Canada, B0N1Y0"
            street = city = province = postal = ""
            if raw_address:
                parts = [p.strip() for p in raw_address.split(",")]
                # Last part is postal code
                if parts and re.match(r"[A-Z]\d[A-Z]\s*\d[A-Z]\d", parts[-1]):
                    postal   = parts[-1].strip()
                    parts    = parts[:-1]
                # "Canada" is second to last — remove it
                if parts and parts[-1].strip().lower() == "canada":
                    parts = parts[:-1]
                # Province is now last
                if parts:
                    province = parts[-1].strip()
                    parts    = parts[:-1]
                # City is now last
                if parts:
                    city  = parts[-1].strip()
                    parts = parts[:-1]
                # Everything else is street
                street = ", ".join(parts).strip()

            records.append({
                "company_name":           name,
                "address":                street,
                "city":                   city,
                "state":                  province,
                "zip_code":               postal,
                "country":                "Canada",
                "phone":                  "",
                "email":                  "",
                "website":                "",
                # ── Niche fields ──────────────────────────────────────
                "licence_number":         licence_num,
                "doing_business_as":      dba,
                "included_establishments":included,
                "province":               province,
                "postal_code":            postal,
            })

        self.logger.info(f"Parsed {len(records)} CFIA records")
        return records


# ── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    crawler = CfiaCrawler()
    stats   = crawler.run()
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
        print(
            "\n  0 records — try opening this URL in your browser to confirm:\n"
            f"  {CfiaCrawler.CSV_URL}\n"
            "  If it downloads a CSV, the URL is correct."
        )