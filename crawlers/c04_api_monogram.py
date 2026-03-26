"""
Crawler #4: API Monogram Composite List  
Source: https://mycerts.api.org/Search/CompositeSearch


  - FILTER: Only keeps records where country = "United States" 
  - Niche fields:
      * api_spec_number     — spec prefix parsed from cert number (e.g. "6A", "5CT", "Q1")
      * spec_full_name      — human-readable spec title (e.g. "Wellhead & Christmas Tree Equipment")
      * product_scope       — equipment/product category derived from spec code
      * cert_program        — Monogram / Q1 / Q2 / ISO / 18LCM
      * is_active           — 1/0 flag for quick filtering
      * state_full          — full US state or Canadian province name
      * facility_type       — Manufacturing / Service / Both (per spec)
      * industry_segment    — Upstream / Midstream / Downstream / General
  - Dedup key: certificationNumber (globally unique — no false dupes)
  
"""

import sys, os, time, re
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crawlers.base_crawler import BaseCrawler


# ── API Spec lookup table ──────────────────────────────────────────────────────
# (Full Name, Product Scope, Industry Segment, Facility Type)
API_SPEC_MAP = {
    "4F":   ("Drilling and Well Servicing Structures",       "Drilling Structures",          "Upstream",   "Manufacturing"),
    "5B":   ("Threading of Casing, Tubing & Line Pipe",      "Pipe Threading",               "Upstream",   "Manufacturing"),
    "5CT":  ("Casing and Tubing",                            "Casing & Tubing",              "Upstream",   "Manufacturing"),
    "5D":   ("Drill Pipe",                                   "Drill Pipe",                   "Upstream",   "Manufacturing"),
    "5L":   ("Line Pipe",                                    "Line Pipe",                    "Midstream",  "Manufacturing"),
    "5LD":  ("CRA Line Pipe",                                "CRA Line Pipe",                "Midstream",  "Manufacturing"),
    "5LC":  ("CRA Casing & Tubing",                          "CRA Tubulars",                 "Upstream",   "Manufacturing"),
    "6A":   ("Wellhead & Christmas Tree Equipment",          "Wellhead Equipment",           "Upstream",   "Manufacturing"),
    "6D":   ("Pipeline Valves",                              "Pipeline Valves",              "Midstream",  "Manufacturing"),
    "6FA":  ("Fire Test for Valves",                         "Fire-tested Valves",           "Midstream",  "Manufacturing"),
    "7-1":  ("Rotary Drill Stem Elements",                   "Drill Stem Tools",             "Upstream",   "Manufacturing"),
    "7-2":  ("Drill-through Equipment",                      "BOP / Drill-through",          "Upstream",   "Manufacturing"),
    "7K":   ("Drilling and Well Servicing Equipment",        "Drilling Equipment",           "Upstream",   "Manufacturing"),
    "8C":   ("Drilling & Production Hoisting Equipment",     "Hoisting Equipment",           "Upstream",   "Manufacturing"),
    "9A":   ("Wire Rope",                                    "Wire Rope",                    "General",    "Manufacturing"),
    "10A":  ("Cement",                                       "Oilwell Cement",               "Upstream",   "Manufacturing"),
    "11AX": ("Subsurface Sucker Rod Pumps",                  "Sucker Rod Pumps",             "Upstream",   "Manufacturing"),
    "11B":  ("Sucker Rods",                                  "Sucker Rods",                  "Upstream",   "Manufacturing"),
    "11D":  ("Pumping Units",                                "Pumping Units",                "Upstream",   "Manufacturing"),
    "11E":  ("Electric Submersible Pump Systems",            "ESP Systems",                  "Upstream",   "Manufacturing"),
    "16A":  ("Drill-through Equipment (BOP)",                "BOP Equipment",                "Upstream",   "Manufacturing"),
    "16C":  ("Choke & Kill Equipment",                       "Choke & Kill Systems",         "Upstream",   "Manufacturing"),
    "16D":  ("Control Systems for Drilling",                 "Drilling Control Systems",     "Upstream",   "Manufacturing"),
    "17D":  ("Subsea Wellhead Equipment",                    "Subsea Equipment",             "Upstream",   "Manufacturing"),
    "20A":  ("Metallic Gaskets",                             "Wellhead Gaskets",             "Upstream",   "Manufacturing"),
    "20B":  ("Nonmetallic Gaskets",                          "Nonmetallic Gaskets",          "Upstream",   "Manufacturing"),
    "594":  ("Wellhead Surface Safety Valves",               "Safety Valves",                "Upstream",   "Manufacturing"),
    "608":  ("Industrial Ball Valves",                       "Ball Valves",                  "Midstream",  "Manufacturing"),
    "609":  ("Butterfly Valves",                             "Butterfly Valves",             "Midstream",  "Manufacturing"),
    "Q1":   ("Quality Management for Mfg Orgs (API Q1)",    "QMS – Manufacturing",          "General",    "Both"),
    "Q2":   ("Quality Management for Service Supply Orgs",   "QMS – Services",               "General",    "Service"),
    "ISO":  ("ISO Certification",                            "ISO Standard",                 "General",    "Both"),
    "18LCM":("Life Cycle Management (API 18LCM)",            "Asset Life Cycle Mgmt",        "General",    "Service"),
}

US_STATES = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California",
    "CO":"Colorado","CT":"Connecticut","DE":"Delaware","FL":"Florida","GA":"Georgia",
    "HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa",
    "KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland",
    "MA":"Massachusetts","MI":"Michigan","MN":"Minnesota","MS":"Mississippi",
    "MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire",
    "NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina",
    "ND":"North Dakota","OH":"Ohio","OK":"Oklahoma","OR":"Oregon","PA":"Pennsylvania",
    "RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota","TN":"Tennessee",
    "TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington",
    "WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming","DC":"District of Columbia",
}

CA_PROVINCES = {
    "AB":"Alberta","BC":"British Columbia","MB":"Manitoba","NB":"New Brunswick",
    "NL":"Newfoundland and Labrador","NS":"Nova Scotia","ON":"Ontario",
    "PE":"Prince Edward Island","QC":"Quebec","SK":"Saskatchewan",
    "NT":"Northwest Territories","NU":"Nunavut","YT":"Yukon",
}


def _parse_spec_code(cert_num: str) -> str:
    """Extract spec prefix from certification number."""
    if not cert_num:
        return ""
    # Handle 7-1-xxxx and 7-2-xxxx
    m = re.match(r'^(7-[12])-\d', cert_num)
    if m:
        return m.group(1)
    # Handle 11AX-xxxx style (letter suffix before dash+digits)
    m = re.match(r'^([A-Z0-9]{2,5}[A-Z]?)-\d', cert_num, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    return ""


class ApiMonogramCrawler(BaseCrawler):

    AGREEMENT_URL = "https://mycerts.api.org/Login/UserAgreement"
    SEARCH_URL    = "https://mycerts.api.org/Search/CompositeSearch"
    API_ENDPOINT  = "https://mycerts.api.org/api/Search"
    PAGE_SIZE     = 100

    KNOWN_STATUS_IDS = [402, 403, 404, 405, 412, 413, 414]

    def get_dataset_name(self):
        return "api_monogram"

    def get_source_url(self):
        return self.SEARCH_URL

    def get_niche_fields(self):
        return [
            "license_number", "cert_program", "api_spec_number",
            "spec_full_name", "product_scope", "certification_status",
            "expiry_date", "is_active", "facility_type", "industry_segment",
            "state_full",
        ]

    # ──────────────────────────────────────────────────────────────────────────
    def crawl(self):
        import requests
        os.makedirs("logs", exist_ok=True)
        self._drop_stale_table()  # migrate schema if needed

        session = requests.Session()
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": self.SEARCH_URL,
        })

        self.logger.info("Getting session cookies...")
        session.get(self.AGREEMENT_URL, timeout=30)
        session.get(self.SEARCH_URL, timeout=30)
        self.logger.info(f"  Cookies: {list(session.cookies.keys())}")

        status_ids = self._fetch_status_ids(session)
        self.logger.info(f"  Status IDs to crawl: {status_ids}")

        all_items = []
        seen_keys = set()

        for status_id in status_ids:
            count = self._fetch_status(session, status_id, all_items, seen_keys)
            self.logger.info(
                f"  statusId={status_id}: {count} new records "
                f"(running total={len(all_items)})"
            )

        self.logger.info(f"Grand total (all countries, pre-filter): {len(all_items)}")
        return all_items

    # ──────────────────────────────────────────────────────────────────────────
    def _fetch_status_ids(self, session) -> list:
        try:
            r = session.get(
                "https://mycerts.api.org/api/Statuses",
                params={"typeId": 106, "_": int(time.time() * 1000)},
                timeout=20
            )
            self.logger.info(f"  /api/Statuses: HTTP {r.status_code} len={len(r.text)}")
            if r.status_code == 200:
                data  = r.json()
                items = (
                    data.get("statuses") or
                    data.get("data") or
                    data.get("items") or
                    (data if isinstance(data, list) else [])
                )
                ids = []
                for item in items:
                    sid = (
                        item.get("id") or item.get("Id") or
                        item.get("statusId") or item.get("value")
                    )
                    if sid is not None:
                        ids.append(sid)
                        self.logger.info(f"    id={sid} name={item.get('name','')}")
                if ids:
                    return ids
        except Exception as e:
            self.logger.warning(f"  Statuses fetch error: {e}")

        self.logger.info("  Using hardcoded status IDs")
        return self.KNOWN_STATUS_IDS

    # ──────────────────────────────────────────────────────────────────────────
    def _fetch_status(self, session, status_id, all_items, seen_keys) -> int:
        new_count = 0
        page_ndx  = 1

        while True:
            try:
                params = {
                    "companyName":         "",
                    "certificationNumber": "",
                    "statusIds":           status_id,
                    "pageNdx":             page_ndx,
                    "pageSize":            self.PAGE_SIZE,
                }
                r = session.get(self.API_ENDPOINT, params=params, timeout=30)

                if r.status_code != 200:
                    self.logger.warning(
                        f"  statusId={status_id} pageNdx={page_ndx}: "
                        f"HTTP {r.status_code} — {r.text[:100]}"
                    )
                    break

                data     = r.json()
                embedded = data.get("_embedded", {})
                items    = embedded.get("searchResults", [])
                total    = int(embedded.get("totalRowCount", 0))

                if not items:
                    self.logger.info(
                        f"  statusId={status_id} pageNdx={page_ndx}: "
                        f"0 items (total={total}) — done"
                    )
                    break

                for item in items:
                    key = str(
                        item.get("certificationNumber") or
                        item.get("CertificationNumber") or
                        item.get("licenseNumber") or
                        (str(item.get("companyName", "")) + str(item.get("city", "")))
                    )
                    if key not in seen_keys:
                        seen_keys.add(key)
                        all_items.append(item)
                        new_count += 1

                self.logger.info(
                    f"  statusId={status_id} pageNdx={page_ndx}: "
                    f"{len(items)} items, total={total}, cumulative={len(all_items)}"
                )

                if len(items) < self.PAGE_SIZE or page_ndx * self.PAGE_SIZE >= total:
                    break

                page_ndx += 1
                time.sleep(0.4)

            except Exception as e:
                self.logger.error(f"  statusId={status_id} pageNdx={page_ndx}: {e}")
                break

        return new_count


    def _drop_stale_table(self):
        """
        Drop existing table if its schema is missing new columns.
        Safe to call every run — recreated automatically by _store().
        """
        import sqlite3, os
        if not os.path.exists(self.db_path):
            return
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(f'''SELECT name FROM sqlite_master
                              WHERE type="table" AND name=?''',
                           (self.get_dataset_name(),))
            if cursor.fetchone():
                cursor.execute(f'''PRAGMA table_info("{self.get_dataset_name()}")''')
                existing_cols = {row[1] for row in cursor.fetchall()}
                required_cols = {"state_full", "cert_program", "api_spec_number",
                                  "spec_full_name", "product_scope", "is_active",
                                  "facility_type", "industry_segment"}
                if not required_cols.issubset(existing_cols):
                    self.logger.info(
                        f"  Schema mismatch — dropping stale table "
                        f"'{self.get_dataset_name()}' for rebuild"
                    )
                    cursor.execute(f'''DROP TABLE "{self.get_dataset_name()}"''')
                    conn.commit()
                    self.logger.info("  Table dropped — will be recreated with new schema")
        except Exception as e:
            self.logger.warning(f"  Migration check error: {e}")
        finally:
            conn.close()

    # ──────────────────────────────────────────────────────────────────────────
    def parse(self, raw_data):
        if not raw_data:
            return []

        records      = []
        filtered_out = 0

        for item in raw_data:
            try:
                if not isinstance(item, dict):
                    continue

                # ── Country filter — US only ─────────────────────
                raw_country = str(
                    item.get("country") or item.get("Country") or ""
                ).strip()

                if raw_country in ("United States", "USA", "US", "U.S.A.", "U.S."):
                    country = "United States"
                else:
                    filtered_out += 1
                    continue

                # ── Company name ──────────────────────────────────────────
                name = str(
                    item.get("companyName") or item.get("CompanyName") or
                    item.get("company")     or item.get("name") or ""
                ).strip()
                if not name:
                    continue

                # ── Cert number & spec code ───────────────────────────────
                cert_num = str(
                    item.get("certificationNumber") or
                    item.get("CertificationNumber") or
                    item.get("licenseNumber") or ""
                ).strip()

                spec_code = _parse_spec_code(cert_num)

                # Look up spec metadata — try exact, then prefix fallback
                spec_meta = API_SPEC_MAP.get(spec_code)
                if not spec_meta:
                    for k, v in API_SPEC_MAP.items():
                        if cert_num.upper().startswith(k):
                            spec_code, spec_meta = k, v
                            break

                if spec_meta:
                    spec_full_name, product_scope, industry_segment, facility_type = spec_meta
                else:
                    spec_full_name = product_scope = industry_segment = facility_type = ""

                # ── Cert program ──────────────────────────────────────────
                if spec_code == "Q1":
                    cert_program = "API Spec Q1"
                elif spec_code == "Q2":
                    cert_program = "API Spec Q2"
                elif spec_code == "ISO":
                    cert_program = "ISO"
                elif spec_code == "18LCM":
                    cert_program = "API 18LCM"
                else:
                    cert_program = "API Monogram"

                # ── Status & active flag ──────────────────────────────────
                status = str(
                    item.get("status") or item.get("Status") or
                    item.get("statusName") or ""
                ).strip() or "Active"

                is_active = 1 if status.lower() == "active" else 0

                # ── State resolution ──────────────────────────────────────
                raw_state = str(
                    item.get("state") or item.get("State") or ""
                ).strip()

                if country == "United States":
                    state_full = US_STATES.get(raw_state.upper(), raw_state)
                else:
                    state_full = CA_PROVINCES.get(raw_state.upper(), raw_state)

                # ── spec_full_name fallback from raw item ─────────────────
                raw_spec_str = str(
                    item.get("specification") or item.get("Specification") or
                    item.get("product")       or item.get("Product") or
                    item.get("programName")   or item.get("ProgramName") or ""
                ).strip()
                if not spec_full_name and raw_spec_str:
                    spec_full_name = raw_spec_str

                records.append({
                    "company_name":         name,
                    "address":    str(item.get("address") or item.get("street") or "").strip(),
                    "city":       str(item.get("city")    or item.get("City")    or "").strip(),
                    "state":      raw_state,
                    "state_full":           state_full,
                    "zip_code":   str(item.get("zip")     or item.get("Zip") or
                                      item.get("postalCode") or "").strip(),
                    "country":              country,
                    "phone":  "", "email": "", "website": "",
                    # Niche fields
                    "license_number":       cert_num,
                    "cert_program":         cert_program,
                    "api_spec_number":      spec_code,
                    "spec_full_name":       spec_full_name,
                    "product_scope":        product_scope,
                    "certification_status": status,
                    "is_active":            str(is_active),
                    "expiry_date": str(
                        item.get("expiryDate")  or item.get("ExpiryDate") or
                        item.get("expireDate")  or ""
                    ).strip(),
                    "facility_type":        facility_type,
                    "industry_segment":     industry_segment,
                })

            except Exception as exc:
                self.stats["total_errors"] += 1
                self.logger.debug(f"  Parse error: {exc}")

        self.logger.info(
            f"Parsed {len(records)} US/Canada records "
            f"({filtered_out} non-US/CA records filtered out)"
        )
        return records


    def _deduplicate(self, records):
        """
        Override base dedup: use license_number as unique key.
        Base uses company_name+address+state — causes false dupes here
        because address/state are often empty in this dataset.
        """
        import hashlib
        seen = set()
        unique = []
        for rec in records:
            raw_key = (
                rec.get("license_number") or
                (
                    (rec.get("company_name") or "").lower() +
                    (rec.get("city") or "").lower() +
                    (rec.get("country") or "").lower()
                )
            )
            hash_key = hashlib.md5(raw_key.encode()).hexdigest()
            if hash_key not in seen:
                seen.add(hash_key)
                rec["dedup_hash"] = hash_key
                unique.append(rec)
        return unique


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    crawler = ApiMonogramCrawler()
    stats   = crawler.run()
    print(f"\n{'='*50}")
    print(f"  New records   : {stats['total_new']}")
    print(f"  Updated       : {stats['total_updated']}")
    print(f"  Duplicates    : {stats['total_duplicates']}")
    print(f"  Errors        : {stats['total_errors']}")
    print(f"{'='*50}")
    if stats["total_new"] + stats["total_updated"] > 0:
        path = crawler.export_csv()
        print(f"  CSV saved     : {path}")
    else:
        print("\n  ⚠  0 records — check logs for details")