"""
Crawler 09 — ACHC Accredited Healthcare Providers
Source   : https://compass-api.achc.org/api/Anonymous/GetPublicLocations/
Records  : ~600 | US

Fetch    : Calls the ACHC Compass API once per accreditation program type
           (acute care, ambulatory surgery, labs, etc). Merges all results.

Parse    : Extracts facility name, address, program type, expiry date.
           Calculates days_until_expiry from today for each facility.

           days_until_expiry turns a static list into pipeline intelligence
           — vendors know exactly when a facility's renewal window opens.
"""

import sys
import os
import re
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crawlers.base_crawler import BaseCrawler

# ---------------------------------------------------------------------------
# All known ACHC/HFAP programme codes.
# Confirmed codes are marked.  The rest are probed — 404/empty responses are
# silently skipped so adding extras here costs nothing.
# ---------------------------------------------------------------------------
PROGRAM_CODES = [
    # ── HFAP (Hospital & Facility Accreditation Programme) ──────────────────
    "hfap",                        # confirmed — 630+ records
    "hfap-lab",
    "hfap-asc",
    "hfap-stroke",
    "hfap-joint",
    # ── ACHC core programmes ────────────────────────────────────────────────
    "achc",
    "home-health",
    "homehealth",
    "home_health",
    "hospice",
    "pharmacy",
    "dme",                         # Durable Medical Equipment
    "dme-pharmacy",
    "private-duty",
    "privateduty",
    "infusion",
    "infusion-therapy",
    "behavioral",
    "behavioral-health",
    "sleep",
    "sleep-lab",
    "renal",
    "dialysis",
    "chap",                        # Community Health Accreditation Partner
    "community",
    "community-health",
    "palliative",
    "palliative-care",
    "telehealth",
    "urgent-care",
    "urgentcare",
    "rehab",
    "rehabilitation",
    "outpatient-rehab",
    "cardiac-rehab",
]

# Valid US state abbreviations for the US-only filter
US_STATE_ABBREVS = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
    "TX","UT","VT","VA","WA","WV","WI","WY","DC","PR","GU","VI","AS","MP",
}


def _is_us_record(item: dict) -> bool:
    """Return True only for genuine US (including territories) records."""
    country = str(item.get("Country") or "").strip()
    state   = str(item.get("StateName") or "").strip().upper()
    zip_    = str(item.get("ZipCode") or "").strip()

    # Explicit non-US country names
    if country and "united states" not in country.lower() and country not in ("US", "USA", ""):
        return False

    # State abbreviation check (if state is 2 chars)
    if len(state) == 2 and state not in US_STATE_ABBREVS:
        return False

    # 5-digit US zip sanity check (only reject if clearly non-US format)
    if zip_ and not re.match(r"^\d{5}", zip_):
        return False

    return True


def _safe_str(val, max_len=None) -> str:
    s = str(val).strip() if val is not None else ""
    if max_len:
        s = s[:max_len]
    return s


def _format_date(val) -> str:
    """Return YYYY-MM-DD from ISO datetime string, or empty string."""
    s = _safe_str(val)
    return s[:10] if len(s) >= 10 else s


def _extract_accreditation_level(specialty_code: str) -> str:
    """
    Infer human-readable accreditation level from the raw specialtyCode.
    Examples:
      acuteCareDeemed                  → Deemed Status
      ambulatorySurgicalCenterDeemed   → Deemed Status
      ambulatoryCare                   → Accredited
      primaryStroke                    → Certified
      comprehensiveStroke              → Certified
      strokeReady                      → Certified
      jointReplacementAdvancedWith...  → Certified (Advanced with Distinction)
    """
    code = specialty_code.lower()
    if "deemed" in code:
        return "Deemed Status"
    if "stroke" in code or "joint" in code or "lithotripsy" in code:
        return "Certified"
    if "international" in code:
        return "International Accreditation"
    return "Accredited"


def _extract_programme_category(programme: str) -> str:
    """Group accreditation programmes into high-level categories."""
    p = programme.upper()
    if "HOSPITAL" in p or "ACUTE" in p or "CRITICAL" in p:
        return "Hospital"
    if "AMBULATORY" in p or "SURGERY" in p or "SURGICAL" in p or "OFFICE-BASED" in p:
        return "Ambulatory / Surgical"
    if "LAB" in p:
        return "Laboratory"
    if "STROKE" in p:
        return "Stroke Centre"
    if "JOINT" in p or "REPLACEMENT" in p:
        return "Joint Replacement"
    if "LITHOTRIPSY" in p:
        return "Lithotripsy"
    if "HOME" in p or "HOSPICE" in p or "IN-HOME" in p:
        return "Home Health / Hospice"
    if "PHARMACY" in p or "DME" in p or "INFUSION" in p:
        return "Pharmacy / DME"
    if "BEHAVIORAL" in p or "MENTAL" in p:
        return "Behavioral Health"
    if "REHAB" in p or "REHABILITATION" in p:
        return "Rehabilitation"
    if "SLEEP" in p:
        return "Sleep Lab"
    if "RENAL" in p or "DIALYSIS" in p:
        return "Renal / Dialysis"
    return "Other"


class AchcCrawler(BaseCrawler):

    BASE_API = "https://compass-api.achc.org/api/Anonymous/GetPublicLocations"

    def get_dataset_name(self) -> str:
        return "achc_accredited_providers"

    def get_source_url(self) -> str:
        return "https://achc.org/search-facilities/"

    def get_niche_fields(self):
        return [
            "location_id",
            "doing_business_as",
            "accreditation_program",
            "accreditation_type",
            "accreditation_level",
            "programme_category",
            "program_status",
            "effective_date",
            "resurvey_date",
            "days_until_resurvey",
            "certification_number",
            "specialty_code",
            "bed_count",
            "trauma_level",
            "teaching_status",
            "services_offered",
        ]

    # ──────────────────────────────────────────────────────────────────────────
    # CRAWL
    # ──────────────────────────────────────────────────────────────────────────
    def crawl(self):
        import requests

        session = requests.Session()
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept":          "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin":          "https://achc.org",
            "Referer":         "https://achc.org/",
        })

        all_items = []
        # Use LocationId as primary dedup key to prevent same facility appearing
        # under multiple programme codes
        seen_location_ids = set()

        for code in PROGRAM_CODES:
            url = f"{self.BASE_API}/{code}"
            try:
                resp = session.get(url, timeout=30)
                self.logger.info(
                    f"  [{code}] HTTP {resp.status_code}  "
                    f"body_len={len(resp.text)}"
                )
                if resp.status_code != 200 or len(resp.text) < 10:
                    time.sleep(0.3)
                    continue

                data = resp.json()
                items = (
                    data if isinstance(data, list)
                    else data.get("data") or data.get("value") or data.get("results") or []
                )

                new_this_code = 0
                for item in items:
                    if not isinstance(item, dict):
                        continue

                    # ── US-only filter ──────────────────────────────────────
                    if not _is_us_record(item):
                        continue

                    # ── Primary dedup: LocationId ───────────────────────────
                    lid = (
                        item.get("LocationId") or
                        item.get("locationId") or
                        item.get("location_id") or
                        ""
                    )
                    # Build a composite key: locationId + programme so that
                    # a hospital with BOTH acute-care AND stroke certifications
                    # appears as TWO rows (different programme/cert)
                    specialty = (
                        item.get("SpecialtyCode") or
                        item.get("specialtyCode") or
                        ""
                    )
                    composite_key = f"{lid}::{specialty}" if lid else None

                    if composite_key and composite_key in seen_location_ids:
                        continue
                    if composite_key:
                        seen_location_ids.add(composite_key)

                    item["_program_code"] = code
                    all_items.append(item)
                    new_this_code += 1

                self.logger.info(
                    f"    → {new_this_code} new US records  "
                    f"(running total = {len(all_items)})"
                )

            except Exception as exc:
                self.logger.debug(f"  [{code}] skipped: {exc}")

            time.sleep(0.4)   # polite rate limiting

        self.logger.info(f"Crawl complete. Total US records: {len(all_items)}")
        return all_items

    # ──────────────────────────────────────────────────────────────────────────
    # PARSE
    # ──────────────────────────────────────────────────────────────────────────
    def parse(self, raw_data):
        if not raw_data:
            return []

        from datetime import date

        records = []
        today = date.today()

        for item in raw_data:
            try:
                if not isinstance(item, dict):
                    continue

                # ── Core identity ────────────────────────────────────────────
                name = _safe_str(
                    item.get("Description") or item.get("description") or ""
                )
                if not name:
                    continue

                dba = _safe_str(
                    item.get("DoingBusinessAs") or
                    item.get("doingBusinessAs") or ""
                )

                location_id = _safe_str(
                    item.get("LocationId") or
                    item.get("locationId") or ""
                )

                # ── Address ──────────────────────────────────────────────────
                line1 = _safe_str(item.get("Line1") or item.get("line1") or "")
                line2 = _safe_str(item.get("Line2") or item.get("line2") or "")
                address = f"{line1} {line2}".strip() if line2 else line1

                city    = _safe_str(item.get("City")      or "")
                state   = _safe_str(item.get("StateName") or "")
                zip_    = _safe_str(item.get("ZipCode")   or "")
                country = _safe_str(item.get("Country")   or "United States")

                # Normalise country
                if country.upper() in ("US", "USA", ""):
                    country = "United States"

                # ── Contact ──────────────────────────────────────────────────
                phone   = _safe_str(item.get("Phone") or item.get("phone") or "")
                email   = _safe_str(item.get("Email") or item.get("email") or "")
                website = _safe_str(item.get("WebSite") or item.get("website") or "")
                if website and not website.startswith("http"):
                    website = "https://" + website

                # ── Accreditation / niche fields ─────────────────────────────
                programme    = _safe_str(
                    item.get("SpecialtyName") or
                    item.get("specialtyName") or
                    item.get("_program_code") or ""
                ).upper()

                specialty_code = _safe_str(
                    item.get("SpecialtyCode") or
                    item.get("specialtyCode") or ""
                )

                acred_level = _extract_accreditation_level(specialty_code)
                prog_cat    = _extract_programme_category(programme)

                accred_status = item.get("AccredStatus") or item.get("accredStatus")
                program_status = "Active" if accred_status else "Inactive"

                effective_date = _format_date(
                    item.get("EffectiveDate") or item.get("effectiveDate") or ""
                )
                resurvey_date = _format_date(
                    item.get("ResurveyDate") or item.get("resurveyDate") or ""
                )

                # Days until resurvey (useful for sales intelligence)
                days_until_resurvey = ""
                if resurvey_date and len(resurvey_date) == 10:
                    try:
                        rd = date.fromisoformat(resurvey_date)
                        days_until_resurvey = str((rd - today).days)
                    except ValueError:
                        pass

                cert_number = _safe_str(
                    item.get("CertificationNumber") or
                    item.get("certificationNumber") or
                    item.get("AccreditationNumber") or ""
                )

                # Hospital-specific niche fields
                bed_count = _safe_str(
                    item.get("BedCount") or item.get("bedCount") or
                    item.get("NumberOfBeds") or ""
                )
                trauma_level = _safe_str(
                    item.get("TraumaLevel") or item.get("traumaLevel") or ""
                )
                teaching_status = _safe_str(
                    item.get("TeachingStatus") or item.get("teachingStatus") or ""
                )
                services_offered = _safe_str(
                    item.get("ServicesOffered") or item.get("servicesOffered") or
                    item.get("Services") or ""
                )

                records.append({
                    # ── Standard fields ──────────────────────────────────────
                    "company_name": name,
                    "address":      address,
                    "city":         city,
                    "state":        state,
                    "zip_code":     zip_,
                    "country":      country,
                    "phone":        phone,
                    "email":        email,
                    "website":      website,
                    # ── Niche / ACHC-specific fields ─────────────────────────
                    "location_id":           location_id,
                    "doing_business_as":     dba,
                    "accreditation_program": programme,
                    "accreditation_type":    specialty_code,
                    "accreditation_level":   acred_level,
                    "programme_category":    prog_cat,
                    "program_status":        program_status,
                    "effective_date":        effective_date,
                    "resurvey_date":         resurvey_date,
                    "days_until_resurvey":   days_until_resurvey,
                    "certification_number":  cert_number,
                    "specialty_code":        specialty_code,
                    "bed_count":             bed_count,
                    "trauma_level":          trauma_level,
                    "teaching_status":       teaching_status,
                    "services_offered":      services_offered,
                })

            except Exception:
                self.stats["total_errors"] += 1

        self.logger.info(f"Parsed {len(records)} US records")
        return records


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    crawler = AchcCrawler()
    stats = crawler.run()
    print(f"\n{'='*50}")
    print(f"  New records      : {stats['total_new']}")
    print(f"  Updated records  : {stats['total_updated']}")
    print(f"  Duplicates       : {stats['total_duplicates']}")
    print(f"  Errors           : {stats['total_errors']}")
    print(f"{'='*50}")
    if stats["total_new"] + stats["total_updated"] > 0:
        path = crawler.export_csv()
        print(f"  CSV saved        : {path}")
    else:
        print("\n  ⚠  0 records written")