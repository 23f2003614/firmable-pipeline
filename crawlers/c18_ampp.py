"""
Crawler #18 — AMPP QP Accredited Contractors (US)
Source : https://core.ampp.org/Accreditations/Search
Dataset: ampp_qp_contractors

Strategy:
  Query the API once per accreditation type (all 15 types visible on the
  search page) for country = "United States".  This guarantees we capture
  every company even if the blank-country / all-types query misses any.
  Deduplication is done in-memory by companyName+state before passing to
  BaseCrawler's hash-based dedup, so no company appears twice.

Full accreditation type list :
  QP1, QP2, QP3, QP5, QP6, QP7, QP8, QP9,
  QS1, QN1,
  AS-1 Shop, AS-1 Field, AS-2, AS-3, AS-3 ITO


"""

import re
from datetime import datetime, timezone, timedelta
from typing import Any, List, Dict

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from base_crawler import BaseCrawler


SEARCH_PAGE_URL = "https://core.ampp.org/Accreditations/Search"
API_URL         = "https://core.ampp.org/Accreditations/Search?handler=DoSearch"

# All accreditation types visible in the live UI (screenshot-confirmed)
ALL_ACCREDITATION_TYPES = [
    "QP1", "QP2", "QP3", "QP5", "QP6", "QP7", "QP8", "QP9",
    "QS1", "QN1",
    "AS-1 Shop", "AS-1 Field", "AS-2", "AS-3", "AS-3 ITO",
]

# Mapping cert prefix → human-readable category
CERT_CATEGORY_MAP = {
    "QP": "Painting Contractor",
    "QS": "Surface Preparation",
    "QN": "Nuclear Coating",
    "AS": "Aerospace Coating",
}


def _parse_expiry_dates(cert_raw: str) -> List[str]:
    """Extract all YYYY-MM-DD dates from a certificationsWithExpiry string."""
    return re.findall(r'\b(\d{4}-\d{2}-\d{2})\b', cert_raw)


def _expiry_status(dates: List[str]) -> str:
    """Return Active / Expiring Soon / Expired based on earliest expiry date."""
    if not dates:
        return "Unknown"
    today = datetime.now(timezone.utc).date()
    soon  = today + timedelta(days=90)
    parsed = []
    for d in dates:
        try:
            parsed.append(datetime.strptime(d, "%Y-%m-%d").date())
        except ValueError:
            pass
    if not parsed:
        return "Unknown"
    earliest = min(parsed)
    if earliest < today:
        return "Expired"
    elif earliest <= soon:
        return "Expiring Soon"
    return "Active"


def _cert_categories(acr_codes: List[str]) -> str:
    """Return a human-readable category string for the set of cert codes."""
    prefixes = set()
    for code in acr_codes:
        for prefix in CERT_CATEGORY_MAP:
            if code.startswith(prefix):
                prefixes.add(CERT_CATEGORY_MAP[prefix])
    if not prefixes:
        return ""
    if len(prefixes) == 1:
        return list(prefixes)[0]
    return "Multiple"


class AMPPCrawler(BaseCrawler):

    def get_dataset_name(self) -> str:
        return "ampp_qp_contractors"

    def get_source_url(self) -> str:
        return SEARCH_PAGE_URL

    def get_niche_fields(self) -> List[str]:
        return [
            "accreditation_types",
            "certifications_with_expiry",
            "cert_count",
            "cert_categories",
            "has_painting_cert",
            "has_surface_prep_cert",
            "has_nuclear_cert",
            "has_aerospace_cert",
            "earliest_expiry",
            "latest_expiry",
            "expiry_status",
            "city",
            "postal_code",
            "fax",
        ]

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    def _make_session(self):
        """GET the search page to seed cookies + grab CSRF token."""
        import requests
        from bs4 import BeautifulSoup

        session = requests.Session()
        resp = session.get(SEARCH_PAGE_URL, headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
        }, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        csrf_token = ""
        t = soup.find("input", {"name": "__RequestVerificationToken"})
        if t:
            csrf_token = t.get("value", "")
            self.logger.info(f"CSRF token obtained: {csrf_token[:20]}...")
        else:
            self.logger.warning("CSRF token NOT found — requests may be rejected")

        return session, csrf_token

    def _api_headers(self, csrf_token: str) -> dict:
        h = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": "https://core.ampp.org",
            "Referer": SEARCH_PAGE_URL,
            "X-Requested-With": "XMLHttpRequest",
        }
        if csrf_token:
            h["RequestVerificationToken"] = csrf_token
        return h

    def _post_search(self, session, headers, acr_type: str) -> List[dict]:
        """POST one search for a single accreditation type, US only."""
        payload = {
            "searchInputAccreditation": acr_type,
            "searchInputCountry":       "United States",
            "searchInputState":         "",
            "searchInputCompany":       "",
            "searchInputCity":          "",
            "searchInputZip":           "",
        }
        try:
            resp = session.post(API_URL, json=payload, headers=headers, timeout=30)
            self.logger.info(
                f"  [{acr_type}] HTTP {resp.status_code} — {len(resp.text)} bytes"
            )
            if not resp.ok:
                self.logger.warning(f"  [{acr_type}] Non-OK response, skipping")
                return []

            data = resp.json()

            # Unwrap common envelope patterns
            if isinstance(data, dict):
                for key in ("results", "Results", "data", "Data", "items", "Items"):
                    if key in data:
                        data = data[key]
                        break

            if not isinstance(data, list):
                self.logger.warning(f"  [{acr_type}] Unexpected type: {type(data)}")
                return []

            self.logger.info(f"  [{acr_type}] {len(data)} records returned")
            return data

        except Exception as exc:
            self.logger.warning(f"  [{acr_type}] Request failed: {exc}")
            return []

    # ------------------------------------------------------------------ #
    #  Override run() to migrate schema before crawling
    # ------------------------------------------------------------------ #

    def run(self):
        self._migrate_table()
        return super().run()

    # ------------------------------------------------------------------ #
    #  Schema migration — drop stale table so BaseCrawler recreates it
    # ------------------------------------------------------------------ #

    def _migrate_table(self):
        """
        If the existing DB table is missing any of our new niche columns,
        drop it so BaseCrawler's _store() will CREATE it fresh with the
        correct schema.  Safe to call on every run — it's a no-op when the
        schema is already up to date.
        """
        import sqlite3, os
        if not os.path.exists(self.db_path):
            return  # nothing to migrate
        required_cols = set(self.get_niche_fields()) | {
            "company_name", "address", "city", "state", "postal_code",
            "country", "phone", "fax", "email", "website",
            "accreditation_types", "certifications_with_expiry",
        }
        try:
            conn   = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (self.get_dataset_name(),)
            )
            if cursor.fetchone() is None:
                conn.close()
                return  # table doesn't exist yet — nothing to do
            cursor.execute(f'PRAGMA table_info("{self.get_dataset_name()}")')
            existing_cols = {row[1] for row in cursor.fetchall()}
            missing = required_cols - existing_cols
            if missing:
                self.logger.warning(
                    f"Schema out of date — missing columns: {missing}. "
                    f"Dropping table '{self.get_dataset_name()}' for fresh creation."
                )
                cursor.execute(f'DROP TABLE IF EXISTS "{self.get_dataset_name()}"')
                conn.commit()
            conn.close()
        except Exception as exc:
            self.logger.warning(f"_migrate_table error (non-fatal): {exc}")

    # ------------------------------------------------------------------ #
    #  crawl() — one POST per accreditation type
    # ------------------------------------------------------------------ #

    def crawl(self) -> Any:
        try:
            from bs4 import BeautifulSoup  # noqa: F401
        except ImportError:
            raise ImportError("Run: pip install beautifulsoup4")

        session, csrf_token = self._make_session()
        headers = self._api_headers(csrf_token)

        # key = (companyName.lower(), state.lower()) → merged item dict
        merged: Dict[tuple, dict] = {}

        for acr_type in ALL_ACCREDITATION_TYPES:
            rows = self._post_search(session, headers, acr_type)
            for item in rows:
                if not isinstance(item, dict):
                    continue
                name  = (item.get("companyName") or "").strip()
                state = (item.get("state") or "").strip()
                if not name:
                    continue
                key = (name.lower(), state.lower())
                if key not in merged:
                    merged[key] = item
                else:
                    # Merge certificationsWithExpiry from multiple type queries
                    existing_cert = merged[key].get("certificationsWithExpiry") or ""
                    new_cert      = item.get("certificationsWithExpiry") or ""
                    if new_cert and new_cert not in existing_cert:
                        merged[key]["certificationsWithExpiry"] = (
                            existing_cert + (", " if existing_cert else "") + new_cert
                        )
            self._rate_limit(0.8)

        self.logger.info(
            f"crawl() complete — {len(merged)} unique US companies across all cert types"
        )
        return list(merged.values())

    # ------------------------------------------------------------------ #
    #  parse()
    # ------------------------------------------------------------------ #

    def parse(self, raw_data: Any) -> List[Dict]:
        records: List[Dict] = []

        for item in raw_data:
            if not isinstance(item, dict):
                continue

            company = (
                item.get("companyName") or item.get("name") or item.get("Name") or ""
            ).strip()
            if not company:
                continue

            cert_raw = (item.get("certificationsWithExpiry") or "").strip()

            # ── Extract cert codes (all families) ──────────────────────────
            # Matches: QP1..QP9, QS1, QN1, AS-1 Shop, AS-1 Field, AS-2, AS-3, AS-3 ITO
            acr_codes = re.findall(
                r'\b(QP\d|QS\d|QN\d|AS-\d(?:\s+(?:Shop|Field|ITO))?)\b',
                cert_raw,
                flags=re.IGNORECASE,
            )
            # Deduplicate codes while preserving order
            seen_codes: set = set()
            unique_codes: List[str] = []
            for c in acr_codes:
                cu = c.upper()
                if cu not in seen_codes:
                    seen_codes.add(cu)
                    unique_codes.append(c.upper())

            acr_types_str = ", ".join(unique_codes)
            cert_count    = len(unique_codes)

            # ── Expiry analysis ────────────────────────────────────────────
            dates    = _parse_expiry_dates(cert_raw)
            e_status = _expiry_status(dates)
            earliest = min(dates) if dates else None
            latest   = max(dates) if dates else None

            # ── Category flags ─────────────────────────────────────────────
            cat_str          = _cert_categories(unique_codes)
            has_painting     = int(any(c.startswith("QP") for c in unique_codes))
            has_surface_prep = int(any(c.startswith("QS") for c in unique_codes))
            has_nuclear      = int(any(c.startswith("QN") for c in unique_codes))
            has_aerospace    = int(any(c.startswith("AS") for c in unique_codes))

            records.append({
                "company_name":             company,
                "address":                  item.get("street"),
                "city":                     item.get("city"),
                "state":                    item.get("state"),
                "postal_code":              item.get("postalCode"),
                "country":                  "United States",
                "phone":                    item.get("phone"),
                "fax":                      item.get("fax"),
                "email":                    item.get("email"),
                "website":                  item.get("website"),
                # ── Niche-specific ──────────────────────────────────────
                "accreditation_types":      acr_types_str,
                "certifications_with_expiry": cert_raw,
                "cert_count":               str(cert_count),
                "cert_categories":          cat_str,
                "has_painting_cert":        str(has_painting),
                "has_surface_prep_cert":    str(has_surface_prep),
                "has_nuclear_cert":         str(has_nuclear),
                "has_aerospace_cert":       str(has_aerospace),
                "earliest_expiry":          earliest,
                "latest_expiry":            latest,
                "expiry_status":            e_status,
            })

        self.logger.info(f"parse() produced {len(records)} records")
        return records


# ------------------------------------------------------------------ #
if __name__ == "__main__":
    crawler = AMPPCrawler()
    stats   = crawler.run()
    print("\n=== Final Stats ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    csv_path = crawler.export_csv()
    if csv_path:
        print(f"\nCSV exported → {csv_path}")