"""
Crawler #19 - ACEC Multi-State Engineering Firms
=================================================
Speed:  135 requests total (5 states x 27 letters), parallel fetch.
        Runs in ~2-3 minutes. Zero detail-page visits.
        All data extracted from listing cards directly.

"""

import re
import hashlib
import string
import sqlite3
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from base_crawler import BaseCrawler


# ── State chapter config ────────────────────────────────────────────────── #
STATE_CHAPTERS = [
    {
        "state":        "MA",
        "base_url":     "https://members.acecma.org",
        "listing_path": "/fullmemberfirmdirectory/FindStartsWith",
        "detail_kw":    "/fullmemberfirmdirectory/Details/",
        "skip_domains": ["acecma.org", "acec.org"],
    },
    {
        "state":        "IN",
        "base_url":     "https://members.acecindiana.org",
        "listing_path": "/directory/FindStartsWith",
        "detail_kw":    "/directory/Details/",
        "skip_domains": ["acecindiana.org", "acec.org"],
    },
    {
        "state":        "VA",
        "base_url":     "https://members.acecva.org",
        "listing_path": "/member-directory/FindStartsWith",
        "detail_kw":    "/member-directory/Details/",
        "skip_domains": ["acecva.org", "acec.org"],
    },
    {
        "state":        "GA",
        "base_url":     "https://business.acecga.org",
        "listing_path": "/list/FindStartsWith",
        "detail_kw":    "/list/Details/",
        "skip_domains": ["acecga.org", "acec.org"],
    },
    {
        "state":        "OH",
        "base_url":     "https://members.acecohio.org",
        "listing_path": "/active-member-directory/FindStartsWith",
        "detail_kw":    "/active-member-directory/Details/",
        "skip_domains": ["acecohio.org", "acec.org"],
    },
]

LETTERS = list(string.ascii_uppercase) + ["#"]

# Non-engineering entries to filter out
NON_FIRM = [
    "acec life", "acec retirement", "acec business insurance",
    "acec p.a.c", "acec pac", "acec georgia p",
    " trust", "improvement district", "regional commission",
    "college of engineering", "university of", "acec national",
    "department of transportation",
]

# Domains to skip when extracting firm website
BAD_DOMAINS = [
    "google.com", "facebook.com", "linkedin.com", "twitter.com",
    "instagram.com", "youtube.com", "acec.org", "growthzone",
    "maps.google",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; FirmableBot/1.0; B2B Research)",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

PRIMARY_SOURCE = "https://members.acecma.org/fullmemberfirmdirectory/FindStartsWith"


class ACECEngineeringFirmsCrawler(BaseCrawler):

    def get_dataset_name(self) -> str:
        return "acec_engineering_firms"

    def get_source_url(self) -> str:
        return PRIMARY_SOURCE

    def get_niche_fields(self) -> List[str]:
        return [
            "state_chapter", "membership_type", "engineering_disciplines",
            "acec_member_id", "detail_url",
        ]

    # ------------------------------------------------------------------ #
    #  crawl() - parallel A-Z fetch, no detail page visits               #
    # ------------------------------------------------------------------ #

    def crawl(self) -> List[Dict]:
        all_items: List[Dict] = []

        for chapter in STATE_CHAPTERS:
            state = chapter["state"]
            self.logger.info(f"\n{'='*50}")
            self.logger.info(f"  Crawling ACEC/{state} (parallel A-Z)...")
            self.logger.info(f"{'='*50}")

            try:
                items = self._crawl_state_parallel(chapter)
                self.logger.info(f"  DONE ACEC/{state}: {len(items)} firms")
                all_items.extend(items)
            except Exception as e:
                self.logger.error(f"  FAIL ACEC/{state}: {e}", exc_info=True)

        return all_items

    def _crawl_state_parallel(self, chapter: Dict) -> List[Dict]:
        """Fetch all 27 letter pages concurrently, then parse cards."""
        base        = chapter["base_url"]
        listing_url = base + chapter["listing_path"]
        state       = chapter["state"]

        # Build all letter URLs
        letter_urls = [(letter, f"{listing_url}?term={letter}")
                       for letter in LETTERS]

        # Parallel fetch — 8 workers (polite but fast)
        html_by_letter: Dict[str, str] = {}

        def fetch(letter_url: Tuple[str, str]):
            letter, url = letter_url
            try:
                resp = requests.get(url, headers=HEADERS, timeout=20)
                if resp.ok:
                    return letter, resp.text
            except Exception as e:
                self.logger.warning(f"  [{state}] {letter}: {e}")
            return letter, None

        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(fetch, lu): lu for lu in letter_urls}
            for future in as_completed(futures):
                letter, html = future.result()
                if html:
                    html_by_letter[letter] = html
                    self.logger.info(f"  [{state}] Letter {letter}: fetched")

        # Parse all cards from fetched HTML
        seen_urls: set     = set()
        items: List[Dict]  = []

        for letter in LETTERS:
            html = html_by_letter.get(letter)
            if not html:
                continue
            new_items = self._parse_listing_page(
                html, chapter, seen_urls
            )
            items.extend(new_items)
            self.logger.info(
                f"  [{state}] Letter {letter}: {len(new_items)} firms parsed"
            )

        return items

    # ------------------------------------------------------------------ #
    #  Parse a single letter listing page                                 #
    # ------------------------------------------------------------------ #

    def _parse_listing_page(
        self, html: str, chapter: Dict, seen_urls: set
    ) -> List[Dict]:

        base      = chapter["base_url"]          # e.g. https://members.acecma.org
        detail_kw = chapter["detail_kw"]          # e.g. /fullmemberfirmdirectory/Details/
        state     = chapter["state"]
        skip      = chapter["skip_domains"]
        # hostname only for URL dedup  e.g. members.acecma.org
        hostname  = base.split("//")[-1].rstrip("/")

        soup  = BeautifulSoup(html, "html.parser")
        items = []

        # KEY: GrowthZone renders TWO anchors per firm pointing to the same detail URL:
        #   1. A standalone <a> outside any heading (parent = div/li)  → NO sibling <ul>
        #   2. An <a> inside <h5>                                       → HAS sibling <ul>
        # We must process ONLY the <h5> anchor to reliably get address/phone/website.
        for a in soup.select(f'h5 a[href*="{detail_kw}"]'):
            href = a.get("href", "")

            # ── Build clean absolute URL ───────────────────────────────
            # Observed href formats across all states:
            #   (a) absolute:            https://members.acecma.org/...Details/slug
            #   (b) protocol-relative:   //members.acecma.org/...Details/slug
            #   (c) domain-in-path:      /members.acecma.org/...Details/slug
            #   (d) truly relative:      /fullmemberfirmdirectory/Details/slug
            if href.startswith("http"):
                full_url = href                                    # (a)
            elif href.startswith("//"):
                full_url = "https:" + href                        # (b) protocol-relative
            elif re.match(r'^/[^/]+\.[^/]', href):
                # (c) /domain.tld/path  — strip the leading /domain part
                path = re.sub(r'^/[^/]+\.[^/]+', '', href)
                full_url = base.rstrip("/") + path
            else:
                full_url = base.rstrip("/") + "/" + href.lstrip("/")  # (d)

            # Keep only the canonical path — strip any query/fragment
            full_url = full_url.split("?")[0].split("#")[0]

            # Skip "More Details" links — only process the firm name anchor
            if a.get_text(strip=True) in ("More Details", ""):
                continue

            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            # ── Firm name ─────────────────────────────────────────────
            firm_name = a.get_text(strip=True)
            if not firm_name:
                slug = full_url.rstrip("/").split("/")[-1]
                slug = re.sub(r"-\d+$", "", slug)
                firm_name = slug.replace("-", " ").title().strip()

            if any(kw in firm_name.lower() for kw in NON_FIRM):
                continue

            acec_id = full_url.rstrip("/").split("/")[-1]

            # ── Card structure ─────────────────────────────────────────
            # GrowthZone renders each firm as:
            #   <h5><a href="...Details/slug">Firm Name</a></h5>
            #   <ul>
            #     <li><a href="maps...">Address text</a></li>
            #     <li><a href="tel:...">Phone</a></li>
            #     <li><a href="http...">Visit Website</a></li>
            #     <li><a href="...Details/slug">More Details</a></li>
            #     [VA only] <li>Discipline Tag</li>  ← no anchor
            #   </ul>
            # The <h5> and <ul> are siblings inside the card container.
            h5_tag = a.parent  # the <h5> wrapping the name anchor

            # Find the sibling <ul> — it may be a direct next sibling
            # or may need to go up one more level first
            sibling_ul = None
            if h5_tag:
                sibling_ul = h5_tag.find_next_sibling("ul")
            if sibling_ul is None and h5_tag and h5_tag.parent:
                # Try parent's next sibling
                sibling_ul = h5_tag.parent.find_next_sibling("ul")

            # ── Address ────────────────────────────────────────────────
            address = street = city = state_c = zipcode = None
            src = sibling_ul if sibling_ul else h5_tag
            if src:
                # Maps links can be absolute OR protocol-relative (//maps.google or //www.google.com/maps)
                maps_a = src.select_one(
                    'a[href*="google.com/maps"], a[href*="maps.google"], '
                    'a[href*="google.com/maps/place"]'
                )
                if not maps_a:
                    # Fallback: any <li> whose text looks like an address
                    for li in (src.find_all("li", recursive=False) if src.name == "ul" else []):
                        li_text = li.get_text(strip=True)
                        if re.search(r',\s*[A-Z]{2}\s+\d{5}', li_text):
                            address = li_text
                            break
                if maps_a:
                    address = maps_a.get_text(" ", strip=True)
                if address:
                    addr_n  = re.sub(r'\s*,\s*', ', ', address)
                    addr_n  = addr_n.replace("United States", "").strip().rstrip(",")
                    m = re.search(
                        r',\s*([A-Za-z][A-Za-z\s\.]+?)\s*,\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)',
                        addr_n
                    )
                    if m:
                        city    = m.group(1).strip()
                        state_c = m.group(2).strip()
                        zipcode = m.group(3).strip()
                        street  = addr_n[:addr_n.index(m.group(1))].strip().rstrip(",").strip()

            # ── Phone ──────────────────────────────────────────────────
            phone = None
            org_phones = {"8044472057", "4045212324", "6032288580"}
            if src:
                for tel_a in src.select('a[href^="tel:"]'):
                    digits = re.sub(r"\D", "", tel_a.get("href", ""))
                    if digits and digits not in org_phones:
                        phone = tel_a.get_text(strip=True)
                        break

            # ── Website ────────────────────────────────────────────────
            website = None
            bad = BAD_DOMAINS + skip
            if src:
                for wa in src.select("a[href]"):
                    h = wa.get("href", "")
                    # Accept both absolute and protocol-relative links
                    if not (h.startswith("http") or h.startswith("//")):
                        continue
                    # Normalise protocol-relative to https
                    h_check = "https:" + h if h.startswith("//") else h
                    if (detail_kw not in h_check
                            and not any(d in h_check for d in bad)):
                        website = h_check
                        break

            # ── Membership type ────────────────────────────────────────
            membership = "Regular Member"
            mem_text   = (h5_tag.get_text(" ") if h5_tag else "") + \
                         (sibling_ul.get_text(" ") if sibling_ul else "")
            if "Non-Resident" in mem_text:
                membership = "Non-Resident Member"
            elif "Associate" in mem_text:
                membership = "Associate Member"

            # ── Engineering disciplines (VA + any state with tag-only <li>) ──
            disciplines = None
            if sibling_ul:
                disc_items = []
                for li in sibling_ul.find_all("li", recursive=False):
                    if li.find("a"):
                        continue  # skip — has link = address/phone/website/details
                    text = li.get_text(strip=True)
                    if text and 2 < len(text) < 60:
                        disc_items.append(text)
                if disc_items:
                    disciplines = ", ".join(disc_items)[:300]

            items.append({
                "company_name":            firm_name,
                "address":                 address,
                "office_street":           street,
                "city":                    city,
                "state":                   state_c or state,
                "zip":                     zipcode,
                "country":                 "US",
                "phone":                   phone,
                "website":                 website,
                "state_chapter":           state,
                "membership_type":         membership,
                "engineering_disciplines": disciplines,
                "acec_member_id":          acec_id,
                "detail_url":              full_url,
            })

        return items

    # ------------------------------------------------------------------ #
    #  parse() - pass through (all data already structured in crawl)      #
    # ------------------------------------------------------------------ #

    def parse(self, raw_data: List[Dict]) -> List[Dict]:
        # Data is already structured dicts from crawl()
        # Just return as-is — base_crawler will call _clean() on each
        return raw_data

    # ------------------------------------------------------------------ #
    #  _deduplicate - use detail_url as unique key per firm               #
    # ------------------------------------------------------------------ #

    def _deduplicate(self, records: List[Dict]) -> List[Dict]:
        seen   = set()
        unique = []
        for rec in records:
            key = rec.get("detail_url") or (
                (rec.get("company_name") or "").lower()
                + "|" + (rec.get("state") or "")
            )
            h = hashlib.md5(key.encode()).hexdigest()
            if h not in seen:
                seen.add(h)
                rec["dedup_hash"] = h
                unique.append(rec)
        return unique


# ------------------------------------------------------------------ #
#  Entry point                                                         #
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    db_path = "data/firmable.db"
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        conn.execute('DROP TABLE IF EXISTS "acec_engineering_firms"')
        conn.commit()
        conn.close()
        print("Dropped old acec_engineering_firms table.\n")

    crawler = ACECEngineeringFirmsCrawler()
    stats   = crawler.run()

    print("\n============ Final Stats ============")
    for k, v in stats.items():
        print(f"  {k:25s}: {v}")

    csv_path = crawler.export_csv()
    if csv_path:
        print(f"\n  CSV -> {csv_path}")