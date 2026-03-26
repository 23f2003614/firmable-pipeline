"""
Crawler #17 — Associated General Contractors of America (AGC)
Source: Multiple public AGC chapter member directories across US states
Coverage: MA, TX (Houston), CO, AK, IA chapters — all publicly accessible


"""

import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, List, Dict
from bs4 import BeautifulSoup
import requests
from playwright.sync_api import sync_playwright

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from base_crawler import BaseCrawler


# ---------------------------------------------------------------------------
# Chapter registry
# ---------------------------------------------------------------------------
CHAPTERS = [
    {
        "chapter_name": "AGC Massachusetts",
        "state": "MA",
        "base": "https://members.agcmass.org",
        "alpha_url": "https://members.agcmass.org/list/searchalpha/{letter}",
        "url_type": "gz",
        "warmup_url": None,
    },
    {
        "chapter_name": "AGC Houston",
        "state": "TX",
        "base": "https://members.agchouston.org",
        "alpha_url": "https://members.agchouston.org/directory/FindStartsWith?term={letter}",
        "url_type": "houston",
        "warmup_url": "https://members.agchouston.org/directory",
    },
    {
        "chapter_name": "AGC Colorado",
        "state": "CO",
        "base": "https://agccolorado.memberzone.com",
        "alpha_url": "https://agccolorado.memberzone.com/list/searchalpha/{letter}",
        "url_type": "gz",
        "warmup_url": None,
    },
    {
        "chapter_name": "AGC Alaska",
        "state": "AK",
        "base": "https://members.agcak.org",
        "alpha_url": "https://members.agcak.org/memberdirectory/FindStartsWith?term={letter}",
        "url_type": "ams",
        "warmup_url": None,
    },
    {
        "chapter_name": "AGC Iowa",
        "state": "IA",
        "base": "https://members.agcia.org",
        "alpha_url": "https://members.agcia.org/active-member-directory/FindStartsWith?term={letter}",
        "url_type": "ams",
        "warmup_url": None,
    },
]

ALPHABET = list("abcdefghijklmnopqrstuvwxyz") + ["0-9"]

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

SKIP_SPECIALTIES = {
    "agc ma", "agc of america", "agc houston", "agc alaska", "agc iowa",
    "agc colorado", "agc georgia", "member directory", "send email", "share",
    "print", "more details", "visit website", "back to search",
}


class AGCCrawler(BaseCrawler):

    def get_dataset_name(self) -> str:
        return "agc_member_directory"

    def get_source_url(self) -> str:
        return "https://www.agc.org/our-members/find-a-member"

    def get_niche_fields(self) -> List[str]:
        return [
            "member_type",
            "construction_specialties",
            "chapter_name",
            "chapter_state",
            "member_detail_url",
        ]

    # ------------------------------------------------------------------
    # Main crawl — all 5 chapters in parallel threads
    # ------------------------------------------------------------------
    def crawl(self) -> Any:
        all_raw = []
        lock = threading.Lock()

        def crawl_one(chapter):
            self.logger.info(f"  [START] {chapter['chapter_name']} ({chapter['state']})")
            try:
                entries = self._crawl_chapter(chapter)
            except Exception as e:
                self.logger.error(f"  [ERROR] {chapter['chapter_name']}: {e}", exc_info=True)
                entries = []
            self.logger.info(f"  [DONE]  {chapter['chapter_name']} -> {len(entries)} entries")
            with lock:
                all_raw.extend(entries)

        with ThreadPoolExecutor(max_workers=len(CHAPTERS)) as pool:
            futures = [pool.submit(crawl_one, ch) for ch in CHAPTERS]
            for f in as_completed(futures):
                if f.exception():
                    self.logger.error(f"  Thread exception: {f.exception()}")

        self.logger.info(f"  All chapters done. Total raw: {len(all_raw)}")
        return all_raw

    # ------------------------------------------------------------------
    # Build a requests.Session for non-Houston chapters
    # ------------------------------------------------------------------
    def _make_session(self, chapter: dict) -> requests.Session:
        session = requests.Session()
        session.headers.update({
            "User-Agent": BROWSER_UA,
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        })
        return session

    # ------------------------------------------------------------------
    # Per-chapter crawl: Playwright for Houston, requests for all others
    # ------------------------------------------------------------------
    def _crawl_chapter(self, chapter: dict) -> List[dict]:
        if chapter["url_type"] == "houston":
            return self._crawl_houston_playwright(chapter)

        entries = []
        url_type = chapter["url_type"]
        session = self._make_session(chapter)

        for letter in ALPHABET:
            if url_type == "gz":
                url = chapter["alpha_url"].format(letter=letter)
            elif url_type == "ams":
                term = "%23" if letter == "0-9" else letter.upper()
                url = chapter["alpha_url"].format(letter=term)
            else:
                continue

            try:
                resp = session.get(url, timeout=30)
                if resp.status_code == 404:
                    time.sleep(1.0)
                    continue
                if resp.status_code == 403:
                    self.logger.error(f"  HTTP 403: {url}")
                    time.sleep(3.0)
                    continue
                resp.raise_for_status()
            except requests.RequestException as e:
                self.logger.warning(f"  Request failed: {url} -> {e}")
                time.sleep(2.0)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            if url_type == "gz":
                page_entries = self._parse_gz_listing(soup, chapter, session)
            elif url_type == "ams":
                page_entries = self._parse_ams_listing(soup, chapter, session)
            else:
                page_entries = []

            entries.extend(page_entries)
            time.sleep(1.5)

        return entries

    # ------------------------------------------------------------------
    # Houston-specific Playwright crawler
    # Launches a real headless Chromium browser — bypasses all 403 blocks
    # ------------------------------------------------------------------
    def _crawl_houston_playwright(self, chapter: dict) -> List[dict]:
        entries = []
        self.logger.info("  [HOUSTON] Launching Playwright Chromium...")

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=BROWSER_UA,
                viewport={"width": 1280, "height": 800},
                locale="en-US",
            )
            page = context.new_page()

            # Warm up: visit directory homepage to set cookies
            try:
                page.goto(chapter["warmup_url"], wait_until="domcontentloaded", timeout=30000)
                time.sleep(2.0)
            except Exception as e:
                self.logger.warning(f"  [HOUSTON] Warmup failed: {e}")

            for letter in ALPHABET:
                term = "%23" if letter == "0-9" else letter.upper()
                url = chapter["alpha_url"].format(letter=term)

                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    # Wait for member cards to appear
                    page.wait_for_selector("h5 > a[href*='/directory/Details/']",
                                           timeout=10000)
                except Exception:
                    # No results for this letter — skip silently
                    time.sleep(1.0)
                    continue

                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                page_entries = self._parse_houston_listing(soup, chapter)
                self.logger.info(
                    f"  [HOUSTON] {letter.upper()} -> {len(page_entries)} entries"
                )
                entries.extend(page_entries)
                time.sleep(1.5)

            browser.close()

        self.logger.info(f"  [HOUSTON] Done -> {len(entries)} total entries")
        return entries

    # ------------------------------------------------------------------
    # GrowthZone listing parser  (MA, CO)
    # ------------------------------------------------------------------
    def _parse_gz_listing(self, soup: BeautifulSoup, chapter: dict,
                           session: requests.Session) -> List[dict]:
        entries = []
        member_links = soup.select(
            "h5 > a[href*='/list/member/'], "
            "h5 > a[href*='/memberdirectory/member/'], "
            "h5 > a[href*='/active-member-directory/member/']"
        )

        for link in member_links:
            name = link.get_text(strip=True)
            detail_url = link["href"]
            if not detail_url.startswith("http"):
                detail_url = chapter["base"] + detail_url

            card_parent = link.find_parent(["li", "div", "article"])
            member_type = ""
            if card_parent:
                spans = card_parent.find_all(string=True)
                type_candidates = [
                    s.strip() for s in spans
                    if s.strip() and s.strip() != name and len(s.strip()) < 60
                ]
                member_type = " | ".join(type_candidates[:2]) if type_candidates else ""

            address_tag = (card_parent.select_one("a[href*='google.com/maps']")
                           if card_parent else None)
            address_raw = address_tag.get_text(" ", strip=True) if address_tag else ""
            phone_tag = card_parent.select_one("a[href^='tel:']") if card_parent else None
            phone = phone_tag.get_text(strip=True) if phone_tag else ""

            entry = {
                "company_name": name,
                "phone": phone,
                "address": address_raw,
                "member_type": self._clean_member_type(member_type),
                "chapter_name": chapter["chapter_name"],
                "chapter_state": chapter["state"],
                "member_detail_url": detail_url,
                "construction_specialties": "",
                "website": "",
                "state": chapter["state"],
            }
            entry = self._enrich_gz_detail(entry, detail_url, session)
            entries.append(entry)
            time.sleep(1.0)

        return entries

    # ------------------------------------------------------------------
    # Houston listing parser  (all data inline — no detail fetch needed)
    # ------------------------------------------------------------------
    def _parse_houston_listing(self, soup: BeautifulSoup, chapter: dict) -> List[dict]:
        entries = []
        name_links = soup.select("h5 > a[href*='/directory/Details/']")
        seen = set()

        for link in name_links:
            href = link.get("href", "")
            if href in seen:
                continue
            seen.add(href)

            name = link.get_text(strip=True)
            if not name or len(name) < 2:
                continue
            if not href.startswith("http"):
                href = chapter["base"] + href

            card = link.find_parent(["div", "li", "article"])
            if not card:
                continue

            addr_a = card.select_one("a[href*='google.com/maps']")
            address_raw = addr_a.get_text(" ", strip=True) if addr_a else ""
            state_val = self._extract_state(address_raw) or chapter["state"]

            tel_a = card.select_one("a[href^='tel:']")
            phone = tel_a.get_text(strip=True) if tel_a else ""

            website = ""
            for a in card.select("a[href^='http']"):
                hv = a.get("href", "")
                if not any(x in hv for x in
                           ["agchouston", "google.com", "facebook", "linkedin",
                            "twitter", "details", "directory", "cloudinary",
                            "px.ads"]):
                    website = hv
                    break

            raw_texts = []
            for el in card.find_all(string=True):
                t = el.strip()
                if (t and t != name and t not in address_raw
                        and t.lower() not in ("more details", "visit website", "")
                        and 3 < len(t) < 80):
                    raw_texts.append(t)

            specialties = [
                t for t in raw_texts
                if t.lower() not in SKIP_SPECIALTIES
                and not t.startswith("(")
                and not t.startswith("http")
            ]
            specialties = list(dict.fromkeys(specialties))

            joined = " ".join(specialties).upper()
            if "GENERAL CONTRACTOR" in joined or "CONSTRUCTION MANAGEMENT" in joined:
                member_type = "General Contractor"
            elif "SUBCONTRACTOR" in joined or "SPECIALTY" in joined:
                member_type = "Subcontractor"
            elif any(x in joined for x in ("INSURANCE", "SURETY", "FINANCIAL", "LEGAL", "SOFTWARE")):
                member_type = "Associate"
            else:
                member_type = ""

            entries.append({
                "company_name": name,
                "phone": phone,
                "address": address_raw,
                "website": website,
                "member_type": member_type,
                "chapter_name": chapter["chapter_name"],
                "chapter_state": chapter["state"],
                "member_detail_url": href,
                "construction_specialties": " | ".join(specialties[:15]),
                "state": state_val,
            })

        return entries

    # ------------------------------------------------------------------
    # AMS listing parser  (AK, IA)
    # ------------------------------------------------------------------
    def _parse_ams_listing(self, soup: BeautifulSoup, chapter: dict,
                            session: requests.Session) -> List[dict]:
        entries = []
        member_links = soup.select(
            "a.member-name, h3 > a, h4 > a, h5 > a, "
            ".directory-list-item a, "
            "a[href*='/Details/'], "
            "a[href*='/memberdirectory/Details/'], "
            "a[href*='/active-member-directory/Details/']"
        )
        seen = set()

        for link in member_links:
            href = link.get("href", "")
            if not href or ("Details" not in href and "directory" not in href):
                continue
            if href in seen:
                continue
            seen.add(href)

            name = link.get_text(strip=True)
            if not name or len(name) < 2:
                continue
            # Only prepend base for true relative paths — AMS hrefs sometimes
            # already contain the full hostname (e.g. members.agcak.org/...)
            if not href.startswith("http"):
                href = chapter["base"].rstrip("/") + "/" + href.lstrip("/")

            card = link.find_parent(["li", "div", "tr", "article"])
            phone, address_raw = "", ""
            if card:
                phone_a = card.select_one("a[href^='tel:']")
                phone = phone_a.get_text(strip=True) if phone_a else ""
                addr_a = card.select_one("a[href*='google.com/maps']")
                address_raw = addr_a.get_text(" ", strip=True) if addr_a else ""

            entry = {
                "company_name": name,
                "phone": phone,
                "address": address_raw,
                "member_type": "",
                "chapter_name": chapter["chapter_name"],
                "chapter_state": chapter["state"],
                "member_detail_url": href,
                "construction_specialties": "",
                "website": "",
                "state": chapter["state"],
            }
            entry = self._enrich_ams_detail(entry, href, session)
            entries.append(entry)
            time.sleep(1.0)

        return entries

    # ------------------------------------------------------------------
    # GrowthZone detail enrichment
    # ------------------------------------------------------------------
    def _enrich_gz_detail(self, entry: dict, detail_url: str,
                           session: requests.Session) -> dict:
        if not detail_url:
            return entry
        try:
            resp = session.get(detail_url, timeout=30)
            resp.raise_for_status()
        except Exception:
            return entry

        soup = BeautifulSoup(resp.text, "html.parser")

        for a in soup.select("a[href^='http']"):
            hv = a.get("href", "")
            if not any(x in hv for x in
                       ["agc", "google.com", "facebook", "linkedin", "twitter",
                        "chambermaster", "growthzone", "memberzone", "maps"]):
                entry["website"] = hv.strip()
                break

        # Specialties from title: "Company | MemberType | Cat1 | Cat2 - AGC XX"
        title_tag = soup.find("title")
        specialties = []
        if title_tag:
            parts = [p.strip() for p in title_tag.get_text().split("|")]
            if len(parts) > 2:
                specialties = parts[1:]
                if specialties:
                    specialties[-1] = re.sub(r"\s*[-–]\s*AGC.*$", "", specialties[-1]).strip()

        # Known GrowthZone member type labels — first matching category becomes member_type
        GZ_MEMBER_TYPES = {
            "constructor", "constructors", "subcontractor", "subcontractors",
            "associate", "associates", "constructor - mcap contributor",
            "subcontractor - mcap contributor", "program managers",
        }
        if specialties and not entry.get("member_type"):
            if specialties[0].lower() in GZ_MEMBER_TYPES:
                entry["member_type"] = specialties[0]
                specialties = specialties[1:]  # remove from specialties list

        if not specialties:
            for heading in soup.find_all(["h5", "h6", "strong", "b"]):
                if "categor" in heading.get_text().lower():
                    sib = heading.find_next_sibling()
                    while sib:
                        t = sib.get_text(strip=True)
                        if t:
                            specialties += [x.strip() for x in re.split(r"[,\n|]", t) if x.strip()]
                        sib = sib.find_next_sibling()
                        if len(specialties) > 20:
                            break
                    break

        specialties = list(dict.fromkeys(
            s for s in specialties
            if s and s.lower() not in SKIP_SPECIALTIES
            and s.lower() != entry.get("company_name", "").lower()
            and len(s) > 2
        ))
        entry["construction_specialties"] = " | ".join(specialties[:15])

        if not entry.get("phone"):
            tel_a = soup.select_one("a[href^='tel:']")
            if tel_a:
                entry["phone"] = tel_a.get_text(strip=True)
        if not entry.get("address"):
            addr_a = soup.select_one("a[href*='google.com/maps']")
            if addr_a:
                entry["address"] = addr_a.get_text(" ", strip=True)

        return entry

    # ------------------------------------------------------------------
    # AMS detail enrichment
    # ------------------------------------------------------------------
    def _enrich_ams_detail(self, entry: dict, detail_url: str,
                            session: requests.Session) -> dict:
        if not detail_url:
            return entry
        try:
            resp = session.get(detail_url, timeout=30)
            resp.raise_for_status()
        except Exception:
            return entry

        soup = BeautifulSoup(resp.text, "html.parser")

        for a in soup.select("a[href^='http']"):
            hv = a.get("href", "")
            if not any(x in hv for x in
                       ["agc", "google.com", "facebook", "linkedin", "twitter",
                        "micronet", "chambermaster", "maps"]):
                entry["website"] = hv.strip()
                break

        specialties = []
        for el in soup.select(".member-category, .category-item, li.category, "
                               "span.category, td.category, .detail-categories li"):
            t = el.get_text(strip=True)
            if t and len(t) > 2:
                specialties.append(t)

        if not specialties:
            cat_div = soup.select_one(
                ".categories, #categories, .member-categories, "
                "[class*='categor'], [id*='categor']"
            )
            if cat_div:
                for line in cat_div.get_text("\n").splitlines():
                    line = line.strip()
                    if 2 < len(line) < 80:
                        specialties.append(line)

        if not entry.get("phone"):
            tel_a = soup.select_one("a[href^='tel:']")
            if tel_a:
                entry["phone"] = tel_a.get_text(strip=True)
        if not entry.get("address"):
            # Try Google Maps link first
            addr_a = soup.select_one("a[href*='google.com/maps']")
            if addr_a:
                entry["address"] = addr_a.get_text(" ", strip=True)
            else:
                # AMS detail pages have address in a plain <address> tag or
                # a div/span with class containing 'address'/'location'/'contact'
                for sel in ["address", "[class*='address']", "[class*='location']",
                            "[class*='contact-info']", ".vcard", "[itemprop='address']"]:
                    addr_el = soup.select_one(sel)
                    if addr_el:
                        raw = " ".join(addr_el.get_text(" ").split())
                        if re.search(r'\d', raw) and len(raw) > 10:
                            entry["address"] = raw[:200]
                            break
        if not entry.get("member_type"):
            for el in soup.select(".member-type, .membership-type, span.type, .member-class"):
                entry["member_type"] = el.get_text(strip=True)
                break

        specialties = list(dict.fromkeys(
            s for s in specialties
            if s and s.lower() not in SKIP_SPECIALTIES and len(s) > 2
        ))
        entry["construction_specialties"] = " | ".join(specialties[:15])
        return entry

    # ------------------------------------------------------------------
    # Parse
    # ------------------------------------------------------------------
    def parse(self, raw_data: Any) -> List[Dict]:
        return raw_data

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _clean_member_type(self, raw: str) -> str:
        skip = {"send email", "share", "print", "google maps", "website",
                "more details", "visit website", ""}
        parts = [p.strip() for p in raw.split("|")]
        clean = [p for p in parts if p.lower() not in skip and len(p) < 80]
        return " | ".join(clean[:3])

    def _extract_state(self, address: str) -> str:
        m = re.search(
            r'\b(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|'
            r'MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|'
            r'TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)\b',
            address
        )
        return m.group(1) if m else ""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    crawler = AGCCrawler()
    stats = crawler.run()
    print("\n=== Run Complete ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    csv_path = crawler.export_csv()
    if csv_path:
        print(f"\nCSV exported -> {csv_path}")