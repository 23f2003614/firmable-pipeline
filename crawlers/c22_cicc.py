"""
Crawler 22 — CICC Registered Immigration Consultants (Canada)
Source   : https://college-ic.ca
Records  : ~16,706 | Canada

Fetch    : Phase 1 — Selenium paginates through RCIC + RISIA registers.
           Saves raw HTML to pages.pkl checkpoint after each batch.
           On restart, Phase 1 is skipped entirely if checkpoint exists.
           Phase 2 — fetches each consultant's profile page for enrichment.

Parse    : Phase 1 extracts basic info + profile URLs. Phase 2 extracts
           registration number, languages, authorized-to-appear status,
           practice areas from individual profile pages.

           Checkpoint/resume system — a crash at page 800 of 2,000
           restarts from page 800, not page 1. Essential for long crawls.
"""

import time
import sys
import os
import re
import json
import pickle

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from base_crawler import BaseCrawler

from typing import List, Dict, Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    WebDriverException,
)
from bs4 import BeautifulSoup
import requests

# ─── Constants ───────────────────────────────────────────────────────────────

SEARCH_URLS = [
    ("RCIC",  "https://register.college-ic.ca/Public-Register-EN/Public-Register-EN/RCIC_Search.aspx"),
    ("RISIA", "https://register.college-ic.ca/Public-Register-EN/Public-Register-EN/RISIA_Search.aspx"),
]

PROFILE_BASE = "https://register.college-ic.ca/Public-Register-EN/Public-Register-EN/MemberProfile.aspx"

PAGE_WAIT   = 25
MIN_ROWS    = 2
PROFILE_DELAY = 0.5   # seconds between profile fetches (polite crawling)

# Human-readable licence level map
LICENCE_LEVEL_MAP = {
    "RCIC - L1":      "Registered Consultant – Entry Level (L1): may practise under supervision",
    "RCIC - L2":      "Registered Consultant – Full Licence (L2): independent practise authorised",
    "RCIC-IRB - L3":  "Registered Consultant – IRB Level (L3): authorised to appear before Immigration & Refugee Board",
    "RCIC":           "Registered Consultant – Legacy/Undifferentiated",
    "RISIA - L4":     "Regulated International Student Immigration Adviser – Institutional (L4)",
    "RISIA - L5":     "Regulated International Student Immigration Adviser – Individual (L5)",
    "RISIA":          "Regulated International Student Immigration Adviser – Legacy",
}

# IRB authorisation derived from licence level
IRB_AUTHORISED_LEVELS = {"RCIC-IRB - L3"}

# ── Checkpoint config ─────────────────────────────────────────────────────────
CHECKPOINT_DIR   = "data/cicc_checkpoint"
PAGES_CACHE      = os.path.join(CHECKPOINT_DIR, "pages.pkl")
ENRICHED_CACHE   = os.path.join(CHECKPOINT_DIR, "enriched.jsonl")
CHECKPOINT_EVERY = 100   # flush enriched records to disk every N profiles


class CICCCrawler(BaseCrawler):

    # ── Abstract method implementations ──────────────────────────────────────

    def get_dataset_name(self) -> str:
        return "cicc_immigration_consultants"

    def get_source_url(self) -> str:
        return SEARCH_URLS[0][1]

    def get_niche_fields(self) -> List[str]:
        return [
            "registration_number",
            "licence_type",
            "licence_status",
            "licence_level_description",
            "irb_authorized",
            "firm_name",
            "consultant_name",
            "active",
            "city",
            "province",
            "postal_code",
            "languages_spoken",
            "areas_of_practice",
            "profile_url",
        ]
    def run(self):
        """Migrate schema before running — adds any missing columns to existing table."""
        import sqlite3
        if os.path.exists(self.db_path):
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (self.get_dataset_name(),)
            )
            if cur.fetchone():
                cur.execute(f'PRAGMA table_info("{self.get_dataset_name()}")')
                existing_cols = {row[1] for row in cur.fetchall()}
                for col in self.get_niche_fields():
                    if col not in existing_cols:
                        self.logger.info(f"Schema migration: adding column '{col}'")
                        cur.execute(
                            f'ALTER TABLE "{self.get_dataset_name()}" ADD COLUMN "{col}" TEXT'
                        )
                conn.commit()
            conn.close()
        return super().run()



    # ── Phase 1: crawl paginated search results ───────────────────────────────

    def crawl(self) -> List[tuple]:
        """
        Returns list of (lic_type, page_html) tuples.
        If a pages checkpoint exists, loads it and skips Selenium entirely.
        """
        os.makedirs(CHECKPOINT_DIR, exist_ok=True)

        if os.path.exists(PAGES_CACHE):
            self.logger.info(
                f"[CHECKPOINT] pages.pkl found — loading cached pages, skipping Phase 1 crawl."
            )
            with open(PAGES_CACHE, "rb") as f:
                page_htmls = pickle.load(f)
            self.logger.info(f"  Loaded {len(page_htmls)} cached pages.")
            return page_htmls

        page_htmls: List[tuple] = []
        for lic_type, url in SEARCH_URLS:
            self.logger.info(f"=== Crawling {lic_type} search register ===")
            collected = self._crawl_one_register(lic_type, url)
            page_htmls.extend(collected)
            self.logger.info(f"  {lic_type}: {len(collected)} pages collected.")

        self.logger.info(f"Total pages collected: {len(page_htmls)}")
        self.logger.info(f"[CHECKPOINT] Saving pages to {PAGES_CACHE} ...")
        with open(PAGES_CACHE, "wb") as f:
            pickle.dump(page_htmls, f)
        self.logger.info(f"[CHECKPOINT] pages.pkl saved.")
        return page_htmls

    def _crawl_one_register(self, lic_type: str, url: str) -> List[tuple]:
        page_htmls: List[tuple] = []
        driver = self._build_driver()
        try:
            driver.get(url)
            time.sleep(3)
            self._submit_blank_search(driver)
            self.logger.info("  Waiting for first results...")

            if not self._wait_for_rows(driver):
                self.logger.warning(f"  No rows for {lic_type} — skipping.")
                return page_htmls

            current_page = self._wait_for_page_number(driver, 0)
            if current_page == 0:
                self.logger.warning("  Could not detect page number.")
                return page_htmls

            while True:
                self.logger.info(f"  [{lic_type}] Page {current_page} captured.")
                page_htmls.append((lic_type, driver.page_source))

                if self._is_last_page(driver):
                    self.logger.info(f"  [{lic_type}] Last page — done.")
                    break

                try:
                    clicked = self._click_next_native(driver, current_page)
                    if not clicked:
                        self.logger.info(f"  [{lic_type}] No next — done.")
                        break
                except (WebDriverException, StaleElementReferenceException) as e:
                    self.logger.warning(f"  click_next error: {e}")
                    break

                next_page = self._wait_for_page_number(driver, current_page)
                if next_page <= current_page:
                    self.logger.info(
                        f"  [{lic_type}] Page number did not advance "
                        f"({next_page} <= {current_page}). Done."
                    )
                    break
                current_page = next_page

        finally:
            try:
                driver.quit()
            except Exception:
                pass

        return page_htmls

    # ── Phase 2: parse search result pages ───────────────────────────────────

    def parse(self, raw_data: List[tuple]) -> List[Dict]:
        """
        Parse paginated search HTML, then enrich each record
        by fetching its individual profile page.
        """
        # Step A — extract base records from search pages
        base_records: List[Dict] = []
        for page_idx, (lic_type, html) in enumerate(raw_data):
            soup = BeautifulSoup(html, "html.parser")
            table = soup.find("table", class_="rgMasterTable")
            if not table:
                continue
            tbodies = table.find_all("tbody")
            if len(tbodies) < 2:
                continue
            rows = tbodies[1].find_all("tr")
            page_recs = 0
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 5:
                    continue
                rec = self._extract_row(cells, lic_type)
                if rec:
                    base_records.append(rec)
                    page_recs += 1
            if page_recs > 0 or (page_idx + 1) % 100 == 0:
                self.logger.info(
                    f"  Page {page_idx+1} [{lic_type}]: {page_recs} records."
                )
        self.logger.info(f"Base records extracted: {len(base_records)}")

        # Step B — enrich with profile page detail
        enriched = self._enrich_all(base_records)
        self.logger.info(f"Total enriched records: {len(enriched)}")
        return enriched

    # ── Profile enrichment ────────────────────────────────────────────────────

    def _enrich_all(self, records: List[Dict]) -> List[Dict]:
        """
        Fetch each consultant's profile page and add niche-specific fields.

        Checkpoint strategy:
        - enriched.jsonl is an append-only log; each line is one completed record.
        - On start, we load all already-done reg_nums into a set.
        - We skip any record whose reg_num is already in that set.
        - Every CHECKPOINT_EVERY records we flush newly enriched records to disk.
        - On completion the final flush ensures nothing is lost.
        """
        os.makedirs(CHECKPOINT_DIR, exist_ok=True)

        # ── Load already-enriched records ────────────────────────────────────
        done: Dict[str, Dict] = {}          # reg_num → enriched record
        if os.path.exists(ENRICHED_CACHE):
            with open(ENRICHED_CACHE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                        key = rec.get("registration_number", "")
                        if key:
                            done[key] = rec
                    except json.JSONDecodeError:
                        pass
            self.logger.info(
                f"[CHECKPOINT] Loaded {len(done)} already-enriched records from enriched.jsonl"
            )

        # ── Open append file for new results ─────────────────────────────────
        enriched_file = open(ENRICHED_CACHE, "a", encoding="utf-8")

        def _flush(batch: List[Dict]):
            for r in batch:
                enriched_file.write(json.dumps(r, ensure_ascii=False) + "\n")
            enriched_file.flush()

        session = requests.Session()
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        })

        total        = len(records)
        pending      = 0   # newly enriched since last flush
        batch_buf: List[Dict] = []

        for idx, rec in enumerate(records):
            reg_num = rec.get("registration_number", "")

            # ── Resume: skip if already done ─────────────────────────────────
            if reg_num and reg_num in done:
                rec.update(done[reg_num])   # merge saved enrichment into rec
                continue

            # ── Fetch profile ─────────────────────────────────────────────────
            profile_url = f"{PROFILE_BASE}?Reg={reg_num}" if reg_num else None
            if profile_url:
                rec["profile_url"] = profile_url
                try:
                    resp = session.get(profile_url, timeout=20)
                    if resp.status_code == 200:
                        detail = self._parse_profile(resp.text, rec.get("licence_status", ""))
                        rec.update(detail)
                    else:
                        self.logger.debug(
                            f"  Profile HTTP {resp.status_code} for {reg_num}"
                        )
                except Exception as e:
                    self.logger.debug(f"  Profile fetch error {reg_num}: {e}")

            self._apply_derived_fields(rec)

            # ── Buffer for checkpoint flush ───────────────────────────────────
            if reg_num:
                batch_buf.append(rec)
                pending += 1

            if pending >= CHECKPOINT_EVERY:
                _flush(batch_buf)
                batch_buf.clear()
                pending = 0
                self.logger.info(
                    f"  [CHECKPOINT] Flushed {CHECKPOINT_EVERY} records "
                    f"({idx+1}/{total} total processed)"
                )

            if (idx + 1) % 500 == 0:
                self.logger.info(f"  Enriched {idx+1}/{total} profiles...")

            time.sleep(PROFILE_DELAY)

        # ── Final flush ───────────────────────────────────────────────────────
        if batch_buf:
            _flush(batch_buf)
            self.logger.info(f"  [CHECKPOINT] Final flush: {len(batch_buf)} records.")

        enriched_file.close()
        return records

    def _parse_profile(self, html: str, licence_status: str) -> Dict:
        """
        Extract all available niche fields from a CICC member profile page.

        Fields targeted:
          address, city, province, postal_code
          phone, email, website
          languages_spoken
          areas_of_practice
        """
        soup = BeautifulSoup(html, "html.parser")
        result: Dict = {}

        # ── Helper: find label → adjacent value ──────────────────────────────
        def find_field(label_text: str) -> Optional[str]:
            """
            CICC profile uses <span class='formLabel'> beside <span class='formData'>.
            We scan all labels for a case-insensitive substring match.
            """
            for label_el in soup.find_all(
                lambda el: el.name in ("span", "td", "label", "div")
                and label_text.lower() in (el.get_text(strip=True) or "").lower()
            ):
                # Try next sibling
                sibling = label_el.find_next_sibling()
                if sibling:
                    val = sibling.get_text(separator=" ", strip=True)
                    if val and val.lower() not in ("n/a", "-", ""):
                        return val
                # Try parent's next sibling
                parent = label_el.parent
                if parent:
                    ps = parent.find_next_sibling()
                    if ps:
                        val = ps.get_text(separator=" ", strip=True)
                        if val and val.lower() not in ("n/a", "-", ""):
                            return val
            return None

        def find_by_id_suffix(suffix: str) -> Optional[str]:
            """Many CICC profile fields use ASP.NET IDs ending in a known suffix."""
            for el in soup.find_all(id=re.compile(suffix + r"$", re.I)):
                val = el.get_text(separator=" ", strip=True)
                if val and val.lower() not in ("n/a", "-", ""):
                    return val
            return None

        # ── Address block ────────────────────────────────────────────────────
        # CICC renders address in a block; try composite text first
        address_val = find_by_id_suffix("lblAddress") or find_field("Address")
        if address_val:
            result["address"] = address_val
            # Try to extract city / province / postal from address string
            # Typical CA format: "123 Main St, Calgary, AB  T2P 1A1"
            city_prov_postal = re.search(
                r",\s*([^,]+),\s*([A-Z]{2})\s+([\w\d]{3}\s?[\w\d]{3})",
                address_val
            )
            if city_prov_postal:
                result["city"]        = city_prov_postal.group(1).strip()
                result["province"]    = city_prov_postal.group(2).strip()
                result["postal_code"] = city_prov_postal.group(3).strip().upper()
            else:
                # Fallback: try individual fields
                result["city"]        = (
                    find_by_id_suffix("lblCity") or find_field("City")
                )
                result["province"]    = (
                    find_by_id_suffix("lblProvince") or find_field("Province")
                    or find_by_id_suffix("lblState") or find_field("State")
                )
                result["postal_code"] = (
                    find_by_id_suffix("lblPostalCode") or find_field("Postal")
                    or find_by_id_suffix("lblZip") or find_field("Zip")
                )
        else:
            result["city"]        = find_by_id_suffix("lblCity") or find_field("City")
            result["province"]    = (
                find_by_id_suffix("lblProvince") or find_field("Province")
            )
            result["postal_code"] = (
                find_by_id_suffix("lblPostalCode") or find_field("Postal Code")
            )

        # ── Contact details ──────────────────────────────────────────────────
        result["phone"] = (
            find_by_id_suffix("lblPhone") or find_field("Phone")
            or find_by_id_suffix("lblTelephone") or find_field("Telephone")
        )

        # Email — look for mailto link first (most reliable)
        email_tag = soup.find("a", href=re.compile(r"^mailto:", re.I))
        if email_tag:
            result["email"] = email_tag["href"].replace("mailto:", "").strip().lower()
        else:
            raw_email = (
                find_by_id_suffix("lblEmail") or find_field("Email")
            )
            if raw_email and "@" in raw_email:
                result["email"] = raw_email.lower()

        # Website — look for external http(s) link (not the CICC portal itself)
        for a in soup.find_all("a", href=re.compile(r"^https?://", re.I)):
            href = a["href"]
            if "college-ic.ca" not in href and "register.college" not in href:
                result["website"] = href
                break
        if not result.get("website"):
            raw_web = find_by_id_suffix("lblWebsite") or find_field("Website")
            if raw_web and raw_web.startswith("http"):
                result["website"] = raw_web

        # ── Languages spoken ─────────────────────────────────────────────────
        langs = (
            find_by_id_suffix("lblLanguages") or find_field("Language")
            or find_by_id_suffix("lblLanguagesSpoken") or find_field("Languages Spoken")
        )
        if langs:
            result["languages_spoken"] = langs

        # ── Areas of practice ────────────────────────────────────────────────
        areas = (
            find_by_id_suffix("lblAreasOfPractice") or find_field("Areas of Practice")
            or find_by_id_suffix("lblSpecialization") or find_field("Specialization")
            or find_by_id_suffix("lblPracticeAreas") or find_field("Practice")
        )
        if areas:
            result["areas_of_practice"] = areas

        return result

    def _apply_derived_fields(self, rec: Dict):
        """Compute fields that can be derived without a network call."""
        lic_status = rec.get("licence_status", "")

        # Human-readable licence level description
        rec.setdefault(
            "licence_level_description",
            LICENCE_LEVEL_MAP.get(lic_status, lic_status or "Unknown")
        )

        # IRB authorisation flag
        rec.setdefault(
            "irb_authorized",
            "Yes" if lic_status in IRB_AUTHORISED_LEVELS else "No"
        )

    # ── Selenium helpers ──────────────────────────────────────────────────────

    def _build_driver(self) -> webdriver.Chrome:
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
        driver = webdriver.Chrome(options=opts)
        driver.implicitly_wait(3)
        return driver

    def _submit_blank_search(self, driver):
        strategies = [
            (By.XPATH, "//input[@type='submit' and (contains(@value,'Search') or contains(@value,'Find'))]"),
            (By.CSS_SELECTOR, "input[type='submit']"),
            (By.CSS_SELECTOR, "button[type='submit']"),
        ]
        for by, sel in strategies:
            try:
                btn = WebDriverWait(driver, 8).until(
                    EC.element_to_be_clickable((by, sel))
                )
                btn.click()
                self.logger.info(f"  Search submitted via [{sel}]")
                return
            except (TimeoutException, NoSuchElementException):
                continue
        time.sleep(PAGE_WAIT)

    def _wait_for_rows(self, driver) -> bool:
        def _has_rows(d):
            try:
                tds = d.find_elements(By.CSS_SELECTOR, "table tr td")
                return len(tds) >= MIN_ROWS
            except (StaleElementReferenceException, WebDriverException):
                return False
        try:
            WebDriverWait(driver, PAGE_WAIT).until(_has_rows)
            return True
        except TimeoutException:
            self.logger.warning("  Timed out waiting for rows.")
            return False

    def _wait_for_page_number(self, driver, previous_page: int) -> int:
        """Wait for page number in DOM to advance past previous_page."""
        def _page_advanced(d):
            try:
                el = d.find_element(By.CSS_SELECTOR, "a.rgCurrentPage")
                txt = el.text.strip()
                if txt.isdigit():
                    return int(txt) > previous_page
                return False
            except (NoSuchElementException, StaleElementReferenceException):
                return False

        try:
            WebDriverWait(driver, PAGE_WAIT).until(_page_advanced)
        except TimeoutException:
            pass

        try:
            el = driver.find_element(By.CSS_SELECTOR, "a.rgCurrentPage")
            txt = el.text.strip()
            if txt.isdigit():
                return int(txt)
        except (NoSuchElementException, StaleElementReferenceException):
            pass
        return previous_page

    def _is_last_page(self, driver) -> bool:
        try:
            btn = driver.find_element(
                By.CSS_SELECTOR, "input[type='submit'][title='Next Page']"
            )
            onclick = (btn.get_attribute("onclick") or "").strip()
            disabled = btn.get_attribute("disabled")
            return onclick == "return false;" or bool(disabled)
        except (NoSuchElementException, StaleElementReferenceException):
            return False

    def _click_next_native(self, driver, current_page: int) -> bool:
        next_page = current_page + 1

        # Strategy 1: numeric page link
        try:
            link = driver.find_element(
                By.XPATH, f"//a[@title='Go to Page {next_page}']"
            )
            link.click()
            return True
        except (NoSuchElementException, StaleElementReferenceException):
            pass

        # Strategy 2: __doPostBack via Next Page button
        try:
            btn = driver.find_element(
                By.CSS_SELECTOR, "input[type='submit'][title='Next Page']"
            )
            onclick = (btn.get_attribute("onclick") or "").strip()
            if onclick == "return false;":
                return False
            btn_name = btn.get_attribute("name") or ""
            if btn_name:
                driver.execute_script(f"__doPostBack('{btn_name}', '')")
                self.logger.info("  Triggered __doPostBack for next page.")
                return True
        except (NoSuchElementException, StaleElementReferenceException):
            pass

        return False

    # ── Row extraction from search table ─────────────────────────────────────

    def _extract_row(self, cells, lic_type: str) -> Optional[Dict]:
        try:
            def text(cell):
                t = cell.get_text(separator=" ", strip=True)
                return t if t and t.lower() not in ("n/a", "-", "", "select") else None

            reg_num = text(cells[1])
            name    = text(cells[2])
            firm    = text(cells[3])
            lic_lvl = text(cells[4])
            active  = text(cells[5]) if len(cells) > 5 else None

            if not name and not firm:
                return None

            company = firm or name

            # Profile URL extracted from table link (used as website placeholder
            # until proper enrichment fills in the real firm website)
            profile_link = None
            for col in [cells[2], cells[3]]:
                a_tag = col.find("a")
                if a_tag and a_tag.get("href"):
                    href = a_tag["href"]
                    profile_link = (
                        href if href.startswith("http")
                        else "https://register.college-ic.ca" + href
                    )
                    break

            return {
                # ── Core fields ──────────────────────────────────────────────
                "company_name":        company,
                "address":             None,
                "phone":               None,
                "website":             None,          # filled by enrichment
                "email":               None,          # filled by enrichment
                # ── Niche fields (base) ──────────────────────────────────────
                "registration_number": reg_num,
                "licence_type":        lic_type,
                "licence_status":      lic_lvl,
                "licence_level_description": None,   # filled by _apply_derived_fields
                "irb_authorized":      None,          # filled by _apply_derived_fields
                "firm_name":           firm,
                "consultant_name":     name,
                "active":              active,
                # ── Profile enrichment fields (filled in phase 2) ────────────
                "city":                None,
                "province":            None,
                "postal_code":         None,
                "languages_spoken":    None,
                "areas_of_practice":   None,
                "profile_url":         profile_link,
            }
        except Exception as e:
            self.logger.debug(f"Row parse error: {e}")
            return None


# ─── Entry point ──────────────────────────────────────────────────────────────

def reset_checkpoint():
    """Delete all checkpoint files to force a full re-crawl."""
    import shutil
    if os.path.exists(CHECKPOINT_DIR):
        shutil.rmtree(CHECKPOINT_DIR)
        print(f"[CHECKPOINT] Deleted {CHECKPOINT_DIR}/ — next run will start fresh.")
    else:
        print("[CHECKPOINT] No checkpoint directory found — nothing to reset.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="CICC Immigration Consultants Crawler")
    parser.add_argument(
        "--reset", action="store_true",
        help="Delete checkpoint files and start a full re-crawl"
    )
    args = parser.parse_args()

    if args.reset:
        reset_checkpoint()

    crawler = CICCCrawler()
    stats = crawler.run()
    print("\n=== CICC Crawl Complete ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    csv_path = crawler.export_csv()
    if csv_path:
        print(f"  CSV exported -> {csv_path}")