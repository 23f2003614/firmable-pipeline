"""
Crawler #22 — CICC Registered Immigration Consultants (Canada)

 
Phase 1: Crawls RCIC + RISIA search pages (Selenium, paginated).
Phase 2: For each record, fetches the individual profile page to extract details.
  
"""

import time
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from base_crawler import BaseCrawler

from typing import List, Dict
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


SEARCH_URLS = [
    ("RCIC",  "https://register.college-ic.ca/Public-Register-EN/Public-Register-EN/RCIC_Search.aspx"),
    ("RISIA", "https://register.college-ic.ca/Public-Register-EN/Public-Register-EN/RISIA_Search.aspx"),
]

PAGE_WAIT        = 25
MIN_ROWS         = 2


class CICCCrawler(BaseCrawler):

    def get_dataset_name(self) -> str:
        return "cicc_immigration_consultants"

    def get_source_url(self) -> str:
        return SEARCH_URLS[0][1]

    def get_niche_fields(self) -> List[str]:
        return ["registration_number", "licence_type", "licence_status",
                "firm_name", "consultant_name", "active"]

    def crawl(self) -> List[tuple]:
        page_htmls: List[tuple] = []
        for lic_type, url in SEARCH_URLS:
            self.logger.info(f"=== Crawling {lic_type} register ===")
            collected = self._crawl_one_register(lic_type, url)
            page_htmls.extend(collected)
            self.logger.info(f"  {lic_type}: {len(collected)} pages collected.")
        self.logger.info(f"Total pages collected: {len(page_htmls)}")
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

                # Check if last page
                if self._is_last_page(driver):
                    self.logger.info(f"  [{lic_type}] Last page — done.")
                    break

                # Click next and wait for page number to increment
                try:
                    clicked = self._click_next_native(driver, current_page)
                    if not clicked:
                        self.logger.info(f"  [{lic_type}] No next — done.")
                        break
                except (WebDriverException, StaleElementReferenceException) as e:
                    self.logger.warning(f"  click_next error: {e}")
                    break

                # Wait for page number to change from current_page to current_page+1
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

    def parse(self, raw_data: List[tuple]) -> List[Dict]:
        records: List[Dict] = []
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
                    records.append(rec)
                    page_recs += 1
            if page_recs > 0 or (page_idx + 1) % 100 == 0:
                self.logger.info(
                    f"  Page {page_idx+1} [{lic_type}]: {page_recs} records."
                )
        self.logger.info(f"Total parsed: {len(records)}")
        return records

    # ─────────────────────────────────────────────────────────────────────────

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
        """
        Wait until the current page number in the DOM is > previous_page.
        Returns the new page number, or previous_page if timeout.
        """
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

        # Read final value
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

    def _extract_row(self, cells, lic_type: str) -> Dict | None:
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
                "company_name":        company,
                "address":             None,
                "phone":               None,
                "website":             profile_link,
                "email":               None,
                "registration_number": reg_num,
                "licence_type":        lic_type,
                "licence_status":      lic_lvl,
                "firm_name":           firm,
                "consultant_name":     name,
                "active":              active,
            }
        except Exception as e:
            self.logger.debug(f"Row parse error: {e}")
            return None


if __name__ == "__main__":
    crawler = CICCCrawler()
    stats = crawler.run()
    print("\n=== CICC Crawl Complete ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    csv_path = crawler.export_csv()
    if csv_path:
        print(f"  CSV exported -> {csv_path}")