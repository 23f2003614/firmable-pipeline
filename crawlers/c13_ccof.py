"""
Crawler #13 — CCOF Certified Organic Operators (US)
Source  : https://ccof.org/directory-member/{slug}/
Sitemap : https://ccof.org/wp-sitemap-posts-directory-member-{N}.xml

Strategy:
  Step 1 — Pull all /directory-member/ slugs from WordPress XML sitemaps
            (ccof.org/wp-sitemap-posts-directory-member-1.xml, -2.xml, …)
  Step 2 — For each slug, fetch the SSR detail page and parse structured
            fields directly from the rendered HTML.

"""

import re
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from base_crawler import BaseCrawler

from typing import Any, Dict, List
from bs4 import BeautifulSoup

BASE = "https://ccof.org"
# Real sitemap pattern confirmed from ccof.org/wp-sitemap.xml:
#   directory-member-sitemap.xml, directory-member-sitemap2.xml, ... (up to 9)
SITEMAP_BASE  = f"{BASE}/directory-member-sitemap"   # + "" | "2" | "3" ... ".xml"


class CCOFCrawler(BaseCrawler):

    REQUEST_DELAY   = 1.2   # seconds between requests
    MAX_SITEMAP_IDX = 15    # confirmed 9 exist; cap at 15 for safety

    def get_dataset_name(self) -> str:
        return "ccof_certified_organic_operators"

    def get_source_url(self) -> str:
        return f"{BASE}/resources/member-directory/"

    def get_niche_fields(self) -> List[str]:
        return [
            "contact_name",
            "certifications",
            "cert_status",
            "cert_date",
            "client_code",
            "chapter",
            "location_city",
            "location_state",
            "products_and_services",
            "operation_type",
        ]

    # ------------------------------------------------------------------ #
    #  crawl() - collect every /directory-member/ URL via WP XML sitemaps #
    # ------------------------------------------------------------------ #

    def crawl(self) -> Any:
        member_urls: List[str] = []
        seen: set = set()

        self.logger.info("Collecting member URLs from WordPress XML sitemaps ...")

        for idx in range(1, self.MAX_SITEMAP_IDX + 1):
            # Pattern confirmed from wp-sitemap.xml:
            #   idx=1 -> directory-member-sitemap.xml
            #   idx=2 -> directory-member-sitemap2.xml, etc.
            suffix = "" if idx == 1 else str(idx)
            sitemap_url = f"{SITEMAP_BASE}{suffix}.xml"
            resp = self._safe_request(sitemap_url)

            if resp is None or resp.status_code == 404:
                self.logger.info(f"  Sitemap {idx}: not found -- end of sitemaps.")
                break

            # Parse XML with BeautifulSoup
            try:
                soup = BeautifulSoup(resp.content, "lxml-xml")
            except Exception:
                soup = BeautifulSoup(resp.content, "html.parser")

            locs = [loc.get_text(strip=True) for loc in soup.find_all("loc")]
            member_locs = [u for u in locs if "/directory-member/" in u]

            new = 0
            for url in member_locs:
                if url not in seen:
                    seen.add(url)
                    member_urls.append(url)
                    new += 1

            self.logger.info(
                f"  Sitemap {idx}: {len(member_locs)} URLs, +{new} new "
                f"(total {len(member_urls)})"
            )

            if len(member_locs) == 0:
                self.logger.info(f"  Sitemap {idx}: empty -- stopping.")
                break

            self._rate_limit(self.REQUEST_DELAY)

        if not member_urls:
            self.logger.warning(
                "Sitemaps returned 0 URLs -- "
                "trying WP REST API fallback (per_page=100, paginated) ..."
            )
            member_urls = self._slug_fallback_via_rest()

        self.logger.info(f"Total member URLs collected: {len(member_urls)}")
        return member_urls

    # ------------------------------------------------------------------ #
    #  Fallback: WP REST API for custom post type 'directory-member'      #
    # ------------------------------------------------------------------ #

    def _slug_fallback_via_rest(self) -> List[str]:
        """
        Try known WP REST endpoint slugs for the directory-member CPT.
        """
        candidate_endpoints = [
            f"{BASE}/wp-json/wp/v2/directory-member",
            f"{BASE}/wp-json/wp/v2/directory_member",
            f"{BASE}/wp-json/ccof/v1/members",
            f"{BASE}/wp-json/ccof/v2/members",
        ]
        headers = {"Accept": "application/json"}
        for endpoint in candidate_endpoints:
            urls: List[str] = []
            for page in range(1, 200):
                resp = self._safe_request(
                    endpoint,
                    params={"per_page": 100, "page": page, "_fields": "link"},
                    headers=headers,
                )
                if resp is None or resp.status_code not in (200,):
                    break
                try:
                    batch = resp.json()
                except Exception:
                    break
                if not isinstance(batch, list) or not batch:
                    break
                for item in batch:
                    link = item.get("link", "")
                    if "/directory-member/" in link:
                        urls.append(link)
                total_pages = int(resp.headers.get("X-WP-TotalPages", page))
                if page >= total_pages:
                    break
                self._rate_limit(self.REQUEST_DELAY)
            if urls:
                self.logger.info(
                    f"REST fallback via {endpoint}: found {len(urls)} URLs."
                )
                return urls

        self.logger.error("All slug-collection methods exhausted -- 0 URLs found.")
        return []

    # ------------------------------------------------------------------ #
    #  parse() - concurrent fetch+parse using ThreadPoolExecutor          #
    # ------------------------------------------------------------------ #

    def parse(self, member_urls: List[str]) -> List[Dict]:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        records: List[Dict] = []
        total = len(member_urls)
        WORKERS = 5  # reduced to avoid ConnectionReset from server

        self.logger.info(f"  Parsing {total} pages with {WORKERS} threads ...")

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {executor.submit(self._parse_detail_page, url): url
                       for url in member_urls}
            done = 0
            for future in as_completed(futures):
                done += 1
                try:
                    rec = future.result()
                    if rec:
                        records.append(rec)
                except Exception as e:
                    self.logger.warning(f"  Error parsing {futures[future]}: {e}")
                if done % 500 == 0:
                    self.logger.info(f"  Parsed {done}/{total} pages ...")

        self.logger.info(f"  Done — {len(records)} records extracted.")
        return records

    # ------------------------------------------------------------------ #
    #  Detail page parser - field map confirmed from live pages           #
    # ------------------------------------------------------------------ #

    def _parse_detail_page(self, url: str) -> Dict | None:
        import time
        time.sleep(0.5)  # 0.5s per thread = ~10 req/s across 5 workers, avoids resets
        resp = self._safe_request(url)
        if resp is None:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        rec: Dict = {"profile_url": url}

        # -- company name ------------------------------------------------
        h1 = soup.find("h1")
        rec["company_name"] = h1.get_text(strip=True) if h1 else None
        if not rec["company_name"]:
            return None

        # -- Helper: extract text from <li> that contains a label --------
        # Confirmed page structure:
        #   <li><strong>Contact Name:</strong> Seena Chriti</li>
        #   <li><strong>Phone:</strong> 786-439-7120</li>
        #   <li><strong>Address:</strong> 11081 Labelle Ave ...</li>
        def li_after_label(label: str) -> str | None:
            label_l = label.lower().rstrip(":")
            for strong in soup.find_all(["strong", "b"]):
                if label_l in strong.get_text(strip=True).lower():
                    parent_li = strong.find_parent("li") or strong.find_parent("div")
                    if parent_li:
                        full = parent_li.get_text(" ", strip=True)
                        after = re.sub(
                            rf"^{re.escape(strong.get_text(strip=True))}\s*:?\s*",
                            "",
                            full,
                            flags=re.I,
                        ).strip()
                        return after or None
            return None

        # -- contact fields ----------------------------------------------
        rec["contact_name"] = li_after_label("Contact Name")
        rec["phone"]        = li_after_label("Phone")

        mailto = soup.find("a", href=re.compile(r"^mailto:", re.I))
        rec["email"] = (
            mailto["href"].replace("mailto:", "").strip() if mailto else None
        )

        # website: <a> inside the Website <li>
        website_li = None
        for strong in soup.find_all(["strong", "b"]):
            if "website" in strong.get_text(strip=True).lower():
                website_li = strong.find_parent("li")
                break
        if website_li:
            ext = website_li.find("a", href=re.compile(r"^https?://", re.I))
            rec["website"] = ext["href"] if ext else None
        else:
            ext = soup.find(
                "a",
                href=re.compile(r"^https?://(?!(?:www\.)?ccof\.org)", re.I),
            )
            rec["website"] = ext["href"] if ext else None

        # -- address -----------------------------------------------------
        rec["address"] = li_after_label("Address")

        # -- certifications block ----------------------------------------
        # Each cert is a <li> with text like:
        #   "Certification: Handling  Status: Certified  Date: July 8, 2024"
        cert_names, cert_statuses, cert_dates = [], [], []
        for li in soup.find_all("li"):
            text = li.get_text(" ", strip=True)
            if "Certification:" not in text:
                continue
            m_name   = re.search(r"Certification:\s*(.+?)\s+Status:", text)
            m_status = re.search(r"Status:\s*(\w+)", text)
            m_date   = re.search(r"Date:\s*(.+)", text)
            if m_name:
                cert_names.append(m_name.group(1).strip())
            if m_status:
                cert_statuses.append(m_status.group(1).strip())
            if m_date:
                cert_dates.append(m_date.group(1).strip())

        rec["certifications"] = " | ".join(cert_names)    or None
        rec["cert_status"]    = " | ".join(cert_statuses) or None
        rec["cert_date"]      = " | ".join(cert_dates)    or None

        # -- client code -------------------------------------------------
        # Rendered as:  ## Client Code  then <strong>pr3448</strong>
        # or as a <p> / standalone text
        client_heading = soup.find(
            string=re.compile(r"Client Code", re.I)
        )
        if client_heading:
            parent = client_heading.find_parent()
            nxt = parent.find_next_sibling() if parent else None
            if nxt:
                code_text = nxt.get_text(strip=True)
                rec["client_code"] = code_text if re.match(r"[a-z]{2}\d{3,5}", code_text) else None
            else:
                m = re.search(r"\b([a-z]{2}\d{3,5})\b", soup.get_text())
                rec["client_code"] = m.group(1) if m else None
        else:
            m = re.search(r"\b([a-z]{2}\d{3,5})\b", soup.get_text())
            rec["client_code"] = m.group(1) if m else None

        # -- member overview lists ---------------------------------------
        # Structure (confirmed from live pages):
        #   ## Member Overview
        #   Chapter:   <li>Processor/Handler Chapter (PR)</li>
        #   Location:  <li>Blue Ash</li> <li>Ohio</li>
        #   Product And Service: <li>Ingredient</li> <li>Snack Foods</li>
        overview_section = None
        for heading in soup.find_all(["h2", "h3"]):
            if "member overview" in heading.get_text(strip=True).lower():
                # Grab everything after this heading
                overview_section = heading.find_next_sibling()
                break

        def overview_items(label: str) -> List[str]:
            """Return all <li> texts under a label inside Member Overview."""
            search_root = overview_section if overview_section else soup
            label_l = label.lower()
            for node in search_root.find_all(
                string=re.compile(label_l, re.I)
            ):
                parent = node.find_parent()
                if parent:
                    ul = parent.find_next_sibling("ul") or parent.find("ul")
                    if ul:
                        return [li.get_text(strip=True) for li in ul.find_all("li")]
                    # Fallback: next <li> siblings
                    sibs = []
                    for sib in parent.find_next_siblings("li"):
                        t = sib.get_text(strip=True)
                        if t:
                            sibs.append(t)
                        else:
                            break
                    if sibs:
                        return sibs
            return []

        chapters  = overview_items("Chapter")
        locations = overview_items("Location")
        products  = overview_items("Product")

        rec["chapter"]               = " | ".join(chapters) or None
        rec["products_and_services"] = " | ".join(products) or None
        rec["location_city"]         = locations[0] if len(locations) > 0 else None
        rec["location_state"]        = locations[1] if len(locations) > 1 else None

        # -- operation type (derived from certifications) ----------------
        op_types = []
        for op in ["Crops", "Handling", "Livestock", "Wild Crop", "Retailer"]:
            if op.lower() in (rec.get("certifications") or "").lower():
                op_types.append(op)
        rec["operation_type"] = " / ".join(op_types) or None

        return rec


# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    crawler = CCOFCrawler()
    stats = crawler.run()
    print("\n=== STATS ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    csv_path = crawler.export_csv()
    if csv_path:
        print(f"\nCSV -> {csv_path}")