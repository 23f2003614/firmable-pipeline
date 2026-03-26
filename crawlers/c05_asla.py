"""
Crawler #5 — ASLA FirmFinder (American Society of Landscape Architects)
Source  : https://connect.asla.org/search?ListingType=FirmFinder


"""

import sys, os, re, time, string
from typing import Any, List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from base_crawler import BaseCrawler

API_URL    = "https://connect.asla.org/AdvancedSearch/SearchCompanyCard"
BASE_URL   = "https://connect.asla.org"
SEARCH_URL = (
    "https://connect.asla.org/search?FreeTextSearch=&SubCategoryIds="
    "&SortBy=Featured&View=Card&ContentType=All&ListingType=FirmFinder"
    "&ListingTypeId=e74b1462-1188-4954-8af1-99eba28e0ac7"
    "&DemographicsSubCategoryId=&MemberPerks=false&ReferFriendCampaign=false"
    "&Latitude=0&Longitude=0&Distance=100&PlaceName=&LocationCountry="
    "&OpenStore=false&IsStandAlone=false&DistanceSearchHQ=true"
    "&DistanceSearchBranches=true&isMapViewToggleSwitchEnabled=false"
    "&AutomationListId="
)
LISTING_TYPE_ID = "e74b1462-1188-4954-8af1-99eba28e0ac7"
ENRICH_LISTINGS = True
ENRICH_WORKERS  = 8
SWEEP_TERMS     = list(string.ascii_uppercase) + list("0123456789")

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
       "AppleWebKit/537.36 (KHTML, like Gecko) "
       "Chrome/124.0.0.0 Safari/537.36")

# UI strings — never valid data values
_UI_STRINGS = {
    "view email address", "send message", "call number", "visit website",
    "claim listing", "report page", "follow", "following", "message",
    "edit profile", "add image caption", "share via", "report a problem",
    "create a post", "invite supplier", "plan details", "view all",
    "possible duplicates", "update company details", "invoice",
    "video recommendation", "log in", "save", "close", "reset",
    "overview", "contacts", "locations", "firmfinder", "all users",
    "suppliers", "product directory", "add to lists",  # ← NEW
}

# Staff name tokens to strip (UI labels, location markers)
_STAFF_STRIP = re.compile(
    r"\b(add to lists?|location|office|branch|headquarters|hq)\b",
    re.I
)

# Nav practice-type labels to skip
_NAV_PRACTICE = {
    "firmfinder", "product directory", "browse",
    "find firms", "find products", "asla",
}


def _make_session(extra_headers=None):
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    retry = Retry(
        total=3, backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"], raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=8)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    s.headers.update({"User-Agent": _UA, "Accept": "*/*"})
    if extra_headers:
        s.headers.update(extra_headers)
    return s


def _is_real_tagline(txt: str) -> bool:
    """
    Return True only if txt looks like a genuine marketing tagline.
    Rejects: company names, short proper nouns, UI labels, pure nouns.
    Accepts: sentences with action verbs and > 4 words.
    """
    if not txt or len(txt) < 15:
        return False
    if txt.lower() in _UI_STRINGS:
        return False
    words = txt.split()
    if len(words) < 5:
        return False
    # Must contain at least one verb-like word (ends in -ing, -ed, -s common verb)
    # or a connecting word typical of taglines
    verb_pat = re.compile(
        r"\b(transform|craft|creat|design|build|deliver|elevat|innovate|"
        r"pioneer|provid|shap|develop|inspect|serv|lead|bring|connect|"
        r"ing|ment)\b",
        re.I
    )
    return bool(verb_pat.search(txt))


class ASLACrawler(BaseCrawler):

    def get_dataset_name(self) -> str:
        return "asla_firmfinder"

    def get_source_url(self) -> str:
        return "https://connect.asla.org/search?ListingType=FirmFinder"

    def get_niche_fields(self) -> List[str]:
        return [
            "tagline", "practice_type", "specialisations",
            "hq_address", "city", "state", "zip_code",
            "lat", "lng", "contact_count", "staff_names",
            "firm_category", "description", "listing_url", "listing_slug",
        ]

    # ──────────────────────────────────────────────────────────────────────────
    def crawl(self) -> Any:
        all_html = []
        LIMIT    = 200

        for term in SWEEP_TERMS:
            session = _make_session({
                "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Referer":          SEARCH_URL,
            })
            try:
                seed = session.get(SEARCH_URL, timeout=30)
                self.logger.info(
                    f"[{term}] Session seeded HTTP {seed.status_code} | "
                    f"cookies: {list(session.cookies.keys())}"
                )
            except Exception as e:
                self.logger.warning(f"[{term}] Seed failed: {e}")
                continue

            offset         = 1
            letter_total   = 9999
            page_in_letter = 1

            while offset <= letter_total:
                payload = {
                    "FreeTextSearch": term,
                    "offset": str(offset), "limit": str(LIMIT),
                    "total":  str(letter_total) if letter_total < 9999 else "",
                    "ContentType": "All", "CategoryContainerId[]": "",
                    "SortBy": "A-Z", "ResourceType": "",
                    "MemberPerks": "false",
                    "isFeaturedCompanyExistsForCard": "false",
                    "isFeaturedCompanyExistsForList": "false",
                    "SelectedIds": "", "IsRecommendedOnly": "false",
                    "IsFeaturedOnly": "false", "IsLikedOnly": "false",
                    "ReferFriendCampaign": "false",
                    "ListingType": "FirmFinder",
                    "DemographicsCategoryContainerId[]": "",
                    "ListingTypeId": LISTING_TYPE_ID,
                    "AutomationListId": "",
                    "Latitude": "0", "Longitude": "0", "Distance": "100",
                    "PlaceName": "", "LocationCountry": "", "OpenStore": "false",
                    "WorkingDay": "Wed", "CurrentTimeInformation": "09.00",
                    "TotalShownCount": str(offset - 1),
                    "rating": "", "minNps": "-100", "maxNps": "100",
                    "recommendations": "0", "ratings": "0",
                    "SearchLimit": str(LIMIT), "SetMaxResult": "0",
                    "isStandAlone": "false", "isShowAds": "false",
                    "View": "Card", "DistanceSearchHQ": "true",
                    "DistanceSearchBranches": "true",
                    "IncludeNonHQLocationsInDistanceSearch": "true",
                    "ShowFilterForLocationTypes": "true", "MemberType": "All",
                }
                try:
                    resp = session.post(API_URL, data=payload, timeout=45)
                    resp.raise_for_status()
                    html_chunk = resp.text
                except Exception as e:
                    self.logger.error(f"[{term}] p{page_in_letter} failed: {e}")
                    break

                soup  = BeautifulSoup(html_chunk, "html.parser")
                cards = self._find_cards(soup)
                n     = len(cards)

                if page_in_letter == 1:
                    t_tag = soup.find("input", {"id": "tempTotal"})
                    if t_tag and t_tag.get("value", "").isdigit():
                        v = int(t_tag["value"])
                        letter_total = v if v > 0 else 0
                    else:
                        letter_total = 0
                    self.logger.info(f"[{term}] total={letter_total} | p1 cards={n}")
                else:
                    self.logger.info(f"[{term}] p{page_in_letter} offset={offset} cards={n}")

                if letter_total == 0 or n == 0:
                    break

                all_html.append(html_chunk)
                offset         += LIMIT
                page_in_letter += 1
                self._rate_limit(1.0)

            self._rate_limit(1.0)

        self.logger.info(f"Total HTML chunks collected: {len(all_html)}")
        return all_html

    # ──────────────────────────────────────────────────────────────────────────
    def _find_cards(self, soup):
        cards = soup.find_all("div", class_="companyData")
        if cards:
            return cards
        uuid_pat = re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            re.I
        )
        cards = [d for d in soup.find_all("div", id=uuid_pat) if d.find("h3")]
        if cards:
            return cards
        cards = soup.find_all("div", class_=re.compile(r"company", re.I))
        if cards:
            return cards
        cards = [d for d in soup.find_all("div", class_=re.compile(r"\bcard\b", re.I))
                 if d.find("h3")]
        if cards:
            return cards
        seen, result = set(), []
        for tag in soup.find_all(attrs={"onclick": re.compile(r"/listing/")}):
            parent = tag.find_parent("div") or tag
            if id(parent) not in seen and parent.find("h3"):
                seen.add(id(parent))
                result.append(parent)
        return result

    # ──────────────────────────────────────────────────────────────────────────
    def parse(self, raw_data: Any) -> List[Dict]:
        basic_records = []
        for chunk in raw_data:
            soup = BeautifulSoup(chunk, "html.parser")
            for card in self._find_cards(soup):
                rec = self._extract_card(card)
                if rec.get("company_name"):
                    basic_records.append(rec)

        self.logger.info(f"Cards extracted: {len(basic_records)}")
        if not ENRICH_LISTINGS:
            return basic_records

        self.logger.info(f"Starting parallel enrichment ({ENRICH_WORKERS} workers)...")
        total     = len(basic_records)
        enriched  = [None] * total
        completed = [0]

        def enrich_one(idx_rec):
            idx, rec = idx_rec
            if not rec.get("listing_url"):
                return idx, rec
            session = _make_session({"Accept": "text/html,*/*"})
            for attempt in range(3):
                try:
                    r = session.get(rec["listing_url"], timeout=25)
                    if r.ok:
                        extra = self._parse_listing_page(
                            BeautifulSoup(r.text, "html.parser")
                        )
                        rec.update({k: v for k, v in extra.items() if v})
                    break
                except Exception as e:
                    if attempt < 2:
                        time.sleep(1.5 ** attempt)
                    else:
                        self.logger.warning(
                            f"Enrich failed {rec['listing_url']}: {e}"
                        )
            return idx, rec

        with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as pool:
            futures = {
                pool.submit(enrich_one, (i, rec)): i
                for i, rec in enumerate(basic_records)
            }
            for future in as_completed(futures):
                idx, rec = future.result()
                enriched[idx] = rec
                completed[0] += 1
                if completed[0] % 200 == 0:
                    self.logger.info(
                        f"  Enriched {completed[0]}/{total} "
                        f"({completed[0]*100//total}%)"
                    )

        self.logger.info(f"Enrichment done — {len(enriched)} records")
        return [r for r in enriched if r is not None]

    # ──────────────────────────────────────────────────────────────────────────
    def _extract_card(self, card) -> Dict:
        h3 = card.find("h3")
        if not h3:
            return {}
        name = h3.get("title") or h3.get_text(strip=True)
        name = name.replace("...", "").strip()
        if not name or len(name) < 2:
            return {}

        listing_slug = listing_url = None
        for tag in card.find_all(attrs={"onclick": True}):
            m = re.search(r"'/listing/([^']+)'", tag.get("onclick", ""))
            if m:
                listing_slug = m.group(1)
                listing_url  = f"{BASE_URL}/listing/{listing_slug}"
                break
        if not listing_url:
            for a in card.find_all("a", href=re.compile(r"/listing/")):
                m = re.match(r"/listing/(.+)", a["href"])
                if m:
                    listing_slug = m.group(1).rstrip("/")
                    listing_url  = f"{BASE_URL}/listing/{listing_slug}"
                    break

        desc_container = card.find("div", class_="card-all-view-info-container")
        description    = None
        if desc_container:
            p = desc_container.find("p")
            if p:
                description = re.sub(r"\s*\.\.\.\s*$", "",
                                     p.get_text(strip=True)).strip()
        if not description:
            p = card.find("p")
            if p:
                description = p.get_text(strip=True)[:300]

        badge_img     = card.find("img", alt=re.compile(r"Badge", re.I))
        firm_category = None
        if badge_img:
            alt = badge_img.get("alt", "")
            firm_category = alt.replace(" Badge Icon", "").replace(" Badge", "").strip()

        return {
            "company_name":    name,
            "website":         None, "phone": None, "email": None,
            "hq_address":      None, "city":  None, "state": None,
            "zip_code":        None, "lat":   None, "lng":   None,
            "tagline":         None, "practice_type":  None,
            "specialisations": None, "contact_count":  None,
            "staff_names":     None, "description":    description,
            "firm_category":   firm_category,
            "listing_url":     listing_url,
            "listing_slug":    listing_slug,
        }

    # ──────────────────────────────────────────────────────────────────────────
    def _parse_listing_page(self, soup) -> Dict:
        result = {
            "website": None, "phone": None, "email": None,
            "hq_address": None, "city": None, "state": None, "zip_code": None,
            "lat": None, "lng": None,
            "tagline": None, "practice_type": None, "specialisations": None,
            "contact_count": None, "staff_names": None,
        }

        SKIP_DOMAINS = {
            "connect.asla.org", "asla.org", "google.com",
            "fontawesome", "amazonaws.com", "insightguide",
        }

        # Website
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and not any(d in href for d in SKIP_DOMAINS):
                result["website"] = href
                break

        # Phone
        tel_a = soup.find("a", href=re.compile(r"^tel:"))
        if tel_a:
            result["phone"] = tel_a["href"].replace("tel:", "").strip()
        else:
            text = soup.get_text(separator="\n", strip=True)
            pm = re.search(r"(\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4})", text)
            if pm:
                result["phone"] = pm.group(1)

        # Email
        mailto_a = soup.find("a", href=re.compile(r"^mailto:"))
        if mailto_a:
            email = mailto_a["href"].replace("mailto:", "").strip()
            if not any(x in email.lower() for x in ["noreply", "asla.org", "example"]):
                result["email"] = email
        else:
            text = soup.get_text(separator="\n", strip=True)
            em = re.search(
                r"\b([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})\b", text
            )
            if em:
                e = em.group(1)
                if not any(x in e.lower() for x in
                           ["noreply", "asla.org", "example", "insightguide"]):
                    result["email"] = e

        # HQ Address — strip "X Contacts Headquarters" prefix
        hq_addr = None
        for el in soup.find_all(string=re.compile(r"Headquarters", re.I)):
            parent = el.find_parent()
            if not parent:
                continue
            nxt = parent.find_next_sibling()
            if nxt:
                candidate = nxt.get_text(strip=True)
                if re.search(r"[A-Z]{2}\s+\d{5}", candidate):
                    hq_addr = candidate
                    break
            full = re.sub(r"^\d+\s+Contacts?\s+Headquarters\s*", "",
                          parent.get_text(separator=" ", strip=True), flags=re.I)
            am = re.search(
                r"(\d[\w\s\.,#\-]{5,60}[A-Z]{2}\s+\d{5}(?:-\d{4})?(?:,\s*USA)?)",
                full
            )
            if am:
                hq_addr = am.group(1).strip()
                break

        if not hq_addr:
            text = re.sub(r"\d+\s+Contacts?\s+Headquarters\s*", "",
                          soup.get_text(separator="\n", strip=True), flags=re.I)
            am = re.search(
                r"(\d[\w\s\.,#\-]{5,60}[A-Z]{2}\s+\d{5}(?:-\d{4})?(?:,\s*USA)?)",
                text
            )
            if am:
                hq_addr = am.group(1).strip()

        if hq_addr:
            result["hq_address"] = hq_addr
            csz = re.search(
                r"([A-Za-z][A-Za-z\s\.]{1,28}),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)",
                hq_addr
            )
            if csz:
                result["city"]     = csz.group(1).strip()
                result["state"]    = csz.group(2)
                result["zip_code"] = csz.group(3)

        # Lat/Lng
        map_img = soup.find("img", src=re.compile(r"StaticMap/Image", re.I))
        if map_img:
            src   = map_img.get("src", "")
            lat_m = re.search(r"lat=([\-\d\.]+)", src)
            lng_m = re.search(r"lng=([\-\d\.]+)", src)
            if lat_m: result["lat"] = lat_m.group(1)
            if lng_m: result["lng"] = lng_m.group(1)

        # Tagline — must be a real sentence, not company name or UI label
        for tag in soup.find_all(["h2", "h3"]):
            txt = tag.get_text(strip=True)
            if _is_real_tagline(txt):
                result["tagline"] = txt
                break

        # Practice type — skip nav labels
        for a in soup.find_all("a", href=re.compile(r"browsecategories", re.I)):
            txt = a.get_text(strip=True)
            if txt and txt.lower() not in _NAV_PRACTICE and len(txt) > 3:
                result["practice_type"] = txt
                break

        # Specialisations
        specs = []
        for a in soup.find_all(
            "a", href=re.compile(r"SearchBySubCategoryFromListingPage", re.I)
        ):
            txt = a.get_text(strip=True).rstrip(",").strip()
            if txt and txt not in specs:
                specs.append(txt)
        result["specialisations"] = ", ".join(specs) if specs else None

        # Contact count
        for a in soup.find_all("a", href=re.compile(r"/contacts", re.I)):
            m = re.search(r"(\d+)\s+contact", a.get_text(strip=True), re.I)
            if m:
                result["contact_count"] = m.group(1)
                break

        # Staff names — filter out UI tokens and location labels
        staff      = []
        seen_names = set()
        for heading in soup.find_all(["h2", "h3"]):
            txt = heading.get_text(strip=True)
            # Skip known UI strings
            if txt.lower() in _UI_STRINGS:
                continue
            # Skip if it matches UI strip pattern
            if _STAFF_STRIP.search(txt):
                continue
            # Must look like a person name: 2-4 words, title-case, no digits
            words = txt.split()
            if (2 <= len(words) <= 4 and
                    not re.search(r"\d", txt) and
                    txt[0].isupper() and
                    len(txt) < 40 and
                    txt not in seen_names and
                    # All words capitalised (person name pattern)
                    all(w[0].isupper() for w in words if w)):
                parent = heading.find_parent(["a", "div"])
                if parent:
                    seen_names.add(txt)
                    staff.append(txt)
                    if len(staff) >= 5:
                        break

        result["staff_names"] = "; ".join(staff) if staff else None
        return result


# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    crawler = ASLACrawler()
    stats   = crawler.run()
    print("\n=== ASLA FirmFinder Crawl Complete ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    csv_path = crawler.export_csv()
    if csv_path:
        print(f"  CSV exported to: {csv_path}")