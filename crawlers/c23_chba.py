"""
Crawler 23 — CHBA Canadian Home Builders' Association (Canada)
Source   : https://hub.chba.ca/member-directory
Records  : ~8,432 | Canada

Fetch    : Higher Logic membership platform — FindStartsWith endpoint
           returns members by first letter. Loops A-Z then 0-9 (36
           total requests) for complete member coverage.

Parse    : BeautifulSoup parses HTML member cards. Extracts company name,
           city, province, website, phone from each card element.

           A-Z + 0-9 sweep on Higher Logic is reusable across hundreds
           of associations worldwide that run on this platform.
"""

import sys, os, re, string
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crawlers.base_crawler import BaseCrawler

# Loop A–Z then 0–9 to cover all member names
LETTERS = list(string.ascii_uppercase) + list(string.digits)

# Valid Canadian province / territory codes
VALID_CA_PROVINCES = {
    'AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT'
}

# Recognised member type labels (as they appear on cards)
KNOWN_MEMBER_TYPES = {
    'Builder', 'Renovator', 'Developer', 'Supplier',
    'Trade Contractor', 'Associate', 'Net Zero',
    # 'RenoMark' is a certification flag, not a type — handled separately
}

# Street address pattern — used to distinguish address from city
_STREET_RE = re.compile(
    r'\d.*\b(?:Street|St|Avenue|Ave|Drive|Dr|Road|Rd|Way|Boulevard|Blvd|'
    r'Lane|Ln|Court|Ct|Place|Pl|Crescent|Cres|Highway|Hwy|Route|Rte|'
    r'Sumas|Castleton|Magenta|Hunt|Lionel|Dundas|Woodworth|Coverdale)\b',
    re.IGNORECASE,
)

# Province code pattern
_PROV_RE  = re.compile(r'^[A-Z]{2}$')
# Phone pattern
_PHONE_RE = re.compile(r'^\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4}')


class ChbaCrawler(BaseCrawler):

    BASE_URL   = "https://hub.chba.ca"
    LIST_URL   = "https://hub.chba.ca/member-directory/FindStartsWith?term={letter}"

    def get_dataset_name(self): return "chba_home_builders"
    def get_source_url(self):   return f"{self.BASE_URL}/member-directory"
    def get_niche_fields(self):
        return [
            "member_type",
            "net_zero_qualified",
            "renomark",
            "province",
            "local_hba",
            "member_url",
        ]

    # ------------------------------------------------------------------ #
    def _make_session(self):
        """Create HTTP session — prefer curl_cffi for better TLS fingerprint."""
        try:
            from curl_cffi import requests as cffi_requests
            session = cffi_requests.Session(impersonate="chrome124")
            self.logger.info("Using curl_cffi (chrome124 impersonation)")
        except ImportError:
            import requests
            session = requests.Session()
            self.logger.info("Using requests (no curl_cffi)")

        session.headers.update({
            "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer":         f"{self.BASE_URL}/member-directory",
        })
        return session

    # ------------------------------------------------------------------ #
    def _fetch_letter(self, session, letter):
        """
        Fetch /FindStartsWith?term={letter} and parse all member cards.

        Card HTML structure (Higher Logic gz-directory-card):
          Line 0 : company name
          Line 1 : street address  OR  company name repeated (some cards)
          Line 2 : city            OR  street address (shifted)
          Line 3 : province code
          Line 4 : phone (optional)
          Line N : member type / RenoMark / Net Zero (optional)

        We parse defensively without assuming fixed positions.
        """
        from bs4 import BeautifulSoup

        url = self.LIST_URL.format(letter=letter)
        try:
            r = session.get(url, timeout=30)
            if not r.ok:
                self.logger.warning(f"[{letter}] HTTP {r.status_code}")
                return []
        except Exception as e:
            self.logger.warning(f"[{letter}] request error: {e}")
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.find_all("div", class_="gz-directory-card")
        self.logger.debug(f"[{letter}] {len(cards)} cards found")

        members = []
        for card in cards:
            member = self._parse_card(card)
            if member:
                members.append(member)

        return members

    # ------------------------------------------------------------------ #
    def _parse_card(self, card):
        """
        Parse a single gz-directory-card div into a structured dict.
        Returns None if no company name can be extracted.
        """
        # Split card text into non-empty lines, stripping nav labels
        _SKIP = {"Visit Website", "More Details"}
        lines = [
            l.strip()
            for l in card.get_text(separator="\n").split("\n")
            if l.strip() and l.strip() not in _SKIP and l.strip() != ","
        ]
        if not lines:
            return None

        name = lines[0]
        if not name:
            return None

        # ── Detail / member URL ────────────────────────────────────────
        link_el = card.find("a", href=re.compile(r"/member-directory/Details/"))
        member_url = ""
        slug = ""
        if link_el:
            href = link_el.get("href", "")
            member_url = ("https:" + href) if href.startswith("//") else href
            slug_m = re.search(r"/Details/([^/?#]+)", href)
            slug = slug_m.group(1) if slug_m else ""

        # ── Parse remaining lines ──────────────────────────────────────
        street      = ""
        city        = ""
        province    = ""
        phone       = ""
        member_type = ""
        local_hba   = ""
        net_zero    = False
        renomark    = False

        # Collect flags from ALL lines first (position-independent)
        all_text = " ".join(lines).lower()
        if "net zero" in all_text:
            net_zero = True
        if "renomark" in all_text:
            renomark = True

        # Parse address fields from lines[1:] positionally but defensively
        remaining = lines[1:]

        # Skip if first remaining line duplicates the company name
        if remaining and remaining[0].strip().lower() == name.strip().lower():
            remaining = remaining[1:]

        for line in remaining:
            # Province code — exactly 2 uppercase letters
            if _PROV_RE.match(line) and line in VALID_CA_PROVINCES:
                province = line
                continue

            # Phone number
            if _PHONE_RE.match(line):
                if not phone:
                    phone = line
                continue

            # Member type labels
            if line in KNOWN_MEMBER_TYPES or line == "RenoMark":
                if line == "RenoMark":
                    renomark = True
                    # member_type stays empty or keeps a real type if already set
                elif line == "Net Zero":
                    net_zero = True
                else:
                    if not member_type:
                        member_type = line
                continue

            # Local HBA — typically "XX HBA" or "XX Home Builders' Association"
            if re.search(r'\bHBA\b|Home Builders', line, re.I):
                local_hba = line
                continue

            # Address / city discrimination
            if not street and _STREET_RE.search(line):
                street = line
            elif not city and not line.startswith("(") and len(line) > 1:
                city = line

        return {
            "name":       name,
            "slug":       slug,
            "street":     street,
            "city":       city,
            "province":   province,
            "phone":      phone,
            "member_type": member_type,
            "local_hba":  local_hba,
            "net_zero":   net_zero,
            "renomark":   renomark,
            "member_url": member_url,
        }

    # ------------------------------------------------------------------ #
    def crawl(self):
        try:
            from bs4 import BeautifulSoup  # noqa — just verify install
        except ImportError:
            self.logger.error("Install beautifulsoup4: pip install beautifulsoup4")
            return []

        session  = self._make_session()
        all_members = []
        seen_slugs  = set()

        for letter in LETTERS:
            self.logger.info(f"[{letter}] fetching...")
            members = self._fetch_letter(session, letter)

            added = 0
            for m in members:
                uid = (m.get("slug") or m.get("name", "")).lower().strip()
                if uid and uid not in seen_slugs:
                    seen_slugs.add(uid)
                    all_members.append(m)
                    added += 1

            self.logger.info(f"  +{added} new  (running total: {len(all_members)})")
            self._rate_limit(0.8)

        self.logger.info(f"Crawl complete — {len(all_members)} unique members")
        return all_members

    # ------------------------------------------------------------------ #
    def parse(self, raw_data):
        if not raw_data:
            return []

        records   = []
        skipped   = 0

        for item in raw_data:
            if not isinstance(item, dict):
                continue

            name = item.get("name", "").strip()
            if not name:
                continue

            province = item.get("province", "").strip()

            # ── Drop records with invalid / non-Canadian province codes ──
            if province and province not in VALID_CA_PROVINCES:
                self.logger.debug(f"Skipping '{name}' — invalid province '{province}'")
                skipped += 1
                continue

            street   = item.get("street", "").strip()
            city     = item.get("city", "").strip()
            phone    = item.get("phone", "").strip()

            # ── Normalise member_type ──────────────────────────────────
            raw_type    = item.get("member_type", "").strip()
            member_type = raw_type if raw_type in KNOWN_MEMBER_TYPES else ""

            # ── Net Zero & RenoMark flags ──────────────────────────────
            net_zero = bool(item.get("net_zero"))
            renomark = bool(item.get("renomark"))

            # If member_type was 'Net Zero' at crawl time, convert to flag
            if raw_type == "Net Zero":
                net_zero    = True
                member_type = ""

            records.append({
                "company_name":       name,
                "address":            street,
                "city":               city,
                "state":              province,
                "zip_code":           "",
                "country":            "Canada",
                "phone":              phone,
                "email":              "",
                "website":            "",
                # ── Niche fields ───────────────────────────────────────
                "member_type":        member_type,
                "net_zero_qualified": str(net_zero),
                "renomark":           str(renomark),
                "province":           province,
                "local_hba":          item.get("local_hba", "").strip(),
                "member_url":         item.get("member_url", "").strip(),
            })

        if skipped:
            self.logger.warning(f"Dropped {skipped} records with invalid province codes")

        self.logger.info(f"Parsed {len(records)} valid CHBA records")
        return records


# ── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)

    # Auto-install beautifulsoup4 if missing
    try:
        import bs4
    except ImportError:
        print("Installing beautifulsoup4...")
        import subprocess
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "beautifulsoup4", "--quiet"]
        )

    crawler = ChbaCrawler()
    stats   = crawler.run()

    print(f"\n{'='*45}")
    print(f"  New records  : {stats['total_new']:,}")
    print(f"  Updated      : {stats['total_updated']:,}")
    print(f"  Duplicates   : {stats['total_duplicates']:,}")
    print(f"  Errors       : {stats['total_errors']:,}")
    print(f"{'='*45}")

    if stats["total_new"] + stats["total_updated"] > 0:
        path = crawler.export_csv()
        if path:
            print(f"  CSV saved    : {path}")
    else:
        print(
            "\n  0 records collected.\n"
            "  Open DevTools on hub.chba.ca/member-directory\n"
            "  → Network tab → type a letter → inspect the XHR request\n"
            "  that returns member cards and confirm the URL pattern."
        )