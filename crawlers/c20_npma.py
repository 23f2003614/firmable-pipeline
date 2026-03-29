"""
Crawler 20 — NPMA Pest Management Member Directory
Source   : https://www.pestworld.org/find-local-exterminators/
Records  : ~1,210 | US

Fetch    : Playwright (headless Chromium) — site is fully JS-rendered,
           regular requests return no data. Sweeps ~800 zip codes with
           network-idle wait after each search.

Parse    : BeautifulSoup parses rendered HTML. Extracts company name,
           address, phone, website. Maps full state names to abbreviations.

           Playwright's network-idle wait ensures the page has truly
           finished all async calls — more reliable than fixed sleep timers.
"""

import sys, os, re, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from crawlers.base_crawler import BaseCrawler

STATE_NAME_TO_ABBR = {
    "alabama":"AL","alaska":"AK","arizona":"AZ","arkansas":"AR",
    "california":"CA","colorado":"CO","connecticut":"CT","delaware":"DE",
    "florida":"FL","georgia":"GA","hawaii":"HI","idaho":"ID",
    "illinois":"IL","indiana":"IN","iowa":"IA","kansas":"KS",
    "kentucky":"KY","louisiana":"LA","maine":"ME","maryland":"MD",
    "massachusetts":"MA","michigan":"MI","minnesota":"MN","mississippi":"MS",
    "missouri":"MO","montana":"MT","nebraska":"NE","nevada":"NV",
    "new hampshire":"NH","new jersey":"NJ","new mexico":"NM","new york":"NY",
    "north carolina":"NC","north dakota":"ND","ohio":"OH","oklahoma":"OK",
    "oregon":"OR","pennsylvania":"PA","rhode island":"RI",
    "south carolina":"SC","south dakota":"SD","tennessee":"TN","texas":"TX",
    "utah":"UT","vermont":"VT","virginia":"VA","washington":"WA",
    "west virginia":"WV","wisconsin":"WI","wyoming":"WY","dc":"DC",
    "district of columbia":"DC",
}

STATE_ZIPS = [
    ("AL",["35203","35401","35601","35801","36101","36201","36301","36401","36601","36701","36801","35004","35007","35501"]),
    ("AK",["99501","99701","99901","99603","99611","99654"]),
    ("AZ",["85001","85201","85301","85501","85601","85701","85901","86001","86301","86401","85233","85281","85345"]),
    ("AR",["72201","72401","71601","72701","71901","72032","72601","71730","72301","71801","72501","72901"]),
    ("CA",["90001","90401","90801","91101","91401","91901","92101","92401","92601","92801","93001","93301","93601","93901","94101","94401","94601","94901","95101","95401","95601","95901","96001"]),
    ("CO",["80201","80401","80501","80601","80801","80901","81001","81201","81301","81501","81601"]),
    ("CT",["06101","06320","06401","06510","06601","06702","06801","06902","06070","06405"]),
    ("DE",["19701","19702","19801","19901","19930","19963"]),
    ("FL",["32004","32099","32201","32301","32401","32501","32601","32801","32901","33101","33301","33401","33601","33801","33901","34101","34201","34401","34601","34801","34901"]),
    ("GA",["30101","30201","30301","30401","30501","30601","30701","30801","30901","31101","31201","31401","31601","31701","31901"]),
    ("HI",["96701","96720","96740","96766","96793","96801","96813"]),
    ("ID",["83201","83301","83401","83501","83601","83701","83801","83605","83642"]),
    ("IL",["60101","60301","60601","60801","61001","61201","61401","61601","61801","62001","62201","62401","62701","62901"]),
    ("IN",["46101","46201","46401","46601","46801","47001","47201","47401","47601","47801","47901"]),
    ("IA",["50101","50301","50501","51101","51301","51501","52101","52301","52401","52601","52801"]),
    ("KS",["66101","66401","66601","66801","67001","67201","67401","67601","67801","67901"]),
    ("KY",["40101","40201","40401","40601","40801","41101","41301","41501","41701","42001","42201","42401"]),
    ("LA",["70112","70301","70501","70701","70801","71001","71101","71301","71501","71601"]),
    ("ME",["04001","04101","04201","04401","04605","04730","04901"]),
    ("MD",["20601","20781","20850","21001","21201","21401","21601","21701","21801","21901"]),
    ("MA",["01101","01201","01420","01601","01801","01901","02101","02301","02601","02701","02901"]),
    ("MI",["48101","48201","48401","48601","48801","49001","49201","49401","49601","49801","49901"]),
    ("MN",["55101","55401","55601","55801","56001","56201","56401","56601","56801"]),
    ("MS",["38601","38801","39001","39201","39401","39601","39701","39901"]),
    ("MO",["63101","63301","63501","63701","63901","64401","64601","65101","65301","65601","65801"]),
    ("MT",["59101","59201","59301","59401","59601","59701","59801","59901"]),
    ("NE",["68101","68301","68501","68701","68901","69001","69201","69401"]),
    ("NV",["89101","89201","89301","89501","89701","89801","89901"]),
    ("NH",["03060","03101","03301","03431","03570","03701","03801","03901"]),
    ("NJ",["07101","07301","07501","07701","07901","08101","08301","08501","08701","08901"]),
    ("NM",["87101","87301","87501","87701","87901","88001","88201","88401","88601"]),
    ("NY",["10001","10301","10501","10701","10901","11101","11301","11501","11701","11901","12201","12401","12601","12801","13001","13201","13401","13601","13801","14001","14201","14401","14601","14801"]),
    ("NC",["27101","27301","27401","27601","27801","28001","28201","28401","28601","28801","28901"]),
    ("ND",["58001","58101","58201","58401","58501","58701","58801"]),
    ("OH",["43001","43201","43401","43601","43801","44001","44201","44401","44601","44801","45001","45201","45401","45601","45801"]),
    ("OK",["73001","73101","73401","73601","73801","74001","74101","74401","74601","74801"]),
    ("OR",["97001","97101","97201","97301","97501","97601","97701","97801","97901"]),
    ("PA",["15001","15201","15401","15601","15801","16001","16201","16401","16601","17001","17201","17401","17601","17801","18001","18201","18401","18601","18801","19001","19201","19401","19601","19801"]),
    ("RI",["02806","02840","02860","02893","02901","02908"]),
    ("SC",["29001","29101","29201","29401","29501","29601","29701","29801","29901"]),
    ("SD",["57101","57201","57301","57401","57501","57601","57701"]),
    ("TN",["37101","37201","37401","37601","37801","37901","38101","38201","38401","38501"]),
    ("TX",["75001","75201","75401","75601","75801","76001","76201","76401","76601","76801","77001","77201","77401","77601","77801","78001","78201","78401","78601","78801","79001","79201","79401","79601","79801","79901"]),
    ("UT",["84001","84101","84201","84301","84401","84501","84601","84701","84321","84720"]),
    ("VT",["05001","05101","05201","05301","05401","05601","05701","05819"]),
    ("VA",["22001","22101","22301","22501","22701","22901","23001","23201","23401","23601","23801","24001","24201","24401","24601"]),
    ("WA",["98001","98101","98301","98401","98501","98701","98801","99001","99201","99401","99501"]),
    ("WV",["24701","24901","25101","25301","25501","25701","25901","26101","26301","26501"]),
    ("WI",["53001","53201","53501","53701","53901","54101","54301","54501","54601","54801","54901"]),
    ("WY",["82001","82201","82401","82601","82801","82901"]),
    ("DC",["20001","20002","20003","20004","20005","20006","20007","20009"]),
]

ZIP_LIST = [(st, z) for st, zips in STATE_ZIPS for z in zips]

JUNK_NAMES = {
    "","company","website","call","find a pro","search","qualitypro","greenpro",
    "learn more","view profile","qualitypro certified","greenpro certified",
    "find local exterminators","pest control","what does it mean to be ?",
    "what does it mean to be qualitypro certified?",
    "the following companies have helped make this site possible",
}

def _abbr(raw):
    if not raw: return ""
    s = raw.strip()
    if len(s)==2 and s.isupper(): return s
    return STATE_NAME_TO_ABBR.get(s.lower(), s)


class NpmaCrawler(BaseCrawler):
    BASE_URL   = "https://www.pestworld.org"
    SEARCH_URL = "https://www.pestworld.org/find-local-exterminators/find-a-pro-results/"
    SOURCE_URL = "https://www.pestworld.org/find-local-exterminators/"

    def get_dataset_name(self): return "npma_pest_control"
    def get_source_url(self):   return self.SOURCE_URL
    def get_niche_fields(self):
        return ["qualitypro_certified","greenpro_certified","certifications",
                "npma_member","state_queried","member_url"]

    def _parse_cards(self, html, zipcode, state_abbr):
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        companies, seen_page = [], set()

        cards = (soup.select("li.pro-result-special-col section.pro-result-special") +
                 soup.select("li.pro-result-col") +
                 soup.select("div.company-col"))
        if not cards:
            for h2 in soup.select("h2.orange-text,h2.h3.orange-text"):
                p = h2.find_parent("li") or h2.find_parent("section") or h2.find_parent("div")
                if p: cards.append(p)

        seen_ids = set()
        unique = []
        for c in cards:
            if id(c) not in seen_ids:
                seen_ids.add(id(c)); unique.append(c)

        for card in unique:
            ne = (card.find("h2", class_=re.compile(r"orange",re.I)) or
                  card.find("h3", class_=re.compile(r"orange",re.I)) or
                  card.find(["h2","h3"]))
            if not ne: continue
            name = re.sub(r"(?i)(qualitypro|greenpro)\s*(certified)?","",ne.get_text(strip=True)).strip(" -|/\\")
            if not name or len(name)<4 or name.lower() in JUNK_NAMES: continue
            if name.lower().startswith(("what does","the following")): continue
            if not re.search(r"[A-Za-z]{3,}",name): continue
            if name.lower() in seen_page: continue
            seen_page.add(name.lower())

            street=city=addr_st=postal=""
            ae = card.find("address")
            if ae:
                for br in ae.find_all("br"): br.replace_with("\n")
                for ln in [l.strip() for l in ae.get_text().split("\n") if l.strip()]:
                    m = re.match(r"^(.+?),\s*([A-Z]{2})[,\s]+(\d{5}(?:-\d{4})?)\s*$",ln)
                    if m: city,addr_st,postal = m.group(1).strip(),m.group(2),m.group(3)
                    elif re.match(r"^\d+\s+\w",ln) and not street: street=ln

            phone=""
            ta = card.find("a", href=re.compile(r"^tel:",re.I))
            if ta: phone = ta["href"].replace("tel:","").strip()
            if not phone:
                m = re.search(r"(?:1[\s.\-]?)?\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}",card.get_text(" ",strip=True))
                if m: phone = m.group(0).strip()

            website=""
            for a in card.find_all("a", href=re.compile(r"^https?://",re.I)):
                h = a["href"]
                if "pestworld" not in h.lower() and "npma" not in h.lower():
                    website=h.rstrip("/"); break

            qp=gp=False
            for sp in card.find_all("span"):
                t=sp.get_text(strip=True).lower()
                if "qualitypro" in t: qp=True
                if "greenpro" in t: gp=True
            for img in card.find_all("img"):
                sa=(img.get("alt","")+img.get("src","")).lower()
                if "qualitypro" in sa: qp=True
                if "greenpro" in sa: gp=True

            member_url=""
            for a in card.find_all("a", href=True):
                h=a["href"]
                if "pestworld" in h.lower() and "find-a-pro-results" not in h:
                    member_url = h if h.startswith("http") else self.BASE_URL+h; break
            if not member_url:
                for a in card.find_all("a", href=re.compile(r"^/find-local-exterminators/[^?]",re.I)):
                    member_url = self.BASE_URL + a["href"]; break

            companies.append({
                "name":name,"street":street,"city":city,
                "state_abbr":_abbr(addr_st or state_abbr),
                "postal":postal,"phone":phone,"website":website,
                "qualitypro":qp,"greenpro":gp,
                "member_url":member_url,
                "state_queried":state_abbr,"zip_queried":zipcode,
            })
        return companies

    def crawl(self):
        try: from bs4 import BeautifulSoup
        except ImportError: self.logger.error("pip install beautifulsoup4"); return []
        try: from playwright.sync_api import sync_playwright
        except ImportError: self.logger.error("pip install playwright && playwright install chromium"); return []

        all_cos, seen = [], set()
        self.logger.info(f"Playwright crawl — {len(ZIP_LIST)} ZIPs")

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                viewport={"width":1280,"height":800})
            page = ctx.new_page()
            page.route(re.compile(r"(google-analytics|googletagmanager|facebook\.net|taboola|stackadapt|nextdoor\.com|pinterest|doubleclick|youtube\.com/iframe)"),
                       lambda r: r.abort())

            for i,(state_abbr,zipcode) in enumerate(ZIP_LIST,1):
                self.logger.info(f"  [{state_abbr}/{zipcode}] ({i}/{len(ZIP_LIST)})")
                try:
                    page.goto(f"{self.SEARCH_URL}?ZipCode={zipcode}", wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_selector("li.pro-result-special-col,li.pro-result-col,h2.orange-text", timeout=8000)
                except Exception as e:
                    self.logger.debug(f"    [{zipcode}] {e}")

                companies = self._parse_cards(page.content(), zipcode, state_abbr)
                added=0
                for co in companies:
                    uid = f"{co['name'].lower()}|{co['street'].lower()}|{co['postal']}".strip("|")
                    if uid and uid not in seen:
                        seen.add(uid); all_cos.append(co); added+=1
                self.logger.info(f"    {added} new (total:{len(all_cos)})")
                time.sleep(0.5)
            browser.close()

        self.logger.info(f"Complete — {len(all_cos)} unique companies")
        return all_cos

    def parse(self, raw_data):
        if not raw_data: return []
        records=[]
        for item in raw_data:
            if not isinstance(item,dict): continue
            name=item.get("name","").strip()
            if not name or len(name)<4 or name.lower() in JUNK_NAMES: continue
            certs=[]
            if item.get("qualitypro"): certs.append("QualityPro Certified")
            if item.get("greenpro"):   certs.append("GreenPro Certified")
            if not certs: certs.append("NPMA Member")
            records.append({
                "company_name":         name,
                "address":              item.get("street",""),
                "city":                 item.get("city",""),
                "state":                _abbr(item.get("state_abbr","")),
                "zip_code":             item.get("postal",""),
                "country":              "US",
                "phone":                item.get("phone",""),
                "email":                "",
                "website":              item.get("website",""),
                "qualitypro_certified": "True" if item.get("qualitypro") else "False",
                "greenpro_certified":   "True" if item.get("greenpro") else "False",
                "certifications":       "; ".join(certs),
                "npma_member":          "True",
                "state_queried":        item.get("state_queried",""),
                "member_url":           item.get("member_url",""),
            })
        self.logger.info(f"Parsed {len(records)} records")
        return records


if __name__ == "__main__":
    import subprocess, sqlite3
    os.makedirs("logs", exist_ok=True)
    for pkg,imp in [("beautifulsoup4","bs4"),("playwright","playwright")]:
        try: __import__(imp)
        except ImportError:
            subprocess.check_call([sys.executable,"-m","pip","install",pkg,"--quiet"])
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw: pw.chromium.launch(headless=True).close()
    except Exception:
        subprocess.check_call([sys.executable,"-m","playwright","install","chromium"])

    try:
        conn=sqlite3.connect("data/firmable.db")
        conn.execute("DROP TABLE IF EXISTS npma_pest_control")
        conn.commit(); conn.close()
        print("Old table dropped.")
    except Exception: pass

    crawler=NpmaCrawler()
    stats=crawler.run()
    print(f"\n{'='*50}")
    print(f"  New records  : {stats['total_new']:,}")
    print(f"  Updated      : {stats['total_updated']:,}")
    print(f"  Duplicates   : {stats['total_duplicates']:,}")
    print(f"{'='*50}")
    if stats["total_new"]+stats["total_updated"]>0:
        path=crawler.export_csv()
        if path: print(f"  CSV saved: {path}")