# Firmable B2B Data Pipeline — US & Canada

A niche B2B dataset discovery and pipeline engineering project covering **25 authoritative registers and directories** across the United States and Canada — crawled, cleaned, deduplicated, and stored in a queryable SQLite database with a recurring weekly scheduler.

---

## Table of Contents

- [Overview](#overview)
- [Datasets (25 total)](#datasets-25-total)
- [Project Structure](#project-structure)
- [Setup & Installation](#setup--installation)
- [Running the Crawlers](#running-the-crawlers)
- [Database](#database)
- [Pipeline & Scheduler](#pipeline--scheduler)
- [Live Demo Queries](#live-demo-queries)
- [Research Methodology](#research-methodology)

---

## Overview

| Stat | Value |
|---|---|
| Total datasets | 25 (20 US + 5 Canada) |
| Total records collected | ~186,939 |
| Database | SQLite (`data/firmable.db`) |
| Sample data | `data/samples/` — 100-row CSV per dataset |
| Scheduler | APScheduler (weekly, every Sunday 02:00 UTC) |
| Deduplication | Hash-based upsert — safe to re-run |

---

## Datasets (25 total)

### 🇺🇸 United States (20)

| # | Dataset | Source | Records | B2B Use Case |
|---|---------|--------|---------|--------------|
| 1 | USDA Certified Organic Operators | [ams.usda.gov](https://ams.usda.gov/organic-program/national-organic-program-database) | ~33,729 | Organic ingredient suppliers, food distributors, agri-tech platforms |
| 2 | SEC-Registered Investment Advisers (IAPD) | [adviserinfo.sec.gov](https://adviserinfo.sec.gov) | ~12,711 | Fintech, compliance software, wealthtech vendors targeting RIAs |
| 3 | SAMHSA Substance Use Treatment Facilities | [findtreatment.gov](https://findtreatment.gov) | ~17,786 | EHR vendors, billing software, telehealth platforms |
| 4 | API Monogram Licensed Manufacturers | [api.org](https://www.api.org/products-and-services/standards/api-monogram-apiqr-program/licensee-directory) | ~7,560 | Oil & gas procurement, supplier vetting, supply chain platforms |
| 5 | ASLA Landscape Architecture Firms | [asla.org](https://www.asla.org/findafirm.aspx) | ~1,380 | CAD/BIM software, plant suppliers, project management tools |
| 6 | Brewers Association Craft Breweries | [brewersassociation.org](https://www.brewersassociation.org/directories/breweries/) | ~11,031 | Ingredient suppliers, POS software, distribution platforms |
| 7 | CARF Accredited Providers | [carf.org](https://www.carf.org/providerSearch.aspx) | ~6,080 | Healthcare software, billing platforms, compliance consulting |
| 8 | AWWA Water Utilities | [awwa.org](https://www.awwa.org) | ~947 | Water infrastructure tech, IoT sensors, municipal SaaS |
| 9 | ACHC Accredited Healthcare Providers | [achc.org](https://www.achc.org) | ~603 | Home health SaaS, telehealth, medical supply vendors |
| 10 | PHCC Plumbing-Heating-Cooling Contractors | [phccweb.org](https://www.phccweb.org) | ~1,845 | Field service software, parts distributors, equipment OEMs |
| 11 | NFDA Funeral Homes | [nfda.org](https://www.nfda.org) | ~7,255 | Death care software, pre-need insurance, cremation equipment |
| 12 | NATE Certified HVAC Contractors | [natex.org](https://www.natex.org) | ~4,077 | HVAC equipment manufacturers, parts distributors, service software |
| 13 | CCOF Certified Organic Operators | [ccof.org](https://www.ccof.org/organic-search) | ~8,950 | Organic supply chain, food distribution, retail sourcing |
| 14 | PeeringDB US Data Centers | [peeringdb.com](https://www.peeringdb.com) | ~1,367 | Network infrastructure, colocation, cloud interconnect vendors |
| 15 | NARI Certified Remodelers | [nari.org](https://www.nari.org) | ~2,423 | Construction tech, material suppliers, project management |
| 16 | ASA Staffing Agencies | [americanstaffing.net](https://americanstaffing.net) | ~9,240 | HR tech, payroll software, background screening vendors |
| 17 | AGC Construction Member Directory | [agc.org](https://www.agc.org) | ~1,828 | Construction software, equipment rental, surety bond providers |
| 18 | AMPP Accredited QP Contractors | [ampp.org](https://www.ampp.org) | ~483 | Coatings manufacturers, corrosion protection, industrial maintenance |
| 19 | ACEC Engineering Firms | [acec.org](https://www.acec.org) | ~851 | Engineering software, professional liability insurance, training |
| 20 | NPMA Pest Control Companies | [pestworld.org](https://www.pestworld.org) | ~1,210 | Pest control software, chemical suppliers, franchise development |

### 🇨🇦 Canada (5)

| # | Dataset | Source | Records | B2B Use Case |
|---|---------|--------|---------|--------------|
| 21 | CanadaGAP Certified Operations | [canadagap.ca](https://www.canadagap.ca) | ~1,890 | Food safety compliance, retail supplier qualification |
| 22 | CICC Registered Immigration Consultants | [college-ic.ca](https://college-ic.ca) | ~16,706 | Immigration SaaS, HR platforms, relocation services |
| 23 | CHBA Home Builders | [chba.ca](https://www.chba.ca) | ~8,432 | Construction tech, material suppliers, new home warranty |
| 24 | COR Certified Employers | [alberta.ca](https://www.alberta.ca/find-employers-with-cor) | ~9,713 | OHS software, safety training, WCB insurance |
| 25 | CFIA Food Establishment Licences | [inspection.canada.ca](https://inspection.canada.ca) | ~18,815 | Food compliance, cold chain logistics, distribution |

---

## Project Structure

```
firmable-pipeline/
├── crawlers/
│   ├── base_crawler.py          # Base class: logging, clean, dedup, upsert
│   ├── c01_usda_organic.py
│   ├── c02_sec_iapd.py
│   ├── c03_samhsa.py
│   ├── c04_api_monogram.py
│   ├── c05_asla.py
│   ├── c06_brewers.py
│   ├── c07_carf.py
│   ├── c08_awwa.py
│   ├── c09_achc.py
│   ├── c10_phcc.py
│   ├── c11_nfda.py
│   ├── c12_nate.py
│   ├── c13_ccof.py
│   ├── c14_pedb.py
│   ├── c15_nari.py
│   ├── c16_asa.py
│   ├── c17_agc.py
│   ├── c18_ampp.py
│   ├── c19_acec.py
│   ├── c20_npma.py
│   ├── c21_cgap.py
│   ├── c22_cicc.py
│   ├── c23_chba.py
│   ├── c24_cor.py
│   └── c25_cfia.py
├── database/
│   ├── __init__.py
│   └── schema.py                # DB inspector — table summary, row counts, DDL
├── pipeline/
│   ├── __init__.py
│   ├── pipeline.py              # Orchestrator — runs all/selected crawlers
│   └── scheduler.py             # APScheduler weekly runner
├── data/
│   └── samples/                 # 100-row CSV samples for all 25 datasets
├── requirements.txt
└── README.md
```

> **Note:** `data/firmable.db` (~107MB) is excluded from the repository due to GitHub's file size limit. It is generated locally by running the pipeline. Sample data is available in `data/samples/`.

---

## Setup & Installation

**Requirements:** Python 3.10+

```bash
# 1. Clone the repo
git clone https://github.com/23f2003614/firmable-pipeline.git
cd firmable-pipeline

# 2. Create virtual environment
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Mac/Linux

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Running the Crawlers

### Run a single crawler
```bash
python crawlers/c06_brewers.py
```

### Run all 25 crawlers via pipeline
```bash
python pipeline/pipeline.py
```

### Run specific crawlers only
```bash
python pipeline/pipeline.py --crawlers c01_usda_organic c13_ccof
```

### Dry-run (import check only, no crawling)
```bash
python pipeline/pipeline.py --dry-run
```

> **Re-run safety:** All crawlers use hash-based upsert. Re-running never duplicates existing records — existing rows are updated, new rows are inserted.

---

## Database

SQLite database at `data/firmable.db`. Every table shares this base schema:

| Column | Description |
|--------|-------------|
| `id` | Auto-increment primary key |
| `company_name` | Organisation name |
| `website` | Website URL |
| `phone` | Phone number |
| `email` | Email address |
| `address` | Street address |
| `city` | City |
| `state` | State / Province |
| `country` | Country |
| `dedup_hash` | MD5 hash for deduplication |
| `first_seen` | Timestamp of first crawl |
| `last_seen` | Timestamp of most recent crawl |
| `is_new` | 1 = first time seen, 0 = updated |
| `dataset` | Source dataset name |
| `crawl_date` | ISO timestamp of this crawl |
| + niche columns | Dataset-specific fields (license #, cert type, etc.) |

A `pipeline_runs` meta-table logs every crawler run with start time, finish time, counts, and pass/fail status.

### Inspect the database
```bash
# Full summary — all tables, row counts, new records this week
python database/schema.py

# Inspect one table's columns
python database/schema.py --table brewers_association

# Dump all CREATE statements
python database/schema.py --export-ddl
```

### Example SQL queries
```sql
-- All craft breweries in Texas
SELECT company_name, city, phone, website
FROM brewers_association
WHERE state = 'TX' AND is_craft_brewery = '1';

-- USDA organic operators added this week
SELECT company_name, city, state, certificate_number
FROM usda_organic
WHERE first_seen >= datetime('now', '-7 days');

-- Pipeline run history
SELECT crawler, started_at, status, total_new, total_updated
FROM pipeline_runs
ORDER BY started_at DESC;

-- Canadian immigration consultants by province
SELECT province, COUNT(*) as count
FROM cicc_immigration_consultants
GROUP BY province ORDER BY count DESC;
```

---

## Pipeline & Scheduler

### Run pipeline once (all 25 crawlers)
```bash
python pipeline/pipeline.py
```

### Start weekly scheduler (every Sunday 02:00 UTC)
```bash
python pipeline/scheduler.py
```

### Run immediately + start weekly schedule
```bash
python pipeline/scheduler.py --run-now
```

### Custom schedule (e.g. daily at 03:30)
```bash
python pipeline/scheduler.py --hour 3 --minute 30 --day-of-week "*"
```

**New record detection:** After every pipeline run, a report prints the count of records added in the last 7 days per dataset.

---

## Live Demo Queries

```bash
# Open SQLite shell
sqlite3 data/firmable.db

# Total records across all datasets
SELECT dataset, COUNT(*) as total FROM usda_organic GROUP BY dataset;

# New records this week (any table)
SELECT COUNT(*) FROM brewers_association
WHERE first_seen >= datetime('now', '-7 days');

# Pipeline run history
SELECT crawler, started_at, status, total_new, total_updated
FROM pipeline_runs ORDER BY started_at DESC;
```

---

## Research Methodology

Datasets were selected using three criteria:

1. **Niche specificity** — authoritative registers from professional bodies, licensing boards, and industry associations; not generic aggregators or company lists.
2. **B2B sales value** — each dataset maps to a clear buyer (SaaS vendor, equipment supplier, insurance provider, etc.) who would pay for targeted outreach to that specific niche.
3. **Crawlability** — publicly accessible without login or payment, with structured HTML, JSON APIs, or downloadable PDFs.

---

*GitHub Repository: https://github.com/23f2003614/firmable-pipeline*