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
| Scheduler | APScheduler (weekly, every Sunday 02:00 UTC) |
| Deduplication | Hash-based upsert — safe to re-run |

---

## Datasets (25 total)

### 🇺🇸 United States (20)

| # | Dataset | Source | Records | B2B Use Case |
|---|---------|--------|---------|--------------|
| 1 | USDA Certified Organic Operators | [ams.usda.gov](https://ams.usda.gov/organic-program/national-organic-program-database) | ~33,729 | Target organic food manufacturers, distributors, retailers |
| 2 | SEC-Registered Investment Advisers (IAPD) | [adviserinfo.sec.gov](https://adviserinfo.sec.gov) | ~12,711 | Fintech, compliance software, B2B services targeting RIAs |
| 3 | SAMHSA Substance Use Treatment Facilities | [findtreatment.gov](https://findtreatment.gov) | ~17,786 | Healthcare SaaS, billing software, EHR vendors |
| 4 | API Monogram Licensed Manufacturers | [api.org](https://www.api.org/products-and-services/standards/api-monogram-apiqr-program/licensee-directory) | ~7,560 | Oil & gas equipment procurement, supplier vetting |
| 5 | ASLA Landscape Architecture Firms | [asla.org](https://www.asla.org/findafirm.aspx) | ~1,380 | CAD/BIM software, project management tools |
| 6 | Brewers Association Craft Breweries | [brewersassociation.org](https://www.brewersassociation.org/directories/breweries/) | ~11,031 | Hospitality tech, distribution, ingredient suppliers |
| 7 | CARF Accredited Providers | [carf.org](https://www.carf.org/providerSearch.aspx) | ~6,080 | Healthcare software, accreditation consulting |
| 8 | AWWA Water Utilities | [awwa.org](https://www.awwa.org) | ~947 | Water infrastructure, IoT sensors, municipal tech |
| 9 | ACHC Accredited Healthcare Providers | [achc.org](https://www.achc.org) | ~603 | Home health SaaS, billing, telehealth vendors |
| 10 | PHCC Plumbing-Heating-Cooling Contractors | [phccweb.org](https://www.phccweb.org) | ~1,845 | Field service software, parts suppliers, HVAC tech |
| 11 | NFDA Funeral Homes | [nfda.org](https://www.nfda.org) | ~7,255 | Death care software, pre-need insurance, CRM |
| 12 | NATE Certified HVAC Contractors | [natex.org](https://www.natex.org) | ~4,077 | HVAC equipment manufacturers, service software |
| 13 | CCOF Certified Organic Operators | [ccof.org](https://www.ccof.org/organic-search) | ~8,950 | Organic supply chain, food distribution, retail sourcing |
| 14 | PeeringDB US Data Centers | [peeringdb.com](https://www.peeringdb.com) | ~1,367 | Network infrastructure, colocation, cloud interconnect |
| 15 | NARI Certified Remodelers | [nari.org](https://www.nari.org) | ~2,423 | Construction tech, material suppliers, project management |
| 16 | ASA Staffing Agencies | [americanstaffing.net](https://americanstaffing.net) | ~9,240 | HR tech, payroll software, workforce management |
| 17 | AGC Construction Member Directory | [agc.org](https://www.agc.org) | ~1,828 | Construction software, insurance, equipment leasing |
| 18 | AMPP Accredited QP Contractors | [ampp.org](https://www.ampp.org) | ~483 | Coatings, corrosion protection, industrial maintenance |
| 19 | ACEC Engineering Firms | [acec.org](https://www.acec.org) | ~851 | Engineering software, project management, insurance |
| 20 | NPMA Pest Control Companies | [pestworld.org](https://www.pestworld.org) | ~1,210 | Pest control software, chemical suppliers, franchises |

### 🇨🇦 Canada (5)

| # | Dataset | Source | Records | B2B Use Case |
|---|---------|--------|---------|--------------|
| 21 | CanadaGAP Certified Operations | [canadagap.ca](https://www.canadagap.ca) | ~1,890 | Food safety compliance, retail supplier qualification |
| 22 | CICC Registered Immigration Consultants | [college-ic.ca](https://college-ic.ca) | ~16,706 | Immigration SaaS, HR platforms, relocation services |
| 23 | CHBA Home Builders | [chba.ca](https://www.chba.ca) | ~8,432 | Construction tech, material suppliers, new home warranty |
| 24 | COR Certified Employers (Canada) | [cortus.ca](https://www.cortus.ca) | ~9,713 | Safety training, OHS software, insurance |
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
│   └── firmable.db              # SQLite database (~186k records)
├── requirements.txt
└── README.md
```

---

## Setup & Installation

**Requirements:** Python 3.10+

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/firmable-pipeline.git
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

-- Count records per dataset
SELECT dataset, COUNT(*) as total
FROM usda_organic
GROUP BY dataset;

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

**New record detection:** After every pipeline run, a report prints the count of records added in the last 7 days per dataset — making it easy to surface what's changed week over week.

---

## Live Demo Queries

Quick queries to run during the interview:

```bash
# Open SQLite shell
sqlite3 data/firmable.db

# Total records across all datasets
SELECT SUM(cnt) FROM (
  SELECT COUNT(*) cnt FROM usda_organic UNION ALL
  SELECT COUNT(*) FROM brewers_association UNION ALL
  SELECT COUNT(*) FROM samhsa_treatment_facilities
);

# New records this week
SELECT name FROM sqlite_master WHERE type='table' AND name != 'pipeline_runs';
-- Then for any table:
SELECT COUNT(*) FROM cfia_food_licences WHERE first_seen >= datetime('now','-7 days');
```

---

## Research Methodology

Datasets were selected using three criteria:

1. **Niche specificity** — authoritative registers from professional bodies, licensing boards, and industry associations; not generic aggregators or company lists.
2. **B2B sales value** — each dataset maps to a clear buyer (SaaS vendor, equipment supplier, insurance provider, etc.) who would pay for targeted outreach to that specific niche.
3. **Crawlability** — publicly accessible without login or payment, with structured HTML, JSON APIs, or downloadable PDFs.

Sources were deliberately mixed across government agencies (USDA, SAMHSA, SEC, CFIA), industry associations (ASLA, AGC, NFDA, CHBA), certification bodies (NATE, CARF, CCOF, CanadaGAP), and professional registries (CICC, ACEC, AMPP) to ensure breadth and avoid over-reliance on any single source type.