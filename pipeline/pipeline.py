"""
pipeline/pipeline.py
─────────────────────
Orchestrates all 25 crawlers end-to-end.

Features
────────
• Runs every crawler in sequence (or a named subset via --crawlers)
• Completely safe to re-run — upsert logic in BaseCrawler prevents duplicates
• Logs every run to the `pipeline_runs` meta-table in firmable.db
• Prints a new-records-this-week report after each full run

Usage
─────
    # Run all crawlers
    python pipeline/pipeline.py

    # Run specific crawlers only
    python pipeline/pipeline.py --crawlers c01_usda_organic c06_brewers

    # Dry-run (import check, no crawling)
    python pipeline/pipeline.py --dry-run

    # Custom DB path
    python pipeline/pipeline.py --db data/firmable.db
"""

import sys
import os
import argparse
import importlib
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import List, Optional

# ── resolve project root so imports work regardless of CWD ─────────────────
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from database.schema import ensure_pipeline_runs_table, print_summary

# ── Crawler registry ───────────────────────────────────────────────────────
CRAWLER_REGISTRY: List[tuple] = [
    ("crawlers.c01_usda_organic",   "UsdaOrganicCrawler"),
    ("crawlers.c02_sec_iapd",       "SecIapdCrawler"),
    ("crawlers.c03_samhsa",         "SamhsaCrawler"),
    ("crawlers.c04_api_monogram",   "ApiMonogramCrawler"),
    ("crawlers.c05_asla",           "ASLACrawler"),
    ("crawlers.c06_brewers",        "BrewersAssocCrawler"),
    ("crawlers.c07_carf",           "CarfAccreditedCrawler"),
    ("crawlers.c08_awwa",           "AWWAWaterUtilitiesCrawler"),
    ("crawlers.c09_achc",           "AchcCrawler"),
    ("crawlers.c10_phcc",           "PhccContractorCrawler"),
    ("crawlers.c11_nfda",           "NFDACrawler"),
    ("crawlers.c12_nate",           "NateHvacCrawler"),
    ("crawlers.c13_ccof",           "CCOFCrawler"),
    ("crawlers.c14_pedb",           "DataCenterMapCrawler"),
    ("crawlers.c15_nari",           "NARIRemodelersCrawler"),
    ("crawlers.c16_asa",            "AsaStaffingCrawler"),
    ("crawlers.c17_agc",            "AGCCrawler"),
    ("crawlers.c18_ampp",           "AMPPCrawler"),
    ("crawlers.c19_acec",           "ACECEngineeringFirmsCrawler"),
    ("crawlers.c20_npma",           "NpmaCrawler"),
    ("crawlers.c21_cgap",           "CanadaGAPCrawler"),
    ("crawlers.c22_cicc",           "CICCCrawler"),
    ("crawlers.c23_chba",           "ChbaCrawler"),
    ("crawlers.c24_cor",            "CORCertifiedEmployersCrawler"),
    ("crawlers.c25_cfia",           "CfiaCrawler"),
]

_SHORT_ALIAS = {entry[0].split(".")[-1]: entry for entry in CRAWLER_REGISTRY}


# ── helpers ─────────────────────────────────────────────────────────────────

def _log_run_start(conn: sqlite3.Connection, run_id: str, crawler: str) -> int:
    cur = conn.execute(
        """INSERT INTO pipeline_runs
               (run_id, crawler, started_at, status)
           VALUES (?, ?, ?, 'running')""",
        (run_id, crawler, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    return cur.lastrowid


def _log_run_finish(conn, row_id, stats, status="success", notes=""):
    conn.execute(
        """UPDATE pipeline_runs
           SET finished_at   = ?,
               status        = ?,
               total_parsed  = ?,
               total_new     = ?,
               total_updated = ?,
               total_errors  = ?,
               notes         = ?
           WHERE id = ?""",
        (
            datetime.now(timezone.utc).isoformat(),
            status,
            stats.get("total_parsed", 0),
            stats.get("total_new", 0),
            stats.get("total_updated", 0),
            stats.get("total_errors", 0),
            notes,
            row_id,
        ),
    )
    conn.commit()


def _new_records_report(db_path: str):
    """Print count of new records per table added in the last 7 days."""
    conn = sqlite3.connect(db_path)
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name != 'pipeline_runs' ORDER BY name"
    )
    tables = [r[0] for r in cur.fetchall()]
    total_new = 0
    rows = []
    for t in tables:
        try:
            c = conn.execute(
                f"SELECT COUNT(*) FROM \"{t}\" "
                "WHERE first_seen >= datetime('now', '-7 days')"
            ).fetchone()[0]
            total_new += c
            if c > 0:
                rows.append((t, c))
        except Exception:
            pass
    conn.close()

    print(f"\n{'─'*55}")
    print(f"  NEW RECORDS THIS WEEK")
    print(f"{'─'*55}")
    if rows:
        for t, c in sorted(rows, key=lambda x: -x[1]):
            print(f"  {t:<40}  +{c:>6,}")
    else:
        print("  (none detected)")
    print(f"{'─'*55}")
    print(f"  {'TOTAL':<40}  +{total_new:>6,}")
    print(f"{'─'*55}\n")


def _resolve_crawlers(names):
    if not names:
        return CRAWLER_REGISTRY
    result = []
    for n in names:
        if n in _SHORT_ALIAS:
            result.append(_SHORT_ALIAS[n])
        else:
            print(f"[WARN] Unknown crawler alias '{n}' — skipping")
    return result


# ── main pipeline ───────────────────────────────────────────────────────────

def run_pipeline(db_path="data/firmable.db", crawlers=None, dry_run=False):
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    ensure_pipeline_runs_table(db_path)

    selected = _resolve_crawlers(crawlers)
    run_id   = str(uuid.uuid4())[:8]
    total    = len(selected)

    print(f"\n{'='*60}")
    print(f"  Firmable Pipeline  |  run_id={run_id}")
    print(f"  Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  DB      : {os.path.abspath(db_path)}")
    print(f"  Crawlers: {total} selected")
    if dry_run:
        print("  Mode    : DRY-RUN (import check only)")
    print(f"{'='*60}\n")

    meta_conn = sqlite3.connect(db_path)
    passed = failed = 0

    for idx, (module_path, class_name) in enumerate(selected, 1):
        short = module_path.split(".")[-1]
        print(f"[{idx:>2}/{total}] {short} → {class_name}")

        if dry_run:
            try:
                mod = importlib.import_module(module_path)
                getattr(mod, class_name)
                print(f"        ✓ import OK")
                passed += 1
            except Exception as e:
                print(f"        ✗ import FAILED: {e}")
                failed += 1
            continue

        row_id = _log_run_start(meta_conn, run_id, short)
        try:
            mod     = importlib.import_module(module_path)
            cls     = getattr(mod, class_name)
            crawler = cls(db_path=db_path)
            stats   = crawler.run()
            _log_run_finish(meta_conn, row_id, stats, status="success")
            print(
                f"        ✓ new={stats['total_new']:,}  "
                f"updated={stats['total_updated']:,}  "
                f"errors={stats['total_errors']:,}"
            )
            passed += 1
        except Exception as e:
            err = {"total_parsed": 0, "total_new": 0,
                   "total_updated": 0, "total_errors": 1}
            _log_run_finish(meta_conn, row_id, err,
                            status="failed", notes=str(e)[:500])
            print(f"        ✗ FAILED: {e}")
            failed += 1

    meta_conn.close()

    print(f"\n{'='*60}")
    print(f"  Pipeline complete  |  {passed} passed, {failed} failed")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    if not dry_run:
        _new_records_report(db_path)
        print_summary(db_path)


# ── CLI ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Firmable B2B data pipeline — runs all 25 crawlers"
    )
    parser.add_argument("--db", default="data/firmable.db",
                        help="SQLite DB path (default: data/firmable.db)")
    parser.add_argument("--crawlers", nargs="+", metavar="NAME",
                        help="Run specific crawlers only (e.g. c01_usda_organic)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Import-check only — no crawling or DB writes")
    args = parser.parse_args()
    run_pipeline(db_path=args.db, crawlers=args.crawlers, dry_run=args.dry_run)