"""
pipeline/scheduler.py
──────────────────────
APScheduler-based recurring pipeline.

Schedules the full pipeline to run every Sunday at 02:00 UTC (configurable).
Also supports one-shot immediate execution for testing.

Usage
─────
    # Start the scheduler (runs indefinitely, triggers weekly)
    python pipeline/scheduler.py

    # Trigger one run immediately, then start weekly schedule
    python pipeline/scheduler.py --run-now

    # Custom schedule: every day at 03:30
    python pipeline/scheduler.py --hour 3 --minute 30 --day-of-week "*"

    # Custom DB path
    python pipeline/scheduler.py --db data/firmable.db
"""

import sys
import os
import argparse
import logging
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from pipeline.pipeline import run_pipeline

# ── logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(ROOT, "logs", "scheduler.log"),
                            encoding="utf-8"),
    ],
)
logger = logging.getLogger("scheduler")


# ── scheduled job ────────────────────────────────────────────────────────────

def pipeline_job(db_path: str = "data/firmable.db"):
    logger.info("Scheduled pipeline run triggered.")
    try:
        run_pipeline(db_path=db_path)
        logger.info("Scheduled pipeline run completed successfully.")
    except Exception as e:
        logger.error(f"Scheduled pipeline run FAILED: {e}", exc_info=True)


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Firmable pipeline scheduler (APScheduler)"
    )
    parser.add_argument("--db",          default="data/firmable.db",
                        help="Path to firmable.db")
    parser.add_argument("--run-now",     action="store_true",
                        help="Execute one full pipeline run immediately, then schedule")
    parser.add_argument("--hour",        default="2",
                        help="Hour (UTC) to trigger weekly run (default: 2)")
    parser.add_argument("--minute",      default="0",
                        help="Minute to trigger (default: 0)")
    parser.add_argument("--day-of-week", default="sun",
                        help="Day of week to run (default: sun). Use '*' for daily.")
    args = parser.parse_args()

    # Ensure logs dir exists
    os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)

    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        print("\n[ERROR] APScheduler not installed.")
        print("Run:  pip install apscheduler\n")
        sys.exit(1)

    scheduler = BlockingScheduler(timezone="UTC")

    trigger = CronTrigger(
        day_of_week=args.day_of_week,
        hour=int(args.hour),
        minute=int(args.minute),
    )

    scheduler.add_job(
        func=pipeline_job,
        trigger=trigger,
        kwargs={"db_path": args.db},
        id="firmable_weekly_pipeline",
        name="Firmable Weekly B2B Data Pipeline",
        replace_existing=True,
        max_instances=1,           # prevent overlapping runs
        misfire_grace_time=3600,   # allow up to 1h late start
    )

    jobs = scheduler.get_jobs()
    try:
        next_run = jobs[0].next_run_time if jobs else "N/A"
    except AttributeError:
        next_run = "scheduled"

    print(f"\n{'='*60}")
    print(f"  Firmable Scheduler started")
    print(f"  Schedule : {args.day_of_week.upper()} {args.hour.zfill(2)}:{args.minute.zfill(2)} UTC")
    print(f"  DB       : {os.path.abspath(args.db)}")
    print(f"  Press Ctrl+C to stop")
    print(f"{'='*60}\n")

    if args.run_now:
        logger.info("--run-now flag set. Running pipeline immediately...")
        pipeline_job(db_path=args.db)

    logger.info(f"Scheduler started. Next run: {next_run}")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user.")
        scheduler.shutdown(wait=False)
        print("\nScheduler stopped.")


if __name__ == "__main__":
    main()