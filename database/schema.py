"""
database/schema.py
──────────────────
Schema inspector and migration utility for firmable.db.

Usage:
    python database/schema.py                      # print all tables + row counts
    python database/schema.py --table brewers_association   # inspect one table
    python database/schema.py --export-ddl         # dump CREATE statements
"""

import sqlite3
import os
import sys
import argparse
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "firmable.db")


# ── helpers ────────────────────────────────────────────────────────────────

def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    if not os.path.exists(db_path):
        print(f"[ERROR] Database not found: {db_path}")
        sys.exit(1)
    return sqlite3.connect(db_path)


def list_tables(conn: sqlite3.Connection) -> list[str]:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    return [r[0] for r in cur.fetchall()]


def table_info(conn: sqlite3.Connection, table: str) -> list[dict]:
    cur = conn.execute(f'PRAGMA table_info("{table}")')
    cols = ["cid", "name", "type", "notnull", "dflt_value", "pk"]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def row_count(conn: sqlite3.Connection, table: str) -> int:
    cur = conn.execute(f'SELECT COUNT(*) FROM "{table}"')
    return cur.fetchone()[0]


def new_this_week(conn: sqlite3.Connection, table: str) -> int:
    """Records where first_seen is within the last 7 days."""
    try:
        cur = conn.execute(
            f"""
            SELECT COUNT(*) FROM "{table}"
            WHERE first_seen >= datetime('now', '-7 days')
            """
        )
        return cur.fetchone()[0]
    except Exception:
        return 0


def export_ddl(conn: sqlite3.Connection, table: str) -> str:
    cur = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    )
    row = cur.fetchone()
    return row[0] if row else ""


# ── main report ────────────────────────────────────────────────────────────

def print_summary(db_path: str = DB_PATH):
    conn = get_connection(db_path)
    tables = list_tables(conn)
    if not tables:
        print("No tables found in database.")
        conn.close()
        return

    print(f"\n{'='*72}")
    print(f"  Firmable Pipeline — Database Summary")
    print(f"  DB: {os.path.abspath(db_path)}")
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*72}")
    print(f"  {'Table':<40} {'Rows':>8}  {'New (7d)':>10}  {'Columns':>8}")
    print(f"  {'-'*40} {'-'*8}  {'-'*10}  {'-'*8}")

    total_rows = 0
    for t in tables:
        rc  = row_count(conn, t)
        nw  = new_this_week(conn, t)
        nc  = len(table_info(conn, t))
        total_rows += rc
        print(f"  {t:<40} {rc:>8,}  {nw:>10,}  {nc:>8}")

    print(f"  {'─'*72}")
    print(f"  {'TOTAL':<40} {total_rows:>8,}")
    print(f"{'='*72}\n")
    conn.close()


def inspect_table(table: str, db_path: str = DB_PATH):
    conn = get_connection(db_path)
    info = table_info(conn, table)
    rc   = row_count(conn, table)
    nw   = new_this_week(conn, table)
    print(f"\nTable: {table}  ({rc:,} rows, {nw:,} new in last 7 days)")
    print(f"{'─'*55}")
    print(f"  {'#':<4} {'Column':<35} {'Type':<10}")
    print(f"  {'─'*4} {'─'*35} {'─'*10}")
    for col in info:
        print(f"  {col['cid']:<4} {col['name']:<35} {col['type']:<10}")
    print()
    conn.close()


def dump_ddl(db_path: str = DB_PATH):
    conn = get_connection(db_path)
    for table in list_tables(conn):
        ddl = export_ddl(conn, table)
        print(ddl + ";\n")
    conn.close()


# ── ensure pipeline_runs table exists ──────────────────────────────────────

def ensure_pipeline_runs_table(db_path: str = DB_PATH):
    """
    Creates a meta-table that the pipeline uses to log each run.
    Safe to call on every startup — uses CREATE IF NOT EXISTS.
    """
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id        TEXT    NOT NULL,
            crawler       TEXT    NOT NULL,
            started_at    TEXT    NOT NULL,
            finished_at   TEXT,
            status        TEXT    DEFAULT 'running',
            total_parsed  INTEGER DEFAULT 0,
            total_new     INTEGER DEFAULT 0,
            total_updated INTEGER DEFAULT 0,
            total_errors  INTEGER DEFAULT 0,
            notes         TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_runs_crawler ON pipeline_runs (crawler)"
    )
    conn.commit()
    conn.close()


# ── CLI ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Firmable DB schema inspector")
    parser.add_argument("--db",         default=DB_PATH,  help="Path to firmable.db")
    parser.add_argument("--table",      default=None,     help="Inspect a specific table")
    parser.add_argument("--export-ddl", action="store_true", help="Dump all CREATE statements")
    args = parser.parse_args()

    if args.export_ddl:
        dump_ddl(args.db)
    elif args.table:
        inspect_table(args.table, args.db)
    else:
        print_summary(args.db)