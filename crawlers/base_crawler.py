"""
Base crawler class — all 25 crawlers inherit from this.
Handles: logging, cleaning, dedup, database storage, error handling, retries.
Each child only implements: crawl() and parse()
"""

import os
import time
import csv
import sqlite3
import logging
import hashlib
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple


class BaseCrawler(ABC):

    def __init__(self, db_path="data/firmable.db", log_dir="logs"):
        self.db_path = db_path
        self.log_dir = log_dir
        self.session_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.stats = {
            'total_parsed': 0,
            'total_new': 0,
            'total_updated': 0,
            'total_duplicates': 0,
            'total_errors': 0,
        }
        self._setup_logging()

    def _setup_logging(self):
        os.makedirs(self.log_dir, exist_ok=True)
        log_file = os.path.join(self.log_dir, f"{self.get_dataset_name()}_{self.session_id}.log")
        self.logger = logging.getLogger(self.get_dataset_name())
        self.logger.setLevel(logging.DEBUG)
        if not self.logger.handlers:
            fh = logging.FileHandler(log_file)
            fh.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
            self.logger.addHandler(fh)
            import sys as _sys, io as _io
            try:
                stream = _io.TextIOWrapper(
                    _sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True
                )
            except Exception:
                stream = _sys.stdout
            ch = logging.StreamHandler(stream)
            ch.setLevel(logging.INFO)
            ch.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
            self.logger.addHandler(ch)

    @abstractmethod
    def get_dataset_name(self) -> str:
        pass

    @abstractmethod
    def get_source_url(self) -> str:
        pass

    @abstractmethod
    def get_niche_fields(self) -> List[str]:
        pass

    @abstractmethod
    def crawl(self) -> Any:
        pass

    @abstractmethod
    def parse(self, raw_data: Any) -> List[Dict]:
        pass

    def run(self) -> Dict:
        self.logger.info(f"{'='*50}")
        self.logger.info(f"STARTING: {self.get_dataset_name()}")
        self.logger.info(f"Source: {self.get_source_url()}")
        self.logger.info(f"{'='*50}")
        try:
            self.logger.info("Step 1: Fetching data...")
            raw_data = self.crawl()
            self.logger.info("Step 2: Parsing records...")
            records = self.parse(raw_data)
            self.stats['total_parsed'] = len(records)
            self.logger.info(f"  Parsed {len(records)} records")
            self.logger.info("Step 3: Cleaning data...")
            cleaned = [self._clean(r) for r in records if r.get('company_name')]
            self.logger.info(f"  {len(cleaned)} records after cleaning")
            self.logger.info("Step 4: Deduplicating...")
            unique = self._deduplicate(cleaned)
            self.stats['total_duplicates'] = len(cleaned) - len(unique)
            self.logger.info(f"  {len(unique)} unique records")
            self.logger.info("Step 5: Saving to database...")
            new, updated = self._store(unique)
            self.stats['total_new'] = new
            self.stats['total_updated'] = updated
        except Exception as e:
            self.stats['total_errors'] += 1
            self.logger.error(f"FAILED: {e}", exc_info=True)
            raise
        self.logger.info(f"DONE: {self.stats['total_new']} new, {self.stats['total_updated']} updated")
        return self.stats

    def _clean(self, record: Dict) -> Dict:
        cleaned = {}
        for key, val in record.items():
            if isinstance(val, str):
                val = ' '.join(val.strip().split())
                if val.lower() in ('n/a', 'na', 'none', 'null', '-', ''):
                    val = None
            cleaned[key] = val
        if cleaned.get('phone'):
            phone = ''.join(c for c in cleaned['phone'] if c.isdigit() or c in '+-(). ')
            cleaned['phone'] = phone.strip() or None
        if cleaned.get('website') and not str(cleaned['website']).startswith('http'):
            cleaned['website'] = 'https://' + cleaned['website']
        if cleaned.get('email'):
            cleaned['email'] = str(cleaned['email']).lower()
        cleaned['dataset'] = self.get_dataset_name()
        cleaned['source_url'] = self.get_source_url()
        cleaned['crawl_date'] = datetime.now(timezone.utc).isoformat()
        cleaned['crawl_session'] = self.session_id
        return cleaned

    def _deduplicate(self, records: List[Dict]) -> List[Dict]:
        seen = set()
        unique = []
        for rec in records:
            raw_key = (
                (rec.get('company_name') or '').lower() +
                (rec.get('address') or '').lower() +
                (rec.get('state') or '').lower() +
                self.get_dataset_name()
            )
            hash_key = hashlib.md5(raw_key.encode()).hexdigest()
            if hash_key not in seen:
                seen.add(hash_key)
                rec['dedup_hash'] = hash_key
                unique.append(rec)
        return unique

    def _store(self, records: List[Dict]) -> Tuple[int, int]:
        if not records:
            return 0, 0
        os.makedirs(os.path.dirname(self.db_path) or '.', exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        table = self.get_dataset_name()
        columns = list(records[0].keys())
        col_defs = ', '.join(f'"{c}" TEXT' for c in columns)
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS "{table}" (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {col_defs},
                first_seen TEXT DEFAULT CURRENT_TIMESTAMP,
                last_seen TEXT DEFAULT CURRENT_TIMESTAMP,
                is_new INTEGER DEFAULT 1
            )
        ''')
        cursor.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table}_hash" ON "{table}" (dedup_hash)')
        new_count = 0
        updated_count = 0
        for rec in records:
            cursor.execute(f'SELECT id FROM "{table}" WHERE dedup_hash = ?', (rec['dedup_hash'],))
            existing = cursor.fetchone()
            if existing:
                sets = ', '.join(f'"{k}" = ?' for k in rec.keys())
                cursor.execute(
                    f'UPDATE "{table}" SET {sets}, last_seen = CURRENT_TIMESTAMP, is_new = 0 WHERE dedup_hash = ?',
                    list(rec.values()) + [rec['dedup_hash']]
                )
                updated_count += 1
            else:
                cols = ', '.join(f'"{k}"' for k in rec.keys())
                vals = ', '.join('?' * len(rec))
                cursor.execute(f'INSERT INTO "{table}" ({cols}) VALUES ({vals})', list(rec.values()))
                new_count += 1
        conn.commit()
        conn.close()
        return new_count, updated_count

    def _safe_request(self, url, params=None, headers=None, max_retries=3):
        import requests
        if not headers:
            headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; FirmableBot/1.0; B2B Research)',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,application/json,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
            }
        for attempt in range(max_retries):
            try:
                resp = requests.get(url, params=params, headers=headers, timeout=30)
                resp.raise_for_status()
                return resp
            except requests.exceptions.HTTPError as e:
                if resp.status_code == 429:
                    wait = 2 ** (attempt + 2)
                    self.logger.warning(f"Rate limited. Waiting {wait}s...")
                    time.sleep(wait)
                elif resp.status_code >= 500:
                    time.sleep(2 ** attempt)
                else:
                    self.logger.error(f"HTTP {resp.status_code}: {url}")
                    return None
            except Exception as e:
                self.logger.warning(f"Request failed (attempt {attempt+1}/{max_retries}): {e}")
                time.sleep(2 ** attempt)
        return None

    def _rate_limit(self, seconds=1.0):
        time.sleep(seconds)

    def export_csv(self, path=None):
        if not path:
            os.makedirs("data/cleaned", exist_ok=True)
            path = f"data/cleaned/{self.get_dataset_name()}.csv"

        # Check DB exists first
        if not os.path.exists(self.db_path):
            self.logger.warning(f"Database not found at {self.db_path}. No data to export.")
            return None

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        rows = []
        columns = []
        try:
            # Check table exists before querying
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (self.get_dataset_name(),)
            )
            if cursor.fetchone() is None:
                self.logger.warning(
                    f"Table '{self.get_dataset_name()}' does not exist — "
                    f"crawler returned 0 records, nothing to export."
                )
                conn.close()
                return None

            cursor.execute(f'SELECT * FROM "{self.get_dataset_name()}"')
            columns = [d[0] for d in cursor.description]
            rows = cursor.fetchall()
        except Exception as e:
            self.logger.error(f"Export failed: {e}")
        finally:
            conn.close()

        if not rows:
            self.logger.warning("Table exists but has 0 records — CSV not written.")
            return None

        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)

        self.logger.info(f"Exported {len(rows)} records to {path}")
        return path