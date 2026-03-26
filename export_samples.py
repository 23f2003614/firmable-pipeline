import sqlite3, csv, os

conn = sqlite3.connect('data/firmable.db')
os.makedirs('data/samples', exist_ok=True)

tables = [r[0] for r in conn.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT IN ('pipeline_runs','sqlite_sequence')"
).fetchall()]

for t in tables:
    rows = conn.execute(f'SELECT * FROM "{t}" LIMIT 100').fetchall()
    cols = [d[0] for d in conn.execute(f'SELECT * FROM "{t}" LIMIT 1').description]
    with open(f'data/samples/{t}.csv', 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    print(f'Done: {t}')

conn.close()
print('All CSVs exported!')