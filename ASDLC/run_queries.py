# run_queries.py
import sqlite3
import csv
from pathlib import Path

DB = "ecom.db"
OUT_CSV = "join_report.csv"

JOIN_SQL = """
SELECT
    o.id AS order_id,
    o.order_date,
    o.status AS order_status,
    c.first_name || ' ' || c.last_name AS customer_name,
    p.product_name,
    oi.quantity,
    oi.unit_price,
    (oi.quantity * oi.unit_price) AS line_total,
    o.total_amount
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN order_items oi ON oi.order_id = o.id
JOIN products p ON oi.product_id = p.id
ORDER BY o.order_date DESC, o.id;
"""

AGG_SQL = """
SELECT
    o.id AS order_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    o.order_date,
    o.status,
    SUM(oi.quantity * oi.unit_price) AS items_total,
    o.total_amount,
    CASE WHEN ABS(SUM(oi.quantity * oi.unit_price) - o.total_amount) > 0.01
         THEN 'MISMATCH' ELSE 'OK' END AS consistency_check
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN order_items oi ON oi.order_id = o.id
GROUP BY o.id
ORDER BY o.order_date DESC;
"""

def run_query_and_write(conn, sql, out_csv):
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    # Print top 8 rows preview
    print("Columns:", cols)
    print("Preview (first 8 rows):")
    for r in rows[:8]:
        print(r)
    # write to CSV
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    print(f"Wrote {len(rows)} rows to {out_csv}")

def main():
    if not Path(DB).exists():
        print("Missing database:", DB)
        return
    conn = sqlite3.connect(DB)
    print("Running detailed join and saving to", OUT_CSV)
    run_query_and_write(conn, JOIN_SQL, OUT_CSV)
    print("\nRunning order-aggregate consistency check:")
    run_query_and_write(conn, AGG_SQL, "orders_consistency.csv")
    conn.close()

if __name__ == "__main__":
    main()
