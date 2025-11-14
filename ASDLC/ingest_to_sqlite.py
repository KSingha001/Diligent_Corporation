# ingest_to_sqlite.py
import sqlite3
import csv
from pathlib import Path

CSV_FILES = {
    "customers": "customers.csv",
    "products": "products.csv",
    "orders": "orders.csv",
    "order_items": "order_items.csv",
    "payments": "payments.csv"
}

DB_PATH = "ecom.db"

def create_tables(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        phone TEXT,
        signup_date TEXT,
        country TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        product_name TEXT,
        category TEXT,
        price REAL,
        sku TEXT,
        stock INTEGER
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        order_date TEXT,
        status TEXT,
        total_amount REAL,
        FOREIGN KEY (customer_id) REFERENCES customers(id)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS order_items (
        id INTEGER PRIMARY KEY,
        order_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        unit_price REAL,
        FOREIGN KEY (order_id) REFERENCES orders(id),
        FOREIGN KEY (product_id) REFERENCES products(id)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS payments (
        id INTEGER PRIMARY KEY,
        order_id INTEGER,
        payment_date TEXT,
        amount REAL,
        payment_method TEXT,
        status TEXT,
        transaction_id TEXT,
        FOREIGN KEY (order_id) REFERENCES orders(id)
    );
    """)
    conn.commit()

def insert_from_csv(conn, table, csv_path):
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)  # header row
        placeholders = ",".join(["?"] * len(headers))
        sql = f"INSERT INTO {table} ({','.join(headers)}) VALUES ({placeholders})"
        rows = [tuple(row) for row in reader]
        # convert numeric fields where appropriate
        def convert_row(row, headers):
            out = []
            for val, h in zip(row, headers):
                if h in ("id", "customer_id", "product_id", "order_id", "quantity", "stock"):
                    try:
                        out.append(int(val))
                    except:
                        out.append(None)
                elif h in ("price", "unit_price", "total_amount", "amount"):
                    try:
                        out.append(float(val))
                    except:
                        out.append(0.0)
                else:
                    out.append(val)
            return tuple(out)
        converted = [convert_row(r, headers) for r in rows]
        cur = conn.cursor()
        cur.executemany(sql, converted)
        conn.commit()
        print(f"Inserted {len(converted)} rows into {table}")

def main():
    # check files exist
    for name, path in CSV_FILES.items():
        if not Path(path).exists():
            print(f"Missing file: {path}. Please make sure it's in the workspace.")
            return

    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)
    for table, path in CSV_FILES.items():
        insert_from_csv(conn, table, path)
    conn.close()
    print("Database created at", DB_PATH)

if __name__ == "__main__":
    main()
