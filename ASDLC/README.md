# ASDLC — Synthetic E-Commerce Data & Query Pipeline

This repository contains a small end-to-end pipeline for generating synthetic e-commerce datasets, ingesting them into an SQLite database, and running multi-table SQL joins and reports. It implements the steps from the reference slide: generate sample data, load into SQLite (ecom.db), and produce joined outputs.

Why this exists
- Useful for demos, testing SQL joins, practicing data engineering and analytics tasks, and demonstrating how to validate consistency between order totals and line items.

What the project contains
- `customers.csv`, `products.csv`, `orders.csv`, `order_items.csv`, `payments.csv` — example CSV inputs (root of `ASDLC/`).
- `ingest_to_sqlite.py` — script that reads the CSVs and writes `ecom.db` SQLite database.
- `run_join_queries.py` — runs two SQL reports (line-level and aggregated) against `ecom.db` and writes CSV outputs: `join_report_line.csv`, `join_report_aggregated.csv`.
- `join_report.sql` — the SQL for both reports (line-level join and aggregated per-order consistency check).
- `join_report_summary.md` — short findings from the sample run.
- `src/pipeline.py` — helper code to generate synthetic CSVs programmatically (if you want to regenerate larger datasets).

Quick start

1) (Optional) Create a virtual environment and activate it (Windows Powershell):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2) If you want to regenerate synthetic CSVs (50 rows each):

```powershell
python -c "from src.pipeline import generate_ecommerce_csvs; generate_ecommerce_csvs(base_dir='ASDLC/data')"
```

This writes CSV files to `ASDLC/data/csv/` when using the generator.

3) Ingest existing CSVs (in the repository root) into SQLite:

```powershell
python ingest_to_sqlite.py
```

This reads `customers.csv`, `products.csv`, `orders.csv`, `order_items.csv`, `payments.csv` and creates/overwrites `ASDLC/ecom.db`.

4) Run the join reports and export CSVs:

```powershell
python run_join_queries.py
```

This writes `join_report_line.csv` and `join_report_aggregated.csv` into `ASDLC/` and prints a short preview.

Files produced by the pipeline
- `ASDLC/ecom.db` — the generated SQLite database (not committed).
- `ASDLC/join_report_line.csv` — each row is a single order line with product info and line total.
- `ASDLC/join_report_aggregated.csv` — one row per order showing items total vs stored order total and a consistency flag.

Extending and verifying
- If totals mismatch, investigate `order_items.csv` and `orders.csv` for inconsistent values. The aggregation query identifies mismatches with a 0.01 tolerance.
- You can add more columns to `src/pipeline.py` to create additional realistic fields (addresses, discounts, taxes) and re-run ingestion.

Next steps I can do for you
- Regenerate and ingest 50-row CSVs and re-run the reports, then attach the generated reports.
- Produce an Excel (XLSX) version of the aggregated report highlighting mismatches.
- Push a small Git commit that adds the SQL/report files and a branch-ready README.

If you want me to proceed with any of the above, tell me which option and I will execute it.
