"""
Python Showcase: proves the three pillars of the Cafe Sales pipeline
--------------------------------------------------------------------
Pillar 1: Programmatic cleaning + scalability
Pillar 2: Reproducibility & automation
Pillar 3: Ingestion & integration (CSV, JSON, SQLite, SQL read-back)

Run (after cafe_etl.py works):
    .\\.venv\\Scripts\\python.exe python_showcase.py
Requires: cafe_etl.py and dirty_cafe_sales.csv in the same folder.
"""

import contextlib
import io
import sqlite3
import time
from pathlib import Path

import numpy as np
import pandas as pd

from cafe_etl import (
    COLUMN_MAP,
    DB_PATH,
    PLACEHOLDERS,
    clean_pipeline,
    load_to_sqlite,
)

RAW = "dirty_cafe_sales.csv"
OUT = Path("showcase_output")
OUT.mkdir(exist_ok=True)


def header(title: str) -> None:
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def quiet(func, *args, **kwargs):
    """Run a function but hide its print() output (keeps benchmarks readable)."""
    with contextlib.redirect_stdout(io.StringIO()):
        return func(*args, **kwargs)


def count_unusable(series: pd.Series) -> int:
    """Count cells that are blank or hold placeholder text like ERROR/UNKNOWN."""
    s = series.str.strip()
    return int((s.isna() | s.str.upper().isin(PLACEHOLDERS)).sum())


# ---------------------------------------------------------------------------
# PILLAR 1: PROGRAMMATIC CLEANING AT SCALE
# ---------------------------------------------------------------------------
def pillar_1_cleaning(raw: pd.DataFrame, clean: pd.DataFrame) -> None:
    header("PILLAR 1A: BEFORE vs AFTER CLEANING (data-quality report)")

    before = raw.apply(count_unusable).rename(index=COLUMN_MAP)
    after = ((clean.isna()) | (clean == "Unknown")).sum()
    report = pd.DataFrame({"unusable_before": before, "unusable_after": after})
    report["fixed"] = report["unusable_before"] - report["unusable_after"]
    report["pct_fixed"] = (100 * report["fixed"] / report["unusable_before"].replace(0, np.nan)).round(1)

    print(report.to_string())
    print(f"\nRows: {len(raw):,} raw -> {len(clean):,} clean "
          f"({len(raw) - len(clean):,} unrecoverable rows removed)")
    report.to_csv(OUT / "data_quality_report.csv")

    header("PILLAR 1B: SCALABILITY TEST (replicate data to ~1,000,000 rows)")
    factor = 100
    big_path = OUT / "_big_temp.csv"
    pieces = [
        raw.assign(**{"Transaction ID": raw["Transaction ID"] + f"_{i}"})
        for i in range(factor)
    ]
    pd.concat(pieces, ignore_index=True).to_csv(big_path, index=False)

    start = time.perf_counter()
    big_clean = quiet(clean_pipeline, str(big_path))
    seconds = time.perf_counter() - start
    big_rows = factor * len(raw)

    print(f"Input rows:   {big_rows:,}")
    print(f"Clean rows:   {len(big_clean):,}")
    print(f"Time taken:   {seconds:.1f} seconds ({big_rows / seconds:,.0f} rows/second)")
    print("Note: Excel's hard limit is 1,048,576 rows per sheet.")
    big_path.unlink()  # remove the large temp file


# ---------------------------------------------------------------------------
# PILLAR 2: REPRODUCIBILITY & AUTOMATION
# ---------------------------------------------------------------------------
def make_new_month_file(raw: pd.DataFrame, path: Path) -> None:
    """Simulate 'next month's' messy export: new rows, fresh random damage."""
    new = raw.sample(2000, random_state=7).copy()
    new["Transaction ID"] = "NEWMONTH_" + new["Transaction ID"]
    new.loc[new.sample(frac=0.10, random_state=1).index, "Price Per Unit"] = "ERROR"
    new.loc[new.sample(frac=0.10, random_state=2).index, "Total Spent"] = ""
    new.loc[new.sample(frac=0.05, random_state=3).index, "Item"] = "  UNKNOWN "
    new.to_csv(path, index=False)


def pillar_2_reproducibility(raw: pd.DataFrame) -> None:
    header("PILLAR 2: REPRODUCIBILITY (same code, brand-new messy file)")

    new_file = OUT / "new_month_dirty.csv"
    make_new_month_file(raw, new_file)
    print(f"Created a simulated next-month extract: {new_file}\n")

    first = clean_pipeline(str(new_file))      # visible output
    second = quiet(clean_pipeline, str(new_file))

    print(f"\nDeterminism check: running the pipeline twice gives identical "
          f"results -> {first.equals(second)}")

    load_to_sqlite(first, DB_PATH, "staging_cafe_sales_new_month")
    print("No code was changed to process the new file: only the file path.")


# ---------------------------------------------------------------------------
# PILLAR 3: INGESTION & INTEGRATION
# ---------------------------------------------------------------------------
def pillar_3_ingestion(clean: pd.DataFrame) -> None:
    header("PILLAR 3: INGESTION & INTEGRATION")

    out = clean.copy()
    out["transaction_date"] = out["transaction_date"].dt.strftime("%Y-%m-%d")

    # Flat files: CSV and JSON
    csv_path, json_path = OUT / "clean_sales.csv", OUT / "clean_sales.json"
    out.to_csv(csv_path, index=False)
    out.to_json(json_path, orient="records", indent=2)
    from_csv, from_json = pd.read_csv(csv_path), pd.read_json(json_path)
    print(f"CSV  written and re-read: {len(from_csv):,} rows")
    print(f"JSON written and re-read: {len(from_json):,} rows")

    # Database: load, index, inspect, query
    load_to_sqlite(clean, DB_PATH, "staging_cafe_sales")
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_date "
                     "ON staging_cafe_sales(transaction_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_item "
                     "ON staging_cafe_sales(item)")

        tables = pd.read_sql_query(
            "SELECT name, type FROM sqlite_master "
            "WHERE type IN ('table','index') AND name NOT LIKE 'sqlite_%'", conn)
        print("\nDatabase objects:")
        print(tables.to_string(index=False))

        query = """
            SELECT item,
                   COUNT(*)                   AS orders,
                   ROUND(SUM(transaction_total), 2) AS revenue
            FROM staging_cafe_sales
            GROUP BY item
            ORDER BY revenue DESC
        """
        result = pd.read_sql_query(query, conn)
    print("\nSQL query run from Python (revenue by item):")
    print(result.to_string(index=False))
    result.to_csv(OUT / "revenue_by_item.csv", index=False)

    print("\nTip: the same to_sql() call works for PostgreSQL or SQL Server by "
          "swapping the connection for a SQLAlchemy engine.")


# ---------------------------------------------------------------------------
def main() -> None:
    raw = pd.read_csv(RAW, dtype=str)
    clean = quiet(clean_pipeline, RAW)

    pillar_1_cleaning(raw, clean)
    pillar_2_reproducibility(raw)
    pillar_3_ingestion(clean)

    header("DONE: evidence saved in the 'showcase_output' folder")


if __name__ == "__main__":
    main()