"""
Cafe Sales ETL Pipeline
-----------------------
Raw messy CSV  ->  cleaned DataFrame  ->  clean CSV + SQLite warehouse

Usage:
    python etl_pipeline.py                       # uses dirty_cafe_sales.csv
    python etl_pipeline.py path/to/new_file.csv  # re-run on any new extract
"""

import argparse
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
DEFAULT_INPUT = "dirty_cafe_sales.csv"
CLEAN_CSV = "clean_cafe_sales.csv"
DB_PATH = "cafe_warehouse.db"
TABLE_NAME = "staging_cafe_sales"

# Kaggle column names -> clean snake_case names
COLUMN_MAP = {
    "Transaction ID": "transaction_id",
    "Item": "item",
    "Quantity": "quantity",
    "Price Per Unit": "price",
    "Total Spent": "transaction_total",
    "Payment Method": "payment_method",
    "Location": "location",
    "Transaction Date": "transaction_date",
}

# Placeholder strings that really mean "missing"
PLACEHOLDERS = ["ERROR", "UNKNOWN", "NAN", "NONE", "NULL", "N/A", ""]


# ---------------------------------------------------------------------------
# PILLAR 1: PROGRAMMATIC DATA CLEANING
# ---------------------------------------------------------------------------
def load_raw(path: str) -> pd.DataFrame:
    """Load the raw CSV exactly as-is (all text, so nothing is silently altered)."""
    file = Path(path)
    if not file.exists():
        sys.exit(f"Error: '{path}' not found. Put the raw CSV in this folder "
                 f"or pass its path as an argument.")
    df = pd.read_csv(file, dtype=str)
    print(f"[load]     {len(df):,} rows, {df.shape[1]} columns from {file.name}")
    return df


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to snake_case; fall back to generic formatting for extras."""
    df = df.rename(columns=COLUMN_MAP)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def clean_text(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace, fix casing, and turn placeholder text into real NaN."""
    for col in df.columns:
        df[col] = df[col].str.strip()
        df.loc[df[col].str.upper().isin(PLACEHOLDERS), col] = np.nan

    for col in ["item", "payment_method", "location"]:
        if col in df.columns:
            df[col] = df[col].str.title()
    return df


def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to proper numeric / datetime types."""
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["transaction_total"] = pd.to_numeric(df["transaction_total"], errors="coerce")
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
    return df


def patch_gaps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Algorithmic gap patching. Uses the relationship
        transaction_total = quantity * price
    and the fixed menu prices found in the data to recover missing values.
    """
    before = df[["item", "quantity", "price", "transaction_total"]].isna().sum()

    # 1. Price = Total / Quantity
    m = df["price"].isna() & df["transaction_total"].notna() & (df["quantity"] > 0)
    df.loc[m, "price"] = df.loc[m, "transaction_total"] / df.loc[m, "quantity"]

    # 2. Price from the item's known menu price (most common price per item)
    menu = df.dropna(subset=["item", "price"]).groupby("item")["price"].agg(
        lambda s: s.mode().iloc[0]
    )
    m = df["price"].isna() & df["item"].notna()
    df.loc[m, "price"] = df.loc[m, "item"].map(menu)

    # 3. Item from price, only where that price belongs to exactly one item
    price_to_items = df.dropna(subset=["item", "price"]).groupby("price")["item"].unique()
    unique_price_map = {p: items[0] for p, items in price_to_items.items() if len(items) == 1}
    m = df["item"].isna() & df["price"].notna()
    df.loc[m, "item"] = df.loc[m, "price"].map(unique_price_map)

    # 4. Quantity = Total / Price
    m = df["quantity"].isna() & df["transaction_total"].notna() & (df["price"] > 0)
    df.loc[m, "quantity"] = df.loc[m, "transaction_total"] / df.loc[m, "price"]

    # 5. Total = Quantity * Price
    m = df["transaction_total"].isna() & df["quantity"].notna() & df["price"].notna()
    df.loc[m, "transaction_total"] = df.loc[m, "quantity"] * df.loc[m, "price"]

    after = df[["item", "quantity", "price", "transaction_total"]].isna().sum()
    recovered = (before - after).sum()
    print(f"[patch]    recovered {int(recovered):,} missing values via business logic")
    return df


def finalize(df: pd.DataFrame) -> pd.DataFrame:
    """Handle what can't be recovered, then lock in final dtypes."""
    start = len(df)

    # Rows that still lack core facts can't be analysed
    df = df.dropna(subset=["transaction_date", "item", "quantity", "transaction_total"])

    # Descriptive columns: keep the row, label the gap
    df["payment_method"] = df["payment_method"].fillna("Unknown")
    df["location"] = df["location"].fillna("Unknown")

    df["quantity"] = df["quantity"].round().astype(int)
    df["price"] = df["price"].round(2)
    df["transaction_total"] = df["transaction_total"].round(2)

    df = df.drop_duplicates(subset="transaction_id").reset_index(drop=True)
    print(f"[finalize] dropped {start - len(df):,} unrecoverable/duplicate rows -> {len(df):,} remain")
    return df


def validate(df: pd.DataFrame) -> None:
    """Quality checks: the portfolio-friendly 'proof' the data is trustworthy."""
    mismatch = (df["quantity"] * df["price"] - df["transaction_total"]).abs() > 0.01
    print("[validate] remaining nulls per column:")
    print(df.isna().sum().to_string())
    print(f"[validate] rows where quantity * price != total: {int(mismatch.sum())}")
    print(f"[validate] date range: {df['transaction_date'].min().date()} "
          f"to {df['transaction_date'].max().date()}")


# ---------------------------------------------------------------------------
# PILLAR 2: REPRODUCIBILITY & AUTOMATION
# ---------------------------------------------------------------------------
def clean_pipeline(path: str) -> pd.DataFrame:
    """One call runs every cleaning step in order: the 'one-click' blueprint."""
    df = load_raw(path)
    df = standardize_columns(df)
    df = clean_text(df)
    df = convert_types(df)
    df = patch_gaps(df)
    df = finalize(df)
    validate(df)
    return df


# ---------------------------------------------------------------------------
# PILLAR 3: DATA INGESTION & INTEGRATION
# ---------------------------------------------------------------------------
def export_csv(df: pd.DataFrame, path: str = CLEAN_CSV) -> None:
    """Flat-file output for Tableau Public and Excel Power Query."""
    out = df.copy()
    out["transaction_date"] = out["transaction_date"].dt.strftime("%Y-%m-%d")
    out.to_csv(path, index=False)
    print(f"[export]   wrote {path}")


def load_to_sqlite(df: pd.DataFrame, db_path: str = DB_PATH, table: str = TABLE_NAME) -> None:
    """Load the clean data into a SQLite warehouse table."""
    out = df.copy()
    out["transaction_date"] = out["transaction_date"].dt.strftime("%Y-%m-%d")

    with sqlite3.connect(db_path) as conn:
        out.to_sql(table, conn, if_exists="replace", index=False)
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"[database] loaded {count:,} rows into {db_path} -> table '{table}'")


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Cafe sales ETL pipeline")
    parser.add_argument("input", nargs="?", default=DEFAULT_INPUT,
                        help="path to the raw CSV (default: dirty_cafe_sales.csv)")
    args = parser.parse_args()

    df = clean_pipeline(args.input)
    export_csv(df)
    load_to_sqlite(df)
    print("Pipeline complete.")


if __name__ == "__main__":
    main()



     git config --global user.name "Your Name" and then git config --global user.email "you@example.com" (use the email on your GitHub account). Git needs this to label your commits.