"""
Cafe Sales ETL Pipeline
-----------------------
Raw messy CSV  ->  cleaned DataFrame  ->  clean CSV + SQLite warehouse

Usage:
    python cafe_etl.py                       # uses dirty_cafe_sales.csv
    python cafe_etl.py path/to/new_file.csv  # re-run on any new extract
"""

import argparse
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_INPUT = "dirty_cafe_sales.csv"
CLEAN_CSV = "clean_cafe_sales.csv"
DB_PATH = "cafe_warehouse.db"
TABLE_NAME = "staging_cafe_sales"

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

PLACEHOLDERS = ["ERROR", "UNKNOWN", "NAN", "NONE", "NULL", "N/A", ""]


def load_raw(path: str) -> pd.DataFrame:
    file = Path(path)
    if not file.exists():
        sys.exit(f"Error: '{path}' not found. Put the raw CSV in this folder "
                 f"or pass its path as an argument.")
    df = pd.read_csv(file, dtype=str)
    print(f"[load]     {len(df):,} rows, {df.shape[1]} columns from {file.name}")
    return df


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=COLUMN_MAP)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def clean_text(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        df[col] = df[col].str.strip()
        df.loc[df[col].str.upper().isin(PLACEHOLDERS), col] = np.nan

    for col in ["item", "payment_method", "location"]:
        if col in df.columns:
            df[col] = df[col].str.title()
    return df


def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["transaction_total"] = pd.to_numeric(df["transaction_total"], errors="coerce")
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
    return df


def patch_gaps(df: pd.DataFrame) -> pd.DataFrame:
    before = df[["item", "quantity", "price", "transaction_total"]].isna().sum()

    # 1. Price = Total / Quantity
    m = df["price"].isna() & df["transaction_total"].notna() & (df["quantity"] > 0)
    df.loc[m, "price"] = df.loc[m, "transaction_total"] / df.loc[m, "quantity"]

    # 2. Price from the item's known menu price
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
    start = len(df)

    df = df.dropna(subset=["transaction_date", "item", "quantity", "transaction_total"])

    df["payment_method"] = df["payment_method"].fillna("Unknown")
    df["location"] = df["location"].fillna("Unknown")

    df["quantity"] = df["quantity"].round().astype(int)
    df["price"] = df["price"].round(2)
    df["transaction_total"] = df["transaction_total"].round(2)

    df = df.drop_duplicates(subset="transaction_id").reset_index(drop=True)
    print(f"[finalize] dropped {start - len(df):,} unrecoverable/duplicate rows -> {len(df):,} remain")
    return df


def validate(df: pd.DataFrame) -> None:
    mismatch = (df["quantity"] * df["price"] - df["transaction_total"]).abs() > 0.01
    print("[validate] remaining nulls per column:")
    print(df.isna().sum().to_string())
    print(f"[validate] rows where quantity * price != total: {int(mismatch.sum())}")
    print(f"[validate] date range: {df['transaction_date'].min().date()} "
          f"to {df['transaction_date'].max().date()}")


def clean_pipeline(path: str) -> pd.DataFrame:
    df = load_raw(path)
    df = standardize_columns(df)
    df = clean_text(df)
    df = convert_types(df)
    df = patch_gaps(df)
    df = finalize(df)
    validate(df)
    return df


def export_csv(df: pd.DataFrame, path: str = CLEAN_CSV) -> None:
    out = df.copy()
    out["transaction_date"] = out["transaction_date"].dt.strftime("%Y-%m-%d")
    out.to_csv(path, index=False)
    print(f"[export]   wrote {path}")


def load_to_sqlite(df: pd.DataFrame, db_path: str = DB_PATH, table: str = TABLE_NAME) -> None:
    out = df.copy()
    out["transaction_date"] = out["transaction_date"].dt.strftime("%Y-%m-%d")

    with sqlite3.connect(db_path) as conn:
        out.to_sql(table, conn, if_exists="replace", index=False)
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"[database] loaded {count:,} rows into {db_path} -> table '{table}'")


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