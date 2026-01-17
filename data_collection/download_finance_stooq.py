#!/usr/bin/env python3
"""
Download stock data using Stooq (RELIABLE VERSION)
"""

import pandas as pd
import time
import os
from datetime import datetime


# ------------------------------------------------------------------
# STOOQ DOWNLOAD FUNCTION
# ------------------------------------------------------------------
def download_stock_stooq(ticker, start="2020-09-29", end="2021-08-16"):
    print(f"   Downloading {ticker}...", end=" ", flush=True)

    # Stooq uses lowercase + .us for US stocks
    stooq_symbol = f"{ticker.lower()}.us"
    url = f"https://stooq.com/q/d/l/?s={stooq_symbol}&i=d"

    try:
        df = pd.read_csv(url)

        if df.empty:
            print("❌ No data returned")
            return None

        # Standardize columns
        df.columns = ["date", "open", "high", "low", "close", "volume"]
        df["date"] = pd.to_datetime(df["date"])

        # Filter date range
        df = df[(df["date"] >= start) & (df["date"] <= end)]

        if df.empty:
            print("❌ No data in date range")
            return None

        df["ticker"] = ticker

        print(f"✅ {len(df)} records")
        return df

    except Exception as e:
        print(f"❌ Error: {str(e)[:50]}")
        return None


# ------------------------------------------------------------------
def main():
    print("=" * 70)
    print("📊 DOWNLOADING STOCK DATA WITH STOOQ")
    print("=" * 70)
    print("\nPeriod: 2020-09-29 to 2021-08-16\n")

    os.makedirs("../data/raw", exist_ok=True)

    tickers = ["GME", "AMC", "TSLA", "AAPL", "BB", "NOK", "PLTR", "SPCE"]

    all_data = []
    failed = []

    for ticker in tickers:
        df = download_stock_stooq(ticker)

        if df is not None:
            all_data.append(df)
        else:
            failed.append(ticker)

        # Small delay (not required, but polite)
        time.sleep(0.5)

    print("\n" + "-" * 70)

    if not all_data:
        print("\n❌ FAILED - No data downloaded")
        return

    combined = pd.concat(all_data, ignore_index=True)
    combined = combined.sort_values(["date", "ticker"]).reset_index(drop=True)

    output_file = "data/raw/stock_prices.csv"
    combined.to_csv(output_file, index=False)

    print("\n✅ SUCCESS!")
    print(f"   Downloaded: {len(tickers) - len(failed)}/{len(tickers)} tickers")
    print(f"   Total records: {len(combined)}")
    print(f"   Saved to: {output_file}")
    print(f"   Tickers: {sorted(combined['ticker'].unique().tolist())}")
    print(
        f"   Date range: {combined['date'].min().date()} to {combined['date'].max().date()}"
    )

    if failed:
        print(f"\n⚠️  Failed tickers: {failed}")

    print("\n📋 Sample data (first 5 rows):")
    print(combined.head())

    print("\n📊 Records per ticker:")
    print(combined.groupby("ticker").size().to_string())


# ------------------------------------------------------------------
if __name__ == "__main__":
    main()
